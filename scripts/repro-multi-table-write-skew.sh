#!/usr/bin/env bash
# Write skew across tables in POST /v1/{prefix}/transactions/commit
# (apache/iceberg#18552, apache/polaris#5807). Each round creates fresh tables
# A and B and sends two transactions at once: T1 adds a schema to A and sets it
# current, and asserts with a requirement-only change (updates: []) that B's
# current schema is unchanged; T2 does the reverse. Each invalidates the
# other's assertion, so a serializable catalog commits at most one per round.
#
# usage: repro-multi-table-write-skew.sh CATALOG_URL [ROUNDS]
#   CATALOG_URL  catalog root, without /v1 (e.g. http://localhost:8181)
#   ROUNDS       rounds to run (default 20)
#   WAREHOUSE    env: warehouse for /v1/config, if the catalog needs one
#   TOKEN        env: bearer token, if the catalog needs one (never printed)
#   TIMEOUT      env: seconds allowed per request (default 30); a request that
#                runs out of time makes its round inconclusive
#
# Each round is "both committed" when both transactions got 2xx and loads of A
# and B (same table UUIDs) show both new schemas current; "inconclusive" when a
# transaction got anything but 2xx or 409 (a 5xx, a timeout, no connection) or
# a load failed or disagreed with the answers; and otherwise fine.
# Exit status: 3 if any round both committed, 4 if none did but some round was
# inconclusive, 0 if every round committed at most one, 1 on a setup error.
# Needs bash, curl and jq.
set -euo pipefail
fail() { echo "error: $*" >&2; exit 1; }
[ $# -ge 1 ] || fail "usage: repro-multi-table-write-skew.sh CATALOG_URL [ROUNDS]"
url=${1%/}; rounds=${2:-20}
[[ $rounds =~ ^[1-9][0-9]*$ ]] || fail "rounds must be a positive number"
timeout=${TIMEOUT:-30}
[[ $timeout =~ ^[1-9][0-9]*$ ]] || fail "TIMEOUT must be a positive number of seconds"

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
auth=()
if [ -n "${TOKEN:-}" ]; then # in a file, so the token stays out of the process list
  (umask 077; printf 'Authorization: Bearer %s\n' "$TOKEN" > "$dir/auth"); auth=(-H "@$dir/auth")
fi
# request NAME METHOD PATH [BODY]: the response body goes to $dir/NAME, and the
# HTTP status (000 if there was no response) is printed.
request() {
  local body=()
  [ $# -ge 4 ] && body=(-d "$4")
  curl -sS --max-time "$timeout" -o "$dir/$1" -w '%{http_code}' -H 'Content-Type: application/json' \
    ${auth[@]+"${auth[@]}"} -X "$2" "$base$3" ${body[@]+"${body[@]}"} 2>"$dir/$1.err" || true
}

base=$url
[ "$(request config GET "/v1/config${WAREHOUSE:+?warehouse=$WAREHOUSE}")" = 200 ] \
  || fail "GET /v1/config: $(cat "$dir/config" "$dir/config.err" 2>/dev/null)"
prefix=$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$dir/config")
base="$url/v1${prefix:+/$prefix}"
ns="write_skew_$(date +%s)_$RANDOM"
code=$(request ns POST /namespaces "{\"namespace\":[\"$ns\"]}")
case "$code" in 200|204) ;; *) fail "create namespace: HTTP $code $(cat "$dir/ns")" ;; esac

create() { # round table: creates it, prints "uuid schema-id"
  local code
  code=$(request "$1.create.$2" POST "/namespaces/$ns/tables" "{\"name\":\"$2\",\"stage-create\":false,\"schema\":
    {\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}}")
  [ "$code" = 200 ] || fail "create table $2: HTTP $code $(cat "$dir/$1.create.$2")"
  jq -er '.metadata | "\(."table-uuid") \(."current-schema-id")"' "$dir/$1.create.$2" \
    || fail "create table $2: no table UUID or schema ID"
}

txn() { # changed uuid schema-id column asserted uuid schema-id -> transaction body
  jq -nc --arg ns "$ns" --arg t "$1" --arg tu "$2" --argjson ts "$3" --arg col "$4" \
    --arg o "$5" --arg ou "$6" --argjson os "$7" '{"table-changes": [
    {identifier: {namespace: [$ns], name: $t},
     requirements: [{type: "assert-table-uuid", uuid: $tu},
                    {type: "assert-current-schema-id", "current-schema-id": $ts}],
     updates: [{action: "add-schema", "last-column-id": 2, schema: {type: "struct", "schema-id": ($ts + 1),
                  fields: [{id: 1, name: "id", required: true, type: "long"},
                           {id: 2, name: $col, required: false, type: "long"}]}},
               {action: "set-current-schema", "schema-id": -1}]},
    {identifier: {namespace: [$ns], name: $o},
     requirements: [{type: "assert-table-uuid", uuid: $ou},
                    {type: "assert-current-schema-id", "current-schema-id": $os}],
     updates: []}]}'
}

has_new_schema() { # round table uuid column: loaded, same UUID, and the new column's schema is current
  [ "$(request "$1.load.$2" GET "/namespaces/$ns/tables/$2")" = 200 ] || return 1
  jq -e --arg uuid "$3" --arg col "$4" '.metadata | ."table-uuid" == $uuid and
    (."current-schema-id" as $c | [.schemas[] | select(."schema-id" == $c) | .fields[].name] | index($col) != null)' \
    "$dir/$1.load.$2" >/dev/null
}

both_rounds=0 unknown_rounds=0
for r in $(seq "$rounds"); do
  ia=$(create "$r" "a$r"); ib=$(create "$r" "b$r")
  read -r ua sa <<<"$ia"; read -r ub sb <<<"$ib"
  t1=$(txn "a$r" "$ua" "$sa" from_t1 "b$r" "$ub" "$sb"); t2=$(txn "b$r" "$ub" "$sb" from_t2 "a$r" "$ua" "$sa")
  request "$r.t1" POST /transactions/commit "$t1" > "$dir/$r.t1.code" & p1=$!
  request "$r.t2" POST /transactions/commit "$t2" > "$dir/$r.t2.code" & p2=$!
  wait "$p1" "$p2"
  c1=$(cat "$dir/$r.t1.code"); c2=$(cat "$dir/$r.t2.code")
  result=""
  case "$c1 $c2" in
    2??\ 2??)
      if has_new_schema "$r" "a$r" "$ua" from_t1 && has_new_schema "$r" "b$r" "$ub" from_t2; then
        result="both committed: A and B both show the new schema as current"
      else
        result="inconclusive: both acknowledged, but loads of A and B do not confirm both changes"
      fi ;;
    2??\ 409|409\ 2??|409\ 409) ;;
    *) result="inconclusive: a transaction got neither 2xx nor 409" ;;
  esac
  case "$result" in
    both*) both_rounds=$((both_rounds + 1)) ;;
    inconclusive*) unknown_rounds=$((unknown_rounds + 1)) ;;
  esac
  echo "round $r: $c1 $c2${result:+, $result}"
done
echo "both committed in $both_rounds of $rounds rounds; $unknown_rounds inconclusive"
[ "$both_rounds" -eq 0 ] || exit 3
[ "$unknown_rounds" -eq 0 ] || exit 4

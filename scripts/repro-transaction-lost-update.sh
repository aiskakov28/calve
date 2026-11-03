#!/usr/bin/env bash
# Two concurrent requests to POST /v1/{prefix}/transactions/commit, each
# changing the same fresh table, repeated. A catalog may reject one with 409,
# but it must not answer 200/204 to both and then lose one of the changes.
#
# usage: repro-transaction-lost-update.sh CATALOG_URL [ROUNDS] [MODE]
#   CATALOG_URL  catalog root, without /v1 (e.g. http://localhost:8181)
#   MODE         transaction (default), table: same changes through the
#                single-table endpoint, or sequential: one request after the other
#   WAREHOUSE    env: warehouse for /v1/config, if the catalog needs one
#   TIMEOUT      env: seconds allowed per request (default 30); a request that
#                runs out of time makes its round inconclusive
#   MANIFEST_1, MANIFEST_2
#                env: two existing manifest lists the server can read. When set,
#                each writer adds a snapshot guarded on an empty main branch
#                instead of setting a property, so at most one may commit.
#
# Each round is "lost" when a change answered 2xx is missing from two later
# loads, "inconclusive" when a write got anything but 2xx or 409 (a 5xx, a
# timeout, no connection) or a load failed, and otherwise fine.
# Exit status: 3 if any round lost a change, 4 if none did but some round was
# inconclusive, 0 if every round was fine, 1 on a setup error.
# Needs bash, curl and jq.
set -euo pipefail
url=${1%/}; rounds=${2:-10}; mode=${3:-transaction}
fail() { echo "error: $*" >&2; exit 1; }
case "$mode" in transaction|table|sequential) ;; *) fail "unknown mode '$mode'" ;; esac
[[ $rounds =~ ^[1-9][0-9]*$ ]] || fail "rounds must be a positive number"
timeout=${TIMEOUT:-30}
[[ $timeout =~ ^[1-9][0-9]*$ ]] || fail "TIMEOUT must be a positive number of seconds"
if [ -n "${MANIFEST_1:-}" ] && [ -n "${MANIFEST_2:-}" ]; then snapshots=1
elif [ -z "${MANIFEST_1:-}" ] && [ -z "${MANIFEST_2:-}" ]; then snapshots=""
else fail "set both MANIFEST_1 and MANIFEST_2, or neither"; fi

dir=$(mktemp -d)
# request NAME METHOD PATH [BODY]: the response body goes to $dir/NAME, and the
# HTTP status (000 if there was no response) is printed.
request() {
  local body=()
  [ $# -ge 4 ] && body=(-d "$4")
  local code
  # A transfer that fails after the status line (a stall, a dropped connection)
  # still prints that status, so any curl failure counts as no answer.
  if code=$(curl -sS --max-time "$timeout" -o "$dir/$1" -w '%{http_code}' -H 'Content-Type: application/json' \
    -X "$2" "$base$3" ${body[@]+"${body[@]}"} 2>"$dir/$1.err"); then
    printf '%s' "$code"
  else
    printf 000
  fi
}

base=$url
[ "$(request config GET "/v1/config${WAREHOUSE:+?warehouse=$WAREHOUSE}")" = 200 ] \
  || fail "GET /v1/config: $(cat "$dir/config" "$dir/config.err" 2>/dev/null)"
prefix=$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$dir/config")
base="$url/v1${prefix:+/$prefix}"
ns="lost_update_$(date +%s)"
code=$(request ns POST /namespaces "{\"namespace\":[\"$ns\"]}")
case "$code" in 200|204) ;; *) fail "create namespace: HTTP $code $(cat "$dir/ns")" ;; esac

change() { # table uuid writer -> the writer's table change
  if [ -n "$snapshots" ]; then
    local list id
    if [ "$3" = a ]; then list=$MANIFEST_1 id=1; else list=$MANIFEST_2 id=2; fi
    jq -nc --arg t "$1" --arg uuid "$2" --arg ns "$ns" --arg list "$list" --argjson id "$id" '{
      identifier: {namespace: [$ns], name: $t},
      requirements: [{type: "assert-table-uuid", uuid: $uuid},
                     {type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": null}],
      updates: [{action: "add-snapshot", snapshot: {"snapshot-id": $id, "sequence-number": 1,
                   "timestamp-ms": (now * 1000 | floor), "manifest-list": $list,
                   summary: {operation: "append"}, "schema-id": 0}},
                {action: "set-snapshot-ref", "ref-name": "main", type: "branch", "snapshot-id": $id}]}'
  else
    jq -nc --arg t "$1" --arg uuid "$2" --arg ns "$ns" --arg key "writer_$3" '{
      identifier: {namespace: [$ns], name: $t},
      requirements: [{type: "assert-table-uuid", uuid: $uuid}],
      updates: [{action: "set-properties", updates: {($key): "1"}}]}'
  fi
}

send() { # round table writer change; the status goes to $dir/ROUND.WRITER.code
  if [ "$mode" = table ]; then
    request "$1.$3" POST "/namespaces/$ns/tables/$2" "$(jq -c '{requirements, updates}' <<<"$4")"
  else
    request "$1.$3" POST /transactions/commit "$(jq -c '{"table-changes": [.]}' <<<"$4")"
  fi > "$dir/$1.$3.code"
}

has_change() { # metadata-file writer: does the loaded table have this writer's change?
  if [ -n "$snapshots" ]; then
    jq -e --argjson id "$([ "$2" = a ] && echo 1 || echo 2)" \
      '[.snapshots[]?."snapshot-id"] | index($id) != null' "$1" >/dev/null
  else
    jq -e --arg k "writer_$2" '.properties | has($k)' "$1" >/dev/null
  fi
}

load() { # round table uuid try -> $dir/ROUND.meta.TRY, or fail if not that table
  local name="$1.load.$4"
  [ "$(request "$name" GET "/namespaces/$ns/tables/$2")" = 200 ] || return 1
  jq -e --arg uuid "$3" '.metadata."table-uuid" == $uuid' "$dir/$name" >/dev/null || return 1
  jq -c .metadata "$dir/$name" > "$dir/$1.meta.$4"
}

lost_rounds=0 unknown_rounds=0
for r in $(seq "$rounds"); do
  t="t$r"
  code=$(request "$r.create" POST "/namespaces/$ns/tables" \
    "{\"name\":\"$t\",\"schema\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}}")
  [ "$code" = 200 ] || fail "create table $t: HTTP $code $(cat "$dir/$r.create")"
  uuid=$(jq -er '.metadata."table-uuid"' "$dir/$r.create") || fail "create table $t: no table UUID"
  a=$(change "$t" "$uuid" a); b=$(change "$t" "$uuid" b)
  if [ "$mode" = sequential ]; then
    send "$r" "$t" a "$a"; send "$r" "$t" b "$b"
  else
    send "$r" "$t" a "$a" & pa=$!; send "$r" "$t" b "$b" & pb=$!; wait "$pa" "$pb"
  fi
  ca=$(cat "$dir/$r.a.code"); cb=$(cat "$dir/$r.b.code")

  result="" acked=""
  for w in a b; do
    case "$(cat "$dir/$r.$w.code")" in
      2??) acked+=" $w" ;;
      409) ;;
      *) result="inconclusive: writer $w got HTTP $(cat "$dir/$r.$w.code")" ;;
    esac
  done
  if [ -z "$result" ]; then
    if ! load "$r" "$t" "$uuid" 1; then
      result="inconclusive: the table could not be loaded afterwards"
    else
      missing=""
      for w in $acked; do has_change "$dir/$r.meta.1" "$w" || missing+=" $w"; done
      if [ -n "$missing" ]; then
        # Confirm with a second load before calling it lost.
        sleep 0.5
        if ! load "$r" "$t" "$uuid" 2; then
          result="inconclusive: the second load failed"
        else
          still=""
          for w in $missing; do has_change "$dir/$r.meta.2" "$w" || still+=" $w"; done
          [ -n "$still" ] && result="lost: acknowledged but missing from two loads:$still"
        fi
      fi
    fi
  fi
  case "$result" in
    lost*) lost_rounds=$((lost_rounds + 1)) ;;
    inconclusive*) unknown_rounds=$((unknown_rounds + 1)) ;;
  esac
  echo "round $r: $ca $cb${result:+, $result}"
done
echo "$lost_rounds of $rounds rounds lost an acknowledged change; $unknown_rounds inconclusive"
[ "$lost_rounds" -eq 0 ] || exit 3
[ "$unknown_rounds" -eq 0 ] || exit 4

#!/usr/bin/env bash
# Concurrent single-table commits with no requirements, each setting its own
# property on one fresh table. With no requirement to fail, a catalog should
# retry a lost compare-and-set on the server and apply the commit. Since 1.1.0
# Polaris answers most of them with 409 CommitConflictException "... because it
# was concurrently modified" instead (apache/polaris#5907); that 409 is the finding.
#
# usage: repro-polaris-commit-retry.sh CATALOG_URL [COMMITS] [WRITERS]
#   CATALOG_URL  catalog root, without /v1 (e.g. http://localhost:8184/api/catalog)
#   COMMITS      commits in total (default 200)
#   WRITERS      concurrent writers sharing them (default 8); use 1 for the control
#   WAREHOUSE    env: warehouse for /v1/config, if the catalog needs one
#   TOKEN        env: bearer token, if the catalog needs one (never printed or saved)
#   TIMEOUT      env: seconds allowed per request (default 30)
#
# Every request is sent once. A 2xx means applied, a 409 means rejected (only the
# CommitConflictException one counts as the finding), and a 5xx or no answer
# leaves the outcome unknown. A final read checks that every applied property is
# present and no rejected one is.
# Exit status: 3 if any commit got the CommitConflictException 409, 4 if none did
# but some got a 5xx or no answer, 0 otherwise, 1 on a setup error or a wrong
# final state. Needs bash, curl and jq.
set -euo pipefail
url=${1%/}; commits=${2:-200}; writers=${3:-8}; timeout=${TIMEOUT:-30}
fail() { echo "error: $*" >&2; exit 1; }
for n in "$commits" "$writers" "$timeout"; do
  [[ $n =~ ^[1-9][0-9]*$ ]] || fail "COMMITS, WRITERS and TIMEOUT must be positive numbers"
done
dir=$(mktemp -d); trap 'rm -rf "$dir"' EXIT
auth=(); if [ -n "${TOKEN:-}" ]; then
  (umask 077; printf 'Authorization: Bearer %s\n' "$TOKEN" > "$dir/auth"); auth=(-H "@$dir/auth")
fi
request() { # name method path [body]: body to $dir/name, status printed (000 if no answer)
  local body=(); [ $# -ge 4 ] && body=(-d "$4")
  curl -sS --max-time "$timeout" -o "$dir/$1" -w '%{http_code}' -H 'Content-Type: application/json' \
    ${auth[@]+"${auth[@]}"} -X "$2" "$base$3" ${body[@]+"${body[@]}"} 2>/dev/null || true
}

base=$url
code=$(request config GET "/v1/config${WAREHOUSE:+?warehouse=$WAREHOUSE}")
[ "$code" = 200 ] || fail "GET /v1/config: HTTP $code $(cat "$dir/config" 2>/dev/null)"
prefix=$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$dir/config")
base="$url/v1${prefix:+/$prefix}"
ns="commit_retry_$(date +%s)_$RANDOM"; t="t_$RANDOM$RANDOM"
code=$(request ns POST /namespaces "{\"namespace\":[\"$ns\"]}")
case "$code" in 200|204) ;; *) fail "create namespace: HTTP $code $(cat "$dir/ns")" ;; esac
# Creating a table can take a while on Polaris once many tables exist.
code=$(request create POST "/namespaces/$ns/tables" \
  "{\"name\":\"$t\",\"schema\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}}")
[ "$code" = 200 ] || fail "create table: HTTP $code $(cat "$dir/create")"
uuid=$(jq -er '.metadata."table-uuid"' "$dir/create") || fail "create table: no table UUID"

writer() { # index: sends commits index, index+WRITERS, ... one after another
  local i
  for ((i = $1; i <= commits; i += writers)); do
    request "c$i" POST "/namespaces/$ns/tables/$t" \
      "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"k$i\":\"1\"}}]}" > "$dir/c$i.code"
  done
}
pids=(); for w in $(seq "$writers"); do writer "$w" & pids+=($!); done
wait ${pids[@]+"${pids[@]}"}

conflicts=0 others=0 unknown=0 applied="" rejected=""
for i in $(seq "$commits"); do
  code=$(cat "$dir/c$i.code"); kind=$(jq -r '.error.type // empty' "$dir/c$i" 2>/dev/null || true)
  case "$code" in
    2??) applied+=" k$i"; kind="" ;;
    409) rejected+=" k$i"
      if [ "$kind" = CommitConflictException ] \
        && jq -e '.error.message | test("concurrently modified")' "$dir/c$i" >/dev/null 2>&1; then
        conflicts=$((conflicts + 1))
      else others=$((others + 1)); fi ;;
    *) unknown=$((unknown + 1)) ;;
  esac
  echo "HTTP $code${kind:+ $kind}" >> "$dir/summary"
done
sort "$dir/summary" | uniq -c | sort -rn

code=$(request final GET "/namespaces/$ns/tables/$t")
[ "$code" = 200 ] || fail "final read: HTTP $code"
jq -e --arg uuid "$uuid" '.metadata."table-uuid" == $uuid' "$dir/final" >/dev/null \
  || fail "final read returned a different table"
props=$(jq -r '.metadata.properties // {} | keys[]' "$dir/final")
present=$(grep -c '^k[0-9]*$' <<<"$props" || true)
missing="" extra=""
for k in $applied; do grep -qx "$k" <<<"$props" || missing+=" $k"; done
for k in $rejected; do if grep -qx "$k" <<<"$props"; then extra+=" $k"; fi; done
echo "$present of $commits properties present; $conflicts CommitConflictException 409s, $others other 409s, $unknown unknown outcomes"
[ -z "$missing" ] || fail "applied (2xx) but missing:$missing"
[ -z "$extra" ] || fail "rejected (409) but present:$extra"
[ "$conflicts" -eq 0 ] || exit 3
[ "$unknown" -eq 0 ] || exit 4

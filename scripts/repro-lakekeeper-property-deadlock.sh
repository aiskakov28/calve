#!/usr/bin/env bash
# Concurrent requirement-free commits to one table, each setting its own
# property. Lakekeeper v0.14.2 deadlocks in Postgres on table_properties and
# answers the loser 409 "deadlock detected" after PostgreSQL's deadlock_timeout
# (lakekeeper/lakekeeper#2201), so runs are slow. With one writer it does not.
#
# usage: repro-lakekeeper-property-deadlock.sh CATALOG_URL [COMMITS] [WRITERS]
#   CATALOG_URL  catalog root, without /v1 (e.g. http://localhost:8182/catalog)
#   COMMITS      commits in total (default 200), each with its own property
#   WRITERS      concurrent writers (default 8); use 1 for the control
#   WAREHOUSE    env: warehouse for /v1/config (Lakekeeper needs WAREHOUSE=calve)
#   TIMEOUT      env: seconds allowed per request (default 30)
#
# Every request is sent once. A 409 whose body names a deadlock is the finding,
# any other 409 is a clean conflict, and a 5xx or no answer is an unknown
# outcome. A final read checks that every acknowledged (2xx) property is present
# and no rejected (409) one is.
# Exit status: 3 if any commit got a deadlock 409, 4 if none did but some commit
# got a 5xx or no answer, 0 if every commit got 200 or another 409, 1 on a
# setup error or a property in the wrong state. Needs bash, curl and jq.
set -euo pipefail
url=${1:?usage: $0 CATALOG_URL [COMMITS] [WRITERS]}; url=${url%/}
commits=${2:-200}; writers=${3:-8}; timeout=${TIMEOUT:-30}
fail() { echo "error: $*" >&2; exit 1; }
for n in "$commits" "$writers" "$timeout"; do
  [[ $n =~ ^[1-9][0-9]*$ ]] || fail "COMMITS, WRITERS and TIMEOUT must be positive numbers"
done
dir=$(mktemp -d)
request() { # name method path [body]: body to $dir/name, status printed (000 if no answer)
  local body=(); [ $# -ge 4 ] && body=(-d "$4")
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
ns="property_deadlock_$(date +%s)_$RANDOM"; t="t_$RANDOM$RANDOM"
code=$(request ns POST /namespaces "{\"namespace\":[\"$ns\"]}")
case "$code" in 200|204) ;; *) fail "create namespace: HTTP $code $(cat "$dir/ns")" ;; esac
code=$(request create POST "/namespaces/$ns/tables" \
  "{\"name\":\"$t\",\"schema\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}}")
[ "$code" = 200 ] || fail "create table: HTTP $code $(cat "$dir/create")"
uuid=$(jq -er '.metadata."table-uuid"' "$dir/create") || fail "create table: no table UUID"

writer() { # index: sends commits index, index+WRITERS, ... one after another
  for i in $(seq "$1" "$writers" "$commits"); do
    request "k$i" POST "/namespaces/$ns/tables/$t" \
      "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"k$i\":\"1\"}}]}" > "$dir/k$i.code"
  done
}
start=$(date +%s); pids=()
for w in $(seq "$writers"); do if [ "$w" -le "$commits" ]; then writer "$w" & pids+=($!); fi; done
wait ${pids[@]+"${pids[@]}"}
elapsed=$(( $(date +%s) - start ))

[ "$(request final GET "/namespaces/$ns/tables/$t")" = 200 ] || fail "final read: $(cat "$dir/final" "$dir/final.err" 2>/dev/null)"
jq -e --arg uuid "$uuid" '.metadata."table-uuid" == $uuid' "$dir/final" >/dev/null || fail "final read: table UUID changed"
deadlocks=0 unknown=0 present=0 wrong=""
for i in $(seq "$commits"); do
  code=$(cat "$dir/k$i.code"); has=$(jq -r --arg k "k$i" '(.metadata.properties // {}) | has($k)' "$dir/final")
  [ "$has" = true ] && present=$((present + 1))
  case "$code" in
    2??) [ "$has" = true ] || wrong+=" k$i(acknowledged, missing)"; echo "$code" ;;
    409) [ "$has" = false ] || wrong+=" k$i(rejected, present)"
         jq -e '.error.message | test("deadlock"; "i")' "$dir/k$i" >/dev/null 2>&1 && deadlocks=$((deadlocks + 1))
         echo "409 $(jq -r '.error | "\(.type): \(.message)"' "$dir/k$i" 2>/dev/null | cut -c1-140)" ;;
    5??) unknown=$((unknown + 1)); echo "$code $(jq -r '.error.type' "$dir/k$i" 2>/dev/null)" ;;
    *) unknown=$((unknown + 1)); echo "$code no answer" ;;
  esac
done > "$dir/summary"
sed -E 's/[0-9a-f]{8}-[0-9a-f-]{27}/<id>/g' "$dir/summary" | sort | uniq -c | sort -rn
echo "$commits commits from $writers writers in ${elapsed}s: $deadlocks deadlock 409s, $unknown unknown outcomes"
echo "$present of $commits properties present"
[ -z "$wrong" ] || fail "final read disagrees with acknowledged or rejected commits:$wrong"
[ "$deadlocks" -eq 0 ] || exit 3
[ "$unknown" -eq 0 ] || exit 4

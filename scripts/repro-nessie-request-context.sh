#!/usr/bin/env bash
# Concurrent single-table commits to Nessie's Iceberg REST API. Each commit sets
# its own property with no requirements, so conflicts (409) are expected; a 500
# from an inactive request context (projectnessie/nessie#13271) is the finding.
#
# usage: repro-nessie-request-context.sh CATALOG_URL [WRITERS] [PER_WRITER]
#   CATALOG_URL  e.g. http://localhost:8183/iceberg
#   WRITERS      concurrent writers (default 8); use 1 for the control
#   PER_WRITER   commits each writer sends, one after another (default 25)
#   TIMEOUT      env: seconds allowed per request (default 10)
#
# Every request is sent once. A final read checks that every acknowledged
# property is present and no rejected (409) one is. A 500's outcome is unknown,
# so the script reports whether each one is visible in that read.
# Exit status: 3 if any commit got a 500, 4 if none did but some commit got no
# answer, 0 if every commit got 200 or 409, 1 on a setup error or an
# acknowledged or rejected property in the wrong state. Needs bash, curl, jq.
set -euo pipefail
url=${1%/}; writers=${2:-8}; per=${3:-25}; timeout=${TIMEOUT:-10}
fail() { echo "error: $*" >&2; exit 1; }
for n in "$writers" "$per" "$timeout"; do [[ $n =~ ^[1-9][0-9]*$ ]] || fail "counts must be positive numbers"; done
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
request() { # name method path [body]: body to $dir/name, status printed (000 if no answer)
  local body=(); [ $# -ge 4 ] && body=(-d "$4")
  local code
  # A transfer that fails after the status line (a stall, a dropped connection)
  # still prints that status, so any curl failure counts as no answer.
  if code=$(curl -sS --max-time "$timeout" -o "$dir/$1" -w '%{http_code}' -H 'Content-Type: application/json' \
    -X "$2" "$base$3" ${body[@]+"${body[@]}"} 2>/dev/null); then
    printf '%s' "$code"
  else
    printf 000
  fi
}

base=$url
[ "$(request config GET /v1/config)" = 200 ] || fail "GET /v1/config failed"
prefix=$(jq -r '.overrides.prefix // .defaults.prefix // empty' "$dir/config")
base="$url/v1${prefix:+/$prefix}"
ns="context_probe_$(date +%s)_$RANDOM$RANDOM"; t="t_$RANDOM$RANDOM"
[ "$(request ns POST /namespaces "{\"namespace\":[\"$ns\"]}")" = 200 ] || fail "create namespace: $(cat "$dir/ns")"
code=$(request create POST "/namespaces/$ns/tables" \
  "{\"name\":\"$t\",\"schema\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]},\"stage-create\":false}")
[ "$code" = 200 ] || fail "create table: HTTP $code $(cat "$dir/create")"
uuid=$(jq -er '.metadata."table-uuid"' "$dir/create") || fail "create table: no table UUID"

writer() { # index
  for j in $(seq "$per"); do
    key="w$1_$j"
    request "$key" POST "/namespaces/$ns/tables/$t" \
      "{\"requirements\":[],\"updates\":[{\"action\":\"set-properties\",\"updates\":{\"$key\":\"1\"}}]}" > "$dir/$key.code"
  done
}
pids=()
for i in $(seq "$writers"); do writer "$i" & pids+=($!); done
wait "${pids[@]}"

[ "$(request final GET "/namespaces/$ns/tables/$t")" = 200 ] || fail "final read failed"
jq -e --arg uuid "$uuid" '.metadata."table-uuid" == $uuid' "$dir/final" >/dev/null || fail "final read: table UUID changed"
ok=0 rejected=0 errors=0 unanswered=0 visible=0 wrong=""
for f in "$dir"/w*.code; do
  key=$(basename "$f" .code); code=$(cat "$f")
  present=$(jq -r --arg k "$key" '.metadata.properties | has($k)' "$dir/final")
  case "$code" in
    2??) ok=$((ok + 1)); [ "$present" = true ] || wrong+=" $key(acknowledged, missing)" ;;
    409) rejected=$((rejected + 1)); [ "$present" = false ] || wrong+=" $key(rejected, present)" ;;
    5??) errors=$((errors + 1)); [ "$present" = true ] && visible=$((visible + 1)) ;;
    *) unanswered=$((unanswered + 1)); [ "$present" = true ] && visible=$((visible + 1)) ;;
  esac
done
echo "$((writers * per)) commits: $ok ok, $rejected rejected (409), $errors HTTP 5xx, $unanswered unanswered"
echo "unknown outcomes visible in the final read: $visible of $((errors + unanswered))"
for f in "$dir"/w*.code; do
  if [[ $(cat "$f") == 5* ]]; then
    echo "first 5xx ($(basename "$f" .code)): $(jq -r '.error | "\(.type): \(.message)"' "${f%.code}" 2>/dev/null | cut -c1-200)"
    break
  fi
done
[ -z "$wrong" ] || fail "final read disagrees with acknowledged or rejected commits:$wrong"
[ "$errors" -eq 0 ] || exit 3
[ "$unanswered" -eq 0 ] || exit 4

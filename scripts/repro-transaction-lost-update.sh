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
#   MANIFEST_1, MANIFEST_2
#                env: two existing manifest lists the server can read. When set,
#                each writer adds a snapshot guarded on an empty main branch
#                instead of setting a property, so at most one may commit.
#
# Prints both statuses per round and whether an acknowledged change is missing.
# Exit status: 3 if any acknowledged change is missing, 0 if none, 1 on error.
# Needs bash, curl and jq.
set -euo pipefail
url=${1%/}; rounds=${2:-10}; mode=${3:-transaction}
call() { curl -sS -H 'Content-Type: application/json' "$@"; }

prefix=$(call "$url/v1/config${WAREHOUSE:+?warehouse=$WAREHOUSE}" | jq -r '.overrides.prefix // .defaults.prefix // empty')
base="$url/v1${prefix:+/$prefix}"
ns="lost_update_$(date +%s)"
call -X POST "$base/namespaces" -d "{\"namespace\":[\"$ns\"]}" >/dev/null
dir=$(mktemp -d)

change() { # table uuid writer -> the writer's requirements and updates
  if [ -n "${MANIFEST_1:-}" ]; then
    local list; [ "$3" = a ] && list=$MANIFEST_1 || list=$MANIFEST_2
    jq -nc --arg t "$1" --arg uuid "$2" --arg list "$list" --arg ns "$ns" \
      --argjson id "$([ "$3" = a ] && echo 1 || echo 2)" '{
      identifier: {namespace: [$ns], name: $t},
      requirements: [{type: "assert-table-uuid", uuid: $uuid},
                     {type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": null}],
      updates: [{action: "add-snapshot", snapshot: {"snapshot-id": $id, "sequence-number": 1,
                   "timestamp-ms": (now * 1000 | floor), "manifest-list": $list,
                   summary: {operation: "append"}, "schema-id": 0}},
                {action: "set-snapshot-ref", "ref-name": "main", type: "branch", "snapshot-id": $id}]}'
  else
    jq -nc --arg t "$1" --arg uuid "$2" --arg key "writer_$3" --arg ns "$ns" '{
      identifier: {namespace: [$ns], name: $t},
      requirements: [{type: "assert-table-uuid", uuid: $uuid}],
      updates: [{action: "set-properties", updates: {($key): "1"}}]}'
  fi
}

send() { # round table writer body
  if [ "$mode" = table ]; then
    call -o "$dir/$1.$3" -w '%{http_code}' -X POST "$base/namespaces/$ns/tables/$2" \
      -d "$(jq -c '{requirements, updates}' <<<"$4")"
  else
    call -o "$dir/$1.$3" -w '%{http_code}' -X POST "$base/transactions/commit" \
      -d "$(jq -c '{"table-changes": [.]}' <<<"$4")"
  fi > "$dir/$1.$3.code"
}

missing_rounds=0
for r in $(seq "$rounds"); do
  t="t$r"
  uuid=$(call -X POST "$base/namespaces/$ns/tables" \
    -d "{\"name\":\"$t\",\"schema\":{\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]}}" \
    | jq -er '.metadata."table-uuid"')
  a=$(change "$t" "$uuid" a); b=$(change "$t" "$uuid" b)
  if [ "$mode" = sequential ]; then
    send "$r" "$t" a "$a"; send "$r" "$t" b "$b"
  else
    send "$r" "$t" a "$a" & send "$r" "$t" b "$b" & wait
  fi
  meta=$(call "$base/namespaces/$ns/tables/$t" | jq -c .metadata)
  missing=""
  for w in a b; do
    code=$(cat "$dir/$r.$w.code")
    case "$code" in 2??) ;; *) continue ;; esac
    if [ -n "${MANIFEST_1:-}" ]; then
      id=$([ $w = a ] && echo 1 || echo 2)
      jq -e --argjson id "$id" '[.snapshots[]?."snapshot-id"] | index($id)' <<<"$meta" >/dev/null || missing+=" $w"
    else
      jq -e --arg k "writer_$w" '.properties | has($k)' <<<"$meta" >/dev/null || missing+=" $w"
    fi
  done
  [ -n "$missing" ] && missing_rounds=$((missing_rounds + 1))
  echo "round $r: $(cat "$dir/$r.a.code") $(cat "$dir/$r.b.code")${missing:+, acknowledged but missing:$missing}"
done
echo "$missing_rounds of $rounds rounds lost an acknowledged change"
[ "$missing_rounds" -eq 0 ] || exit 3

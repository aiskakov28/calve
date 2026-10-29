#!/usr/bin/env bash
# Race snapshot expiration against appends on every Compose catalog, then
# replay each history offline. Run after Compose is healthy and
# lakekeeper-init/polaris-init have completed. Paths are repository-relative.
set -euo pipefail
binary="${1:-bin/calve-snapshot}"
output="${2:-.}"
for location in "$binary" "$output"; do
  case "$location" in /*|[A-Za-z]:*|*..*) echo "use repository-relative paths"; exit 1 ;; esac
done
mkdir -p "$output"
repo="$(pwd)"
case "${OSTYPE:-}" in msys*|cygwin*) repo="$(pwd -W)"; export MSYS_NO_PATHCONV=1 ;; esac
reference="$(docker compose ps -q rest-fixture)"
silo="$(docker compose ps -q silo)"
[ -n "$reference" ] && [ -n "$silo" ]
image="$(docker inspect --format '{{.Image}}' "$silo")"
docker cp "$binary" "$reference:/tmp/calve-maintenance"
docker cp "$binary" "$silo:/tmp/calve-maintenance"

for name in reference lakekeeper nessie polaris; do
  runner="$silo" catalog="" warehouse="" token="" endpoint="http://localhost:9000" model="calve-maintenance-v1"
  case "$name" in
    reference) runner="$reference"; catalog="http://localhost:8181"; endpoint="" ;;
    lakekeeper) catalog="http://lakekeeper:8181/catalog"; warehouse="calve" ;;
    # Nessie returns only the current snapshot by design, so there is
    # nothing to expire and no retention claim; expect inconclusive.
    nessie) catalog="http://nessie:19120/iceberg"; model="calve-maintenance-current-v1" ;;
    polaris) catalog="http://polaris:8181/api/catalog"; warehouse="calve"; token="${POLARIS_TOKEN:?set POLARIS_TOKEN}" ;;
  esac
  file="history-maintenance-$name.jsonl"
  log="$output/maintenance-$name.log"
  code=0
  CALVE_TOKEN="$token" docker exec --env CALVE_TOKEN \
    --env AWS_ACCESS_KEY_ID=calve --env AWS_SECRET_ACCESS_KEY=calve-password --env AWS_SESSION_TOKEN= \
    "$runner" /tmp/calve-maintenance maintenance --catalog "$catalog" --warehouse "$warehouse" \
    --s3-endpoint "$endpoint" --s3-region local-01 --model "$model" \
    --appends 30 --timeout 5s --readback-timeout 200ms --out "/tmp/$file" \
    | tee "$log" || code=$?
  case "$code" in 0|4) ;; *) exit "$code" ;; esac
  docker cp "$runner:/tmp/$file" "$output/$file"
  if [ "$name" = reference ]; then
    # An expiration racing an append can leave the fixture's SQLite store
    # locked until restart (see docs/snapshot-workload-results.md). Restart it
    # so later steps start from a usable fixture.
    docker restart "$reference" >/dev/null
    ready=0
    for _ in $(seq 60); do
      if curl -sf http://localhost:8181/v1/config >/dev/null; then ready=1; break; fi
      sleep 1
    done
    [ "$ready" = 1 ] || { echo "reference fixture did not come back after restart"; exit 1; }
  fi
  checked=0
  docker run --rm --network none --mount "type=bind,source=$repo,target=/work,readonly" \
    --workdir /work --entrypoint "/work/$binary" "$image" check "$output/$file" || checked=$?
  if [ "$checked" != "$code" ]; then echo "maintenance replay disagrees on $name"; exit 1; fi
  if ! grep -Eq '^maintenance race \(.*\): [0-9]+ appends \([1-9][0-9]* ok,' "$log"; then
    echo "no acknowledged append on $name"; exit 1
  fi
done

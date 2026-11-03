#!/usr/bin/env bash
# Run after Compose is healthy and lakekeeper-init/polaris-init have completed.
# The static binary and output directory must be relative to the repository.
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
docker cp "$binary" "$reference:/tmp/calve-snapshot"
docker cp "$binary" "$silo:/tmp/calve-snapshot"

recreate_reference() {
  # Recreate the reference fixture, not just restart it, so each run starts
  # from a fresh SQLite store. Under concurrent writes the 1.10.1 fixture has
  # kept answering SQLITE_BUSY until replaced (docs/snapshot-workload-results.md,
  # docs/triage-sweep-environment.md).
  docker compose up -d --no-deps --force-recreate rest-fixture >/dev/null 2>&1
  reference="$(docker compose ps -q rest-fixture)"
  for _ in $(seq 60); do
    if curl -sf http://localhost:8181/v1/config >/dev/null; then
      docker cp "$binary" "$reference:/tmp/calve-snapshot"
      return 0
    fi
    sleep 1
  done
  echo "reference fixture did not come back after recreation"
  exit 1
}

for name in reference lakekeeper nessie polaris gravitino; do
  runner="$silo" catalog="" warehouse="" token="" endpoint="http://localhost:9000"
  model="calve-snapshot-v1" attempts=1
  case "$name" in
    reference) runner="$reference"; catalog="http://localhost:8181"; endpoint=""; attempts=3 ;;
    lakekeeper) catalog="http://lakekeeper:8181/catalog"; warehouse="calve" ;;
    nessie) catalog="http://nessie:19120/iceberg"; model="calve-snapshot-current-v1" ;;
    polaris) catalog="http://polaris:8181/api/catalog"; warehouse="calve"; token="${POLARIS_TOKEN:?set POLARIS_TOKEN}" ;;
    gravitino) catalog="http://gravitino:9001/iceberg" ;;
  esac
  covered=0
  for ((attempt=1;attempt<=attempts;attempt++)); do
    file="history-snapshot-$name-$attempt.jsonl"
    log="$output/snapshot-$name-$attempt.log"
    code=0
    if [ "$name" = reference ]; then
      # Every reference attempt starts on a fresh fixture.
      recreate_reference
      runner="$reference"
    fi
    # Start without evidence from an earlier run, here or in the runner.
    rm -f "$output/$file"
    docker exec "$runner" rm -f "/tmp/$file"
    CALVE_TOKEN="$token" docker exec --env CALVE_TOKEN \
      --env AWS_ACCESS_KEY_ID=calve --env AWS_SECRET_ACCESS_KEY=calve-password --env AWS_SESSION_TOKEN= \
      "$runner" /tmp/calve-snapshot snapshot --catalog "$catalog" --warehouse "$warehouse" \
      --s3-endpoint "$endpoint" --s3-region local-01 --model "$model" \
      --clients 2 --rounds 3 --timeout 5s --readback-timeout 200ms --out "/tmp/$file" \
      | tee "$log" || code=$?
    # Keep whatever this run saved, even when it failed, before propagating it.
    if docker exec "$runner" test -f "/tmp/$file"; then
      docker cp "$runner:/tmp/$file" "$output/$file"
    fi
    case "$code" in 0|3|4) ;; *) exit "$code" ;; esac
    [ -f "$output/$file" ] || { echo "no snapshot history saved on $name"; exit 1; }
    checked=0
    docker run --rm --network none --mount "type=bind,source=$repo,target=/work,readonly" \
      --workdir /work --entrypoint "/work/$binary" "$image" check-table "$output/$file" || checked=$?
    if [ "$checked" != "$code" ]; then echo "snapshot replay disagrees on $name"; exit 1; fi
    [ "$code" != 3 ] || exit 3
    if grep -Eq '^snapshot check .*: [1-9][0-9]* ok,|^snapshot check .* \([1-9][0-9]* resolved as committed\)' "$log"; then
      covered=1; break
    fi
    echo "no proven commit on $name, attempt $attempt"
  done
  [ "$covered" = 1 ] || { echo "no proven snapshot commit coverage on $name"; exit 1; }
done

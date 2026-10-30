#!/usr/bin/env bash
# No-fault baseline sweep: every workload on every Compose catalog, RUNS times.
# Each history lands at OUTPUT/<catalog>/<workload>/<run>.jsonl (a run that
# saved none leaves <run>.missing) for `calve results OUTPUT`. A run's exit
# status never stops the sweep: the histories are the result, recorded with
# each exit status in OUTPUT/exits.tsv.
#
# Run after Compose is healthy and lakekeeper-init/polaris-init have completed.
# CATALOGS and WORKLOADS (space-separated) narrow the matrix.
#
#   scripts/sweep.sh RUNS OUTPUT
set -euo pipefail
runs="${1:?usage: scripts/sweep.sh RUNS OUTPUT}"
output="${2:?usage: scripts/sweep.sh RUNS OUTPUT}"
case "$runs" in ''|*[!0-9]*|0) echo "RUNS must be a positive integer"; exit 1 ;; esac
case "$output" in /*|[A-Za-z]:*|*..*) echo "use a repository-relative output path"; exit 1 ;; esac
catalogs="${CATALOGS:-reference lakekeeper nessie polaris}"
workloads="${WORKLOADS:-append skew atomic schema snapshot maintenance}"
case "${OSTYPE:-}" in msys*|cygwin*) export MSYS_NO_PATHCONV=1 ;; esac
# Every sweep gets its own directory, so no earlier run's histories or catalog
# versions can be counted with this one.
if [ -e "$output" ] && [ -n "$(ls -A "$output")" ]; then
  echo "$output already holds a sweep; use a new directory"
  exit 1
fi

mkdir -p "$output" bin
go build -o bin/calve ./cmd/calve
arch="$(docker info --format '{{.Architecture}}')"
case "$arch" in aarch64|arm64) arch=arm64 ;; *) arch=amd64 ;; esac
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -o bin/calve-linux ./cmd/calve
reference="$(docker compose ps -q rest-fixture)"
silo="$(docker compose ps -q silo)"
[ -n "$reference" ] && [ -n "$silo" ]
docker cp bin/calve-linux "$reference:/tmp/calve-sweep"
docker cp bin/calve-linux "$silo:/tmp/calve-sweep"

: > "$output/catalogs.tsv"
for name in $catalogs; do
  service="$name"
  [ "$name" != reference ] || service=rest-fixture
  printf '%s\t%s\n' "$name" "$(docker inspect --format '{{.Config.Image}}' "$(docker compose ps -q "$service")")" >> "$output/catalogs.tsv"
done

restart_reference() {
  # Contention can leave the fixture's SQLite store locked until restart (see
  # docs/snapshot-workload-results.md); no run inherits another's lock.
  docker restart "$reference" >/dev/null
  for _ in $(seq 60); do
    if curl -sf http://localhost:8181/v1/config >/dev/null; then return 0; fi
    sleep 1
  done
  echo "reference fixture did not come back after restart"
  exit 1
}

polaris_token() {
  # Tokens expire during a long sweep, so each Polaris run gets a fresh one.
  curl -sf -X POST http://localhost:8184/api/catalog/v1/oauth/tokens -d grant_type=client_credentials \
    -d client_id=root -d client_secret=calve-secret -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token
}

# Host workloads use the published ports. Snapshot and maintenance run inside
# the catalog network so their manifests land in the catalog's storage, as in
# check-snapshots.sh and check-maintenance.sh.
run_one() {
  local name="$1" workload="$2" run="$3"
  local dir="$output/$name/$workload"
  local file="$dir/$run.jsonl" log="$dir/$run.log"
  mkdir -p "$dir"
  rm -f "$file" "$dir/$run.missing"
  local host="" inner="" warehouse="" token="" runner="$silo" endpoint="http://localhost:9000" current=""
  case "$name" in
    reference) host="http://localhost:8181"; inner="$host"; runner="$reference"; endpoint="" ;;
    lakekeeper) host="http://localhost:8182/catalog"; inner="http://lakekeeper:8181/catalog"; warehouse=calve ;;
    # Nessie returns only the current snapshot by design.
    nessie) host="http://localhost:8183/iceberg"; inner="http://nessie:19120/iceberg"; current=1 ;;
    polaris) host="http://localhost:8184/api/catalog"; inner="http://polaris:8181/api/catalog"; warehouse=calve; token="$(polaris_token)" ;;
    *) echo "unknown catalog $name"; exit 1 ;;
  esac
  [ "$name" != reference ] || restart_reference
  local code=0
  local common=(--warehouse "$warehouse" --token "$token" --timeout 5s)
  case "$workload" in
    append) bin/calve append --catalog "$host" "${common[@]}" --clients 8 --per-client 25 --out "$file" >"$log" 2>&1 || code=$? ;;
    skew) bin/calve skew --catalog "$host" "${common[@]}" --rounds 10 --out "$file" >"$log" 2>&1 || code=$? ;;
    atomic) bin/calve atomic --catalog "$host" "${common[@]}" --tables 2 --clients 4 --per-client 10 --out "$file" >"$log" 2>&1 || code=$? ;;
    schema) bin/calve schema --catalog "$host" "${common[@]}" --clients 4 --rounds 10 --out "$file" >"$log" 2>&1 || code=$? ;;
    snapshot|maintenance)
      local tmp="/tmp/sweep-$name-$workload-$run.jsonl" args
      if [ "$workload" = snapshot ]; then
        args=(snapshot --clients 2 --rounds 3 --model "calve-snapshot${current:+-current}-v1")
      else
        args=(maintenance --appends 30 --model "calve-maintenance${current:+-current}-v1")
      fi
      docker exec "$runner" rm -f "$tmp"
      CALVE_TOKEN="$token" docker exec --env CALVE_TOKEN \
        --env AWS_ACCESS_KEY_ID=calve --env AWS_SECRET_ACCESS_KEY=calve-password --env AWS_SESSION_TOKEN= \
        "$runner" /tmp/calve-sweep "${args[@]}" --catalog "$inner" --warehouse "$warehouse" \
        --s3-endpoint "$endpoint" --s3-region local-01 --timeout 5s --readback-timeout 200ms --out "$tmp" \
        >"$log" 2>&1 || code=$?
      if docker exec "$runner" test -f "$tmp"; then docker cp "$runner:$tmp" "$file"; fi
      ;;
    *) echo "unknown workload $workload"; exit 1 ;;
  esac
  [ "$name" != reference ] || restart_reference
  [ -f "$file" ] || : > "$dir/$run.missing"
  printf '%s\t%s\t%s\t%s\n' "$name" "$workload" "$run" "$code" >> "$output/exits.tsv"
  echo "$name $workload run $run: exit $code"
}

: > "$output/exits.tsv"
for ((run=1;run<=runs;run++)); do
  for name in $catalogs; do
    for workload in $workloads; do
      run_one "$name" "$workload" "$run"
    done
  done
done
bin/calve results "$output"

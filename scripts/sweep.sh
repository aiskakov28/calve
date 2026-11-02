#!/usr/bin/env bash
# Sweep every workload on every Compose catalog, RUNS times.
#
# Without FAULTS it is the no-fault baseline: each history lands at
# OUTPUT/<catalog>/<workload>/<run>.jsonl (a run that saved none leaves
# <run>.missing). With FAULTS (space-separated kinds), each workload runs under
# one-kind fault plans of DURATION (default 60s), seeded by the run number, and
# each run directory lands at OUTPUT/<catalog>/<workload>/<kind>/<run>/ (or
# <run>.missing). Process faults (kill, pause, restart) skip the reference
# fixture: a restarted fixture keeps its SQLite store and fails nearly every
# write (docs/fault-campaigns.md). Skew and atomic skip Gravitino, which has no
# multi-table transaction endpoint.
#
# `calve results OUTPUT` builds the table. A run's exit status never stops the
# sweep: the saved evidence is the result, recorded with each exit status in
# OUTPUT/exits.tsv.
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
catalogs="${CATALOGS:-reference lakekeeper nessie polaris gravitino}"
workloads="${WORKLOADS:-append skew atomic schema snapshot maintenance}"
faults="${FAULTS:-}"
duration="${DURATION:-60s}"
for kind in $faults; do
  case "$kind" in latency|reset|partition|kill|pause|restart) ;; *) echo "unknown fault kind $kind"; exit 1 ;; esac
done
# skipped succeeds whether a cell is a deliberate skip: process faults on the
# reference fixture, and Gravitino, which has no multi-table transactions, for
# skew and atomic.
skipped() {
  case "$1/$2" in gravitino/skew|gravitino/atomic) return 0 ;; esac
  case "$1/$3" in reference/kill|reference/pause|reference/restart) return 0 ;; esac
  return 1
}
# A sweep made only of skipped cells has nothing to run: that is a deliberate
# skip, not a failed sweep.
cells=0
for name in $catalogs; do
  for workload in $workloads; do
    for kind in ${faults:-none}; do
      skipped "$name" "$workload" "$kind" || cells=$((cells+1))
    done
  done
done
if [ "$cells" = 0 ]; then
  echo "nothing to run: every requested cell is skipped (process faults on the reference fixture, transactions on Gravitino)"
  exit 0
fi
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
  # The tag as configured and the image ID it resolved to, so results stay tied
  # to the exact image even after the tag moves.
  printf '%s\t%s\n' "$name" "$(docker inspect --format '{{.Config.Image}}{{"\t"}}{{.Image}}' "$(docker compose ps -q "$service")")" >> "$output/catalogs.tsv"
done

recreate_reference() {
  # Contention can leave the fixture's SQLite store locked. A restarted
  # container keeps that file and then fails nearly every write, so each
  # reference run gets a recreated container with a fresh store instead.
  docker compose up -d --no-deps --force-recreate rest-fixture >/dev/null 2>&1
  reference="$(docker compose ps -q rest-fixture)"
  for _ in $(seq 60); do
    if curl -sf http://localhost:8181/v1/config >/dev/null; then
      docker cp bin/calve-linux "$reference:/tmp/calve-sweep"
      return 0
    fi
    sleep 1
  done
  echo "reference fixture did not come back after recreation"
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
    reference) host="http://localhost:8181"; inner="$host"; endpoint="" ;;
    lakekeeper) host="http://localhost:8182/catalog"; inner="http://lakekeeper:8181/catalog"; warehouse=calve ;;
    # Nessie returns only the current snapshot by design.
    nessie) host="http://localhost:8183/iceberg"; inner="http://nessie:19120/iceberg"; current=1 ;;
    polaris) host="http://localhost:8184/api/catalog"; inner="http://polaris:8181/api/catalog"; warehouse=calve; token="$(polaris_token)" ;;
    gravitino) host="http://localhost:8185/iceberg"; inner="http://gravitino:9001/iceberg" ;;
    *) echo "unknown catalog $name"; exit 1 ;;
  esac
  if [ "$name" = reference ]; then
    recreate_reference
    runner="$reference"
  fi
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
  [ -f "$file" ] || : > "$dir/$run.missing"
  printf '%s\t%s\t%s\t%s\n' "$name" "$workload" "$run" "$code" >> "$output/exits.tsv"
  echo "$name $workload run $run: exit $code"
}

# Campaigns send catalog traffic through the Toxiproxy ports. Snapshot and
# maintenance write manifests from the host through Silo's loopback port; on
# the reference fixture, whose table files are local to it, they run inside
# the fixture and reach the catalog through the proxy's in-network address.
run_campaign() {
  local name="$1" workload="$2" kind="$3" run="$4"
  local dir="$output/$name/$workload/$kind"
  local out="$dir/$run" log="$dir/$run.log"
  mkdir -p "$dir"
  rm -rf "$out" "$dir/$run.missing"
  local host="" warehouse="" token="" current=""
  case "$name" in
    reference) host="http://localhost:28181" ;;
    lakekeeper) host="http://localhost:28182/catalog"; warehouse=calve ;;
    nessie) host="http://localhost:28183/iceberg"; current=1 ;;
    polaris) host="http://localhost:28184/api/catalog"; warehouse=calve; token="$(polaris_token)" ;;
    gravitino) host="http://localhost:28185/iceberg" ;;
    *) echo "unknown catalog $name"; exit 1 ;;
  esac
  [ "$name" != reference ] || recreate_reference
  local args=(run --workload "$workload" --seed "$run" --targets "$name" --faults "$kind" --duration "$duration"
    --warehouse "$warehouse" --timeout 5s)
  case "$workload" in
    snapshot) args+=(--model "calve-snapshot${current:+-current}-v1") ;;
    maintenance) args+=(--model "calve-maintenance${current:+-current}-v1") ;;
  esac
  local code=0
  if [ "$name" = reference ] && { [ "$workload" = snapshot ] || [ "$workload" = maintenance ]; }; then
    local tmp="/tmp/sweep-$workload-$kind-$run"
    docker exec "$reference" rm -rf "$tmp"
    docker exec "$reference" /tmp/calve-sweep "${args[@]}" --catalog http://toxiproxy:28181 \
      --toxiproxy http://toxiproxy:8474 --s3-endpoint "" --out-dir "$tmp" >"$log" 2>&1 || code=$?
    if docker exec "$reference" test -d "$tmp"; then docker cp "$reference:$tmp" "$out"; fi
  else
    CALVE_TOKEN="$token" AWS_ACCESS_KEY_ID=calve AWS_SECRET_ACCESS_KEY=calve-password AWS_SESSION_TOKEN= \
      bin/calve "${args[@]}" --catalog "$host" --s3-endpoint http://localhost:19000 --s3-region local-01 \
      --out-dir "$out" >"$log" 2>&1 || code=$?
  fi
  [ -d "$out" ] || : > "$dir/$run.missing"
  printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$workload" "$kind" "$run" "$code" >> "$output/exits.tsv"
  echo "$name $workload $kind run $run: exit $code"
}

: > "$output/exits.tsv"
for ((run=1;run<=runs;run++)); do
  for name in $catalogs; do
    for workload in $workloads; do
      if [ -z "$faults" ]; then
        skipped "$name" "$workload" none && continue
        run_one "$name" "$workload" "$run"
        continue
      fi
      for kind in $faults; do
        skipped "$name" "$workload" "$kind" && continue
        run_campaign "$name" "$workload" "$kind" "$run"
      done
    done
  done
done
bin/calve results "$output"

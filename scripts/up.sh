#!/usr/bin/env bash
# Start every Compose catalog, run the one-shot setup jobs and smoke-test each
# catalog. Safe to re-run on a running stack: the setup jobs keep what exists.
#
#   scripts/up.sh
set -euo pipefail
docker compose up -d --wait --wait-timeout 300
docker compose run --rm lakekeeper-init
docker compose run --rm polaris-init
mkdir -p bin
go build -o bin/calve ./cmd/calve

# Polaris needs an OAuth token for its root principal (test-only credentials).
token="$(curl -sf -X POST http://localhost:8184/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=root -d client_secret=calve-secret \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)"
if [ -z "$token" ] || [ "$token" = null ]; then
  echo "no Polaris token"
  exit 1
fi

bin/calve smoke --catalog http://localhost:8181
bin/calve smoke --catalog http://localhost:8182/catalog --warehouse calve
bin/calve smoke --catalog http://localhost:8183/iceberg
bin/calve smoke --catalog http://localhost:8184/api/catalog --warehouse calve --token "$token"
echo "all four catalogs are up"

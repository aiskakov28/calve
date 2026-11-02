#!/usr/bin/env bash
# Run Calve's catalog-only workloads against one catalog and report each
# verdict. The GitHub Action (action.yml) runs it; it runs locally too:
#
#   CATALOG=http://localhost:8181 scripts/check-catalog.sh
#
# CATALOG is the catalog's REST URL. WAREHOUSE and CALVE_TOKEN are passed on
# when the catalog needs them. WORKLOADS (space-separated, default all four)
# narrows the run. FAIL_ON is invalid (default) or inconclusive. Histories and
# logs go to OUT (default calve-results), which must be new or empty. CALVE is
# the calve binary (default bin/calve).
#
# Exit status: 3 if any workload is invalid, else 1 if any errored, else 4 if
# any is inconclusive and FAIL_ON=inconclusive, else 0.
set -euo pipefail
calve="${CALVE:-bin/calve}"
catalog="${CATALOG:?set CATALOG to the REST URL of the catalog}"
warehouse="${WAREHOUSE:-}"
workloads="${WORKLOADS:-append skew atomic schema}"
fail_on="${FAIL_ON:-invalid}"
out="${OUT:-calve-results}"
case "$fail_on" in invalid|inconclusive) ;; *) echo "FAIL_ON must be invalid or inconclusive"; exit 1 ;; esac
for workload in $workloads; do
  case "$workload" in append|skew|atomic|schema) ;; *) echo "unknown workload $workload (use append, skew, atomic or schema)"; exit 1 ;; esac
done
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
  echo "$out already holds results; use a new directory"
  exit 1
fi
mkdir -p "$out"

# Rank verdicts so the worst one wins: a finding outranks an error, which
# outranks an undecided run.
rank() {
  case "$1" in valid) echo 0 ;; inconclusive) echo 1 ;; error) echo 2 ;; invalid) echo 3 ;; esac
}
worst=valid
rows=""
common=(--catalog "$catalog" --warehouse "$warehouse" --timeout 5s)
for workload in $workloads; do
  log="$out/$workload.log" history="$out/$workload.jsonl" code=0
  case "$workload" in
    append) "$calve" append "${common[@]}" --clients 8 --per-client 25 --out "$history" >"$log" 2>&1 || code=$? ;;
    skew) "$calve" skew "${common[@]}" --rounds 10 --out "$history" >"$log" 2>&1 || code=$? ;;
    atomic) "$calve" atomic "${common[@]}" --tables 2 --clients 4 --per-client 10 --out "$history" >"$log" 2>&1 || code=$? ;;
    schema) "$calve" schema "${common[@]}" --clients 4 --rounds 10 --out "$history" >"$log" 2>&1 || code=$? ;;
  esac
  case "$code" in 0) verdict=valid ;; 3) verdict=invalid ;; 4) verdict=inconclusive ;; *) verdict=error ;; esac
  if [ "$(rank "$verdict")" -gt "$(rank "$worst")" ]; then worst="$verdict"; fi
  line="$(grep -m1 -E '^(VALID|INVALID|INCONCLUSIVE)' "$log" || tail -n 1 "$log")"
  echo "$workload: $verdict (exit $code): $line"
  rows+="| $workload | $verdict | ${line//|/\\|} |"$'\n'
done

{
  echo "## Calve on $catalog"
  echo
  echo "| Workload | Verdict | Detail |"
  echo "| --- | --- | --- |"
  printf '%s' "$rows"
  echo
  echo "Histories and logs: \`$out\`. A timeout or 5xx leaves a commit unknown, so a run can be inconclusive without any finding."
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
echo "verdict=$worst" >> "${GITHUB_OUTPUT:-/dev/null}"

case "$worst" in
  invalid) exit 3 ;;
  error) exit 1 ;;
  inconclusive) [ "$fail_on" = inconclusive ] && exit 4 ;;
esac
exit 0

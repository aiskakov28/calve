#!/usr/bin/env python3
"""Record concurrent Iceberg REST commits and check their acknowledged effects."""

import argparse
import base64
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
from time import monotonic_ns, sleep, time_ns
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="http://localhost:8181")
    parser.add_argument("--warehouse", default="")
    parser.add_argument("--namespace", default="calve_issue29_repro")
    parser.add_argument("--tables", type=int, choices=(1, 2), default=1)
    parser.add_argument("--endpoint", choices=("transaction", "table"), default="transaction")
    parser.add_argument("--workload", choices=("property", "snapshot"), default="property")
    parser.add_argument("--manifest-1", help="existing empty manifest list for snapshot ID 1")
    parser.add_argument("--manifest-2", help="existing empty manifest list for snapshot ID 2")
    parser.add_argument("--seed-history", type=Path,
                        help="Calve snapshot history containing manifest lists for IDs 1 and 2")
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--sequential", action="store_true")
    parser.add_argument("--out", type=Path, required=True, help="new evidence directory")
    args = parser.parse_args()
    if args.pairs < 1 or args.endpoint == "table" and args.tables != 1:
        parser.error("--pairs must be positive; the table endpoint requires --tables 1")
    if args.seed_history and args.workload != "snapshot":
        parser.error("--seed-history requires --workload snapshot")
    if args.seed_history:
        if args.manifest_1 or args.manifest_2:
            parser.error("use --seed-history or --manifest-1/--manifest-2, not both")
        manifests = {}
        with args.seed_history.open(encoding="utf-8") as seed:
            for line in seed:
                operation = json.loads(line)
                if operation.get("type") != "invoke" or operation.get("f") != "commit-table":
                    continue
                request = json.loads(base64.b64decode(operation["request"]["body"]))
                for update in request.get("updates", []):
                    if update.get("action") != "add-snapshot":
                        continue
                    snapshot = update["snapshot"]
                    snapshot_id = snapshot["snapshot-id"]
                    if (snapshot_id in (1, 2) and snapshot["sequence-number"] == 1
                            and snapshot.get("parent-snapshot-id") is None):
                        manifests[snapshot_id] = snapshot["manifest-list"]
        args.manifest_1, args.manifest_2 = manifests.get(1), manifests.get(2)
    if args.workload == "snapshot" and (
        args.tables != 1 or not args.manifest_1 or not args.manifest_2
    ):
        parser.error("snapshot requires --tables 1 and both existing manifest lists")
    args.out.mkdir(parents=True, exist_ok=False)
    root = args.catalog.rstrip("/")
    run_id = uuid4().hex[:12]
    lock = Lock()
    pairs = []

    with (args.out / "http.jsonl").open("w", encoding="utf-8") as evidence:
        def call(label, method, path, body=None):
            encoded = None if body is None else json.dumps(body).encode("utf-8")
            record = {"label": label, "method": method, "url": root + path,
                      "request_body": None if encoded is None else encoded.decode("utf-8"),
                      "started_ns": monotonic_ns()}
            request = Request(root + path, data=encoded, method=method,
                              headers={"Content-Type": "application/json"})
            try:
                try:
                    response = build_opener(NoRedirect()).open(request, timeout=15)
                except HTTPError as error:
                    response = error
                with response:
                    record.update(status=response.code, headers=dict(response.headers),
                                  response_body=response.read().decode("utf-8"))
            except (OSError, URLError) as error:
                record.update(status=0, error=str(error))
            finally:
                record["ended_ns"] = monotonic_ns()
                with lock:
                    evidence.write(json.dumps(record) + "\n")
                    evidence.flush()
            return record

        def expect(record, *statuses):
            if record["status"] not in statuses:
                raise RuntimeError(f'{record["label"]}: status {record["status"]}: '
                                   f'{record.get("response_body", record.get("error"))}')
            return json.loads(record["response_body"]) if record.get("response_body") else {}

        config_path = "/v1/config"
        if args.warehouse:
            config_path += "?" + urlencode({"warehouse": args.warehouse})
        config = expect(call("config", "GET", config_path), 200)
        properties = {**config.get("defaults", {}), **config.get("overrides", {})}
        prefix = properties.get("prefix", "").strip("/")
        api = "/v1/" + (quote(prefix, safe="/") + "/" if prefix else "")
        namespace = args.namespace.split(".")
        expect(call("namespace", "POST", api + "namespaces", {"namespace": namespace}),
               200, 201, 204, 409)
        tables_path = api + "namespaces/" + quote("\x1f".join(namespace), safe="") + "/tables"

        for pair in range(args.pairs):
            tables = []
            for index in range(args.tables):
                name = f"issue_{run_id}_{pair}_{index}"
                expect(call(f"{pair}/create/{index}", "POST", tables_path, {
                    "name": name, "schema": {"type": "struct", "fields": [
                        {"id": 1, "name": "id", "required": True, "type": "long"}]},
                }), 200)
                path = tables_path + "/" + quote(name, safe="")
                base = expect(call(f"{pair}/baseline/{index}", "GET", path), 200)["metadata"]
                if args.workload == "snapshot" and (
                    base.get("snapshots") or base.get("current-snapshot-id") not in (None, -1)
                ):
                    raise RuntimeError(f"{pair}: fresh table has snapshots")
                tables.append({"name": name, "path": path, "uuid": base["table-uuid"],
                               "schema_id": base["current-schema-id"]})

            def change(table, writer):
                requirements = [{"type": "assert-table-uuid", "uuid": table["uuid"]}]
                if args.workload == "property":
                    updates = [{"action": "set-properties", "updates": {keys[writer]: "1"}}]
                else:
                    requirements.append({"type": "assert-ref-snapshot-id", "ref": "main",
                                         "snapshot-id": None})
                    snapshot = {"snapshot-id": keys[writer], "sequence-number": 1,
                                "timestamp-ms": time_ns() // 1_000_000,
                                "manifest-list": manifests[writer], "schema-id": table["schema_id"],
                                "summary": {"operation": "append", "total-data-files": "0",
                                            "total-delete-files": "0", "total-records": "0",
                                            "total-files-size": "0"}}
                    updates = [{"action": "add-snapshot", "snapshot": snapshot},
                               {"action": "set-snapshot-ref", "ref-name": "main",
                                "type": "branch", "snapshot-id": keys[writer]}]
                return {"identifier": {"namespace": namespace, "name": table["name"]},
                        "requirements": requirements, "updates": updates}

            keys = ([f"calve.issue29.{run_id}.{pair}.{writer}" for writer in range(2)]
                    if args.workload == "property" else [1, 2])
            manifests = [args.manifest_1, args.manifest_2]
            barrier = Barrier(2, timeout=30)

            def commit(writer):
                changes = [change(table, writer) for table in tables]
                if not args.sequential:
                    barrier.wait()
                if args.endpoint == "transaction":
                    return call(f"{pair}/writer/{writer}", "POST", api + "transactions/commit",
                                {"table-changes": changes})
                changes[0].pop("identifier")
                return call(f"{pair}/writer/{writer}", "POST", tables[0]["path"], changes[0])

            if args.sequential:
                responses = [commit(writer) for writer in range(2)]
            else:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(commit, writer) for writer in range(2)]
                    responses = [future.result() for future in futures]

            # A second later load rules out a transient observation in this run.
            observations = []
            for phase in ("final", "later"):
                if phase == "later":
                    sleep(0.1)
                phase_observations = []
                for index, table in enumerate(tables):
                    result = expect(call(f"{pair}/{phase}/{index}", "GET", table["path"]), 200)
                    metadata = result["metadata"]
                    if metadata["table-uuid"] != table["uuid"]:
                        raise RuntimeError(f'{pair}: table identity changed')
                    if args.workload == "property":
                        values = metadata.get("properties", {})
                        phase_observations.append([values.get(key) == "1" for key in keys])
                    else:
                        snapshots = {snapshot["snapshot-id"]
                                     for snapshot in metadata.get("snapshots", [])}
                        phase_observations.append([key in snapshots for key in keys])
                observations.append(phase_observations)

            ack = [200 <= response["status"] < 300 for response in responses]
            missing = [{"table": tables[index]["name"], "writer": writer, "key": keys[writer]}
                       for index, present in enumerate(observations[-1])
                       for writer in range(2) if ack[writer] and not present[writer]]
            rejected_visible = [
                {"table": tables[index]["name"], "writer": writer, "key": keys[writer]}
                for index, present in enumerate(observations[-1]) for writer in range(2)
                if 400 <= responses[writer]["status"] < 500 and present[writer]
            ]
            uncertain = (not any(ack) or any(
                not (200 <= response["status"] < 300 or response["status"] == 409)
                for response in responses
            ))
            overlap = (responses[0]["started_ns"] < responses[1]["ended_ns"]
                       and responses[1]["started_ns"] < responses[0]["ended_ns"])
            result = {"pair": pair, "tables": tables, "statuses": [r["status"] for r in responses],
                      "overlap": overlap, "observations": observations,
                      "missing_acknowledged": missing, "rejected_visible": rejected_visible,
                      "uncertain": uncertain,
                      "both_guarded_acknowledged": args.workload == "snapshot" and all(ack)}
            pairs.append(result)
            print(f'{pair}: HTTP {result["statuses"]}, missing acknowledged effects: {len(missing)}',
                  flush=True)

    summary = {"catalog": args.catalog, "workload": args.workload,
               "endpoint": args.endpoint, "tables": args.tables,
               "sequential": args.sequential, "pairs": pairs,
               "pairs_with_missing_acknowledged": sum(bool(p["missing_acknowledged"]) for p in pairs),
               "pairs_with_rejected_visible": sum(bool(p["rejected_visible"]) for p in pairs),
               "inconclusive_pairs": sum(p["uncertain"] for p in pairs),
               "pairs_with_both_guarded_acknowledged": sum(p["both_guarded_acknowledged"] for p in pairs),
               "status_pairs": dict(Counter("/".join(map(str, sorted(p["statuses"]))) for p in pairs)),
               "overlapping_pairs": sum(p["overlap"] for p in pairs)}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: summary[key] for key in
                      ("pairs_with_missing_acknowledged", "pairs_with_rejected_visible",
                       "inconclusive_pairs", "pairs_with_both_guarded_acknowledged",
                       "status_pairs", "overlapping_pairs")}, indent=2))
    if (summary["pairs_with_missing_acknowledged"] or summary["pairs_with_rejected_visible"]
            or summary["pairs_with_both_guarded_acknowledged"]):
        raise SystemExit(3)
    if summary["inconclusive_pairs"]:
        raise SystemExit(4)


if __name__ == "__main__":
    main()

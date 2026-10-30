#!/usr/bin/env python3
"""Verify real Parquet appends against a local Iceberg JDBC fixture."""

import argparse
import hashlib
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import CommitTableRequest, TableIdentifier
from pyiceberg.schema import Schema
from pyiceberg.table.update import AssertTableUUID
from pyiceberg.types import LongType, NestedField


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def path_for(location):
    parsed = urlsplit(location)
    if parsed.scheme not in ("", "file") or parsed.netloc not in ("", "localhost"):
        raise ValueError(f"expected local fixture file, got {location}")
    return Path(unquote(parsed.path))


def http(method, url, body=None):
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    request = Request(url, data=data, method=method,
                      headers={"Content-Type": "application/json"})
    result = {"method": method, "url": url, "body": body, "started_ns": time.monotonic_ns()}
    try:
        try:
            response = build_opener(NoRedirect()).open(request, timeout=20)
        except HTTPError as error:
            response = error
        with response:
            result["status"] = response.code
            result["response"] = response.read().decode()
    except (OSError, URLError) as error:
        result["status"], result["error"] = 0, str(error)
    result["ended_ns"] = time.monotonic_ns()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--mode", choices=("transaction", "table", "sequential"), required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error("run without Python optimization so evidence assertions remain enabled")
    if args.pairs < 1:
        parser.error("--pairs must be positive")
    if pyiceberg.__version__ != "0.11.1":
        parser.error("this reproduction requires pyiceberg 0.11.1")
    if args.out.exists():
        raise ValueError(f"choose a new evidence file: {args.out}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog("independent_review", type="rest", uri=args.url)
    namespace = "independent_" + uuid4().hex[:12]
    catalog.create_namespace(namespace)
    table_schema = Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=True))
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    counts = {"both_guarded_acknowledged": 0, "both_success_one_snapshot": 0, "missing_ack_rows": 0,
              "expected_control": 0, "unknown_or_other": 0}

    with args.out.open("x", encoding="utf-8") as output:
        for pair in range(args.pairs):
            name = f"t_{pair}_{uuid4().hex[:10]}"
            table = catalog.create_table((namespace, name), table_schema)
            baseline_uuid = str(table.metadata.table_uuid)
            assert table.metadata.current_snapshot() is None
            identifiers = TableIdentifier(namespace=(namespace,), name=name)
            requests = []
            files = []
            snapshots = []
            for writer in range(2):
                value = 1000 + pair * 2 + writer
                transaction = table.transaction()
                frame = pa.Table.from_arrays([pa.array([value], type=pa.int64())], schema=arrow_schema)
                transaction.append(frame)
                requirements = transaction._requirements + (AssertTableUUID(uuid=table.metadata.table_uuid),)
                request = CommitTableRequest(identifier=identifiers, requirements=requirements,
                                             updates=transaction._updates)
                change = json.loads(request.model_dump_json())
                guards = {r["type"]: r for r in change["requirements"]}
                assert guards["assert-table-uuid"]["uuid"] == baseline_uuid
                assert guards["assert-ref-snapshot-id"]["ref"] == "main"
                assert guards["assert-ref-snapshot-id"]["snapshot-id"] is None
                snapshots_found = [u.snapshot for u in transaction._updates if hasattr(u, "snapshot")]
                assert len(snapshots_found) == 1
                snapshot = snapshots_found[0]
                snapshots.append(snapshot.snapshot_id)
                entries = [entry for manifest in snapshot.manifests(table.io)
                           for entry in manifest.fetch_manifest_entry(table.io)]
                assert len(entries) == 1
                data_path = path_for(entries[0].data_file.file_path)
                assert data_path.is_file()
                values = pq.read_table(data_path).column("id").to_pylist()
                assert values == [value]
                files.append({"writer": writer, "row": value, "path": str(data_path),
                              "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
                              "manifest_list": snapshot.manifest_list,
                              "snapshot_id": snapshot.snapshot_id})
                requests.append(change)
            assert snapshots[0] != snapshots[1]
            assert files[0]["path"] != files[1]["path"]
            baseline = http("GET", f"{args.url}/v1/namespaces/{namespace}/tables/{name}")
            assert baseline["status"] == 200
            assert not json.loads(baseline["response"])["metadata"].get("snapshots")
            barrier = threading.Barrier(2, timeout=30)

            def send(writer):
                if args.mode != "sequential":
                    barrier.wait()
                if args.mode == "table":
                    path = f"/v1/namespaces/{namespace}/tables/{name}"
                    body = requests[writer]
                else:
                    path = "/v1/transactions/commit"
                    body = {"table-changes": [requests[writer]]}
                return http("POST", args.url + path, body)

            if args.mode == "sequential":
                responses = [send(writer) for writer in range(2)]
            else:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    tasks = [pool.submit(send, writer) for writer in range(2)]
                    responses = [task.result() for task in tasks]
            time.sleep(0.5)
            final = http("GET", f"{args.url}/v1/namespaces/{namespace}/tables/{name}")
            assert final["status"] == 200
            assert final["started_ns"] > max(response["ended_ns"] for response in responses)
            metadata = json.loads(final["response"])["metadata"]
            assert metadata["table-uuid"] == baseline_uuid
            rest_location = json.loads(final["response"])["metadata-location"]
            with sqlite3.connect("file:" + args.db + "?mode=ro", uri=True) as database:
                rows = database.execute(
                    "SELECT metadata_location FROM iceberg_tables WHERE table_namespace=? AND table_name=?",
                    (namespace, name)).fetchall()
            assert len(rows) == 1
            database_location = rows[0][0]
            assert path_for(database_location) == path_for(rest_location)
            persisted = json.loads(path_for(database_location).read_text())
            assert persisted["table-uuid"] == baseline_uuid
            assert {s["snapshot-id"] for s in persisted.get("snapshots", [])} == {
                s["snapshot-id"] for s in metadata.get("snapshots", [])}
            loaded = catalog.load_table((namespace, name))
            observed_rows = loaded.scan().to_arrow().column("id").to_pylist()
            statuses = [response["status"] for response in responses]
            acknowledged = [files[i]["row"] for i, status in enumerate(statuses) if 200 <= status < 300]
            missing_rows = sorted(set(acknowledged) - set(observed_rows))
            retained = {s["snapshot-id"] for s in persisted.get("snapshots", [])}
            missing_snapshots = [snapshots[i] for i, status in enumerate(statuses)
                                 if 200 <= status < 300 and snapshots[i] not in retained]
            settled = all(200 <= status < 300 or status == 409 for status in statuses)
            if len(acknowledged) == 2:
                counts["both_guarded_acknowledged"] += 1
            if len(acknowledged) == 2 and len(retained) == 1:
                counts["both_success_one_snapshot"] += 1
            if missing_rows and missing_snapshots and settled:
                counts["missing_ack_rows"] += 1
            if len(acknowledged) == 1 and statuses.count(409) == 1 and not missing_rows:
                counts["expected_control"] += 1
            if not settled or not acknowledged:
                counts["unknown_or_other"] += 1
            evidence = {"pair": pair, "mode": args.mode, "namespace": namespace, "table": name,
                        "uuid": baseline_uuid, "pyiceberg": pyiceberg.__version__,
                        "pyarrow": pa.__version__, "baseline": baseline, "files": files,
                        "responses": responses, "final": final, "database_location": database_location,
                        "persisted_metadata": persisted, "observed_rows": observed_rows,
                        "missing_ack_rows": missing_rows, "missing_ack_snapshots": missing_snapshots}
            output.write(json.dumps(evidence) + "\n")
            output.flush()
            print(json.dumps({"pair": pair, "statuses": statuses, "rows": observed_rows,
                              "missing_ack_rows": missing_rows, "missing_snapshots": missing_snapshots}),
                  flush=True)
    print(json.dumps({"mode": args.mode, "pairs": args.pairs, "counts": counts}, indent=2))
    if counts["missing_ack_rows"] or counts["both_guarded_acknowledged"]:
        raise SystemExit(3)
    if counts["unknown_or_other"]:
        raise SystemExit(4)


if __name__ == "__main__":
    main()

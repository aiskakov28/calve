#!/usr/bin/env python3
"""Race two raw REST schema commits; retain HTTP evidence without client retries."""

import argparse
import copy
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
from time import monotonic_ns
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def schema_change(base, column):
    schema = copy.deepcopy(next(
        s for s in base["schemas"]
        if s["schema-id"] == base["current-schema-id"]
    ))
    schema["schema-id"] = max(s["schema-id"] for s in base["schemas"]) + 1
    field_id = base["last-column-id"] + 1
    schema["fields"].append({
        "id": field_id, "name": column, "required": False, "type": "long",
    })
    return {
        "requirements": [
            {"type": "assert-table-uuid", "uuid": base["table-uuid"]},
            {"type": "assert-current-schema-id",
             "current-schema-id": base["current-schema-id"]},
        ],
        "updates": [
            {"action": "add-schema", "schema": schema, "last-column-id": field_id},
            {"action": "set-current-schema", "schema-id": -1},
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="http://localhost:8182/catalog")
    parser.add_argument("--warehouse", default="calve")
    parser.add_argument("--namespace", default="calve_schema_cas_repro")
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--out", type=Path, required=True, help="new evidence directory")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    args.out.mkdir(parents=True, exist_ok=False)
    lock = Lock()
    root = args.catalog.rstrip("/")

    with (args.out / "http.jsonl").open("w", encoding="utf-8") as evidence:
        def call(label, method, path, body=None):
            wire = None if body is None else json.dumps(body).encode("utf-8")
            record = {"label": label, "method": method, "url": root + path,
                      "request_body": None if wire is None else wire.decode("utf-8"),
                      "started_ns": monotonic_ns()}
            request = Request(root + path, data=wire, method=method,
                              headers={"Content-Type": "application/json"})
            try:
                try:
                    response = build_opener(NoRedirect()).open(request, timeout=30)
                except HTTPError as error:
                    response = error
                with response:
                    record.update(status=response.code, headers=dict(response.headers),
                                  response_body=response.read().decode("utf-8"))
            except Exception as error:
                record["error"] = str(error)
                raise
            finally:
                record["ended_ns"] = monotonic_ns()
                with lock:
                    evidence.write(json.dumps(record) + "\n")
                    evidence.flush()
            return record

        def expect(record, *statuses):
            if record["status"] not in statuses:
                raise RuntimeError(f'{record["label"]}: HTTP {record["status"]}: '
                                   f'{record["response_body"]}')
            return json.loads(record["response_body"]) if record["response_body"] else {}

        config = expect(call("config", "GET", "/v1/config?" +
                             urlencode({"warehouse": args.warehouse})), 200)
        properties = {**config.get("defaults", {}), **config.get("overrides", {})}
        prefix = properties.get("prefix", "").strip("/")
        api = "/v1/" + (quote(prefix, safe="/") + "/" if prefix else "")
        namespace = args.namespace.split(".")
        expect(call("namespace", "POST", api + "namespaces",
                    {"namespace": namespace}), 200, 201, 204, 409)
        tables = api + "namespaces/" + quote("\x1f".join(namespace), safe="") + "/tables"

        def setup(label):
            name = "schema_cas_" + uuid4().hex
            expect(call(label + "/create", "POST", tables, {
                "name": name, "schema": {"type": "struct", "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]},
            }), 200)
            path = tables + "/" + name
            base = expect(call(label + "/baseline", "GET", path), 200)["metadata"]
            return path, base

        def observe(label, path, base, responses):
            final = expect(call(label + "/final", "GET", path), 200)["metadata"]
            if final["table-uuid"] != base["table-uuid"]:
                raise RuntimeError("Table identity changed")
            current = next(s for s in final["schemas"]
                           if s["schema-id"] == final["current-schema-id"])
            columns = {field["name"] for field in current["fields"]}
            for column, response in responses:
                if 200 <= response["status"] < 300 and column not in columns:
                    raise RuntimeError(f"Acknowledged column {column} is absent")
                if 400 <= response["status"] < 500 and any(
                    f["name"] == column for s in final["schemas"] for f in s["fields"]
                ):
                    raise RuntimeError(f"Rejected column {column} is present")
            if sum(column in columns for column, _ in responses) != 1:
                raise RuntimeError("Expected exactly one contender's column in current schema")
            return {"table_uuid": base["table-uuid"], "statuses": [r["status"] for _, r in responses],
                    "visible_columns": sorted(columns)}

        path, base = setup("control")
        first = call("control/first", "POST", path, schema_change(base, "first"))
        expect(first, 200, 204)
        stale = call("control/stale", "POST", path, schema_change(base, "stale"))
        expect(stale, 409)
        control = observe("control", path, base, [("first", first), ("stale", stale)])
        print("Sequential control: first accepted, stale schema rejected with 409", flush=True)

        rounds = []
        duplicate_errors = 0
        for round_number in range(args.rounds):
            label = f"race-{round_number}"
            path, base = setup(label)
            barrier = Barrier(2, timeout=30)

            def commit(column):
                change = schema_change(base, column)
                barrier.wait()
                return call(label + "/" + column, "POST", path, change)

            with ThreadPoolExecutor(max_workers=2) as pool:
                pending = [pool.submit(commit, column) for column in ("writer_a", "writer_b")]
                responses = [(column, future.result()) for column, future in
                             zip(("writer_a", "writer_b"), pending)]
            if sum(200 <= r["status"] < 300 for _, r in responses) != 1:
                raise RuntimeError(f"{label}: expected exactly one acknowledgment")
            for _, response in responses:
                if response["status"] == 503 and "table_schema_pkey" in response["response_body"]:
                    duplicate_errors += 1
                else:
                    expect(response, 200, 204, 409)
            rounds.append(observe(label, path, base, responses))
            print(f'{label}: {rounds[-1]["statuses"]}', flush=True)

    summary = {"catalog": args.catalog, "warehouse": args.warehouse,
               "control": control, "rounds": rounds, "duplicate_key_503": duplicate_errors,
               "reproduced": duplicate_errors > 0,
               "status_pairs": dict(Counter("/".join(map(str, sorted(r["statuses"]))) for r in rounds)),
               "note": "An absent column does not prove that a 503 commit was rejected."}
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("reproduced", "duplicate_key_503", "status_pairs")}, indent=2))


if __name__ == "__main__":
    main()

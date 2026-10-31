#!/usr/bin/env python3
"""Exercise concurrent Iceberg REST commits without SDK retries or faults."""

import argparse
import collections
import concurrent.futures
import http.client
import json
import pathlib
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def request(method, url, body=None):
    payload = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=payload, method=method, headers={"Content-Type": "application/json"}
    )
    started = time.time_ns()
    observed_status = None
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            observed_status = response.status
            status, raw = observed_status, response.read()
    except urllib.error.HTTPError as exc:
        try:
            status, raw = exc.code, exc.read()
        except (OSError, http.client.HTTPException) as read_error:
            return {"started_ns": started, "finished_ns": time.time_ns(), "status": exc.code, "error": str(read_error)}
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
        return {"started_ns": started, "finished_ns": time.time_ns(), "status": None, "observed_status": observed_status, "error": str(exc)}
    return {
        "started_ns": started,
        "finished_ns": time.time_ns(),
        "status": status,
        "body": raw.decode("utf-8", "replace"),
    }


def require_success(result, operation):
    if result["status"] is None or not 200 <= result["status"] < 300:
        raise RuntimeError(f"{operation}: {result}")
    return json.loads(result["body"]) if result["body"] else {}


def unknown_outcome(event):
    status = event["status"]
    return (status is None or "error" in event or status >= 500
            or (not 200 <= status < 300 and status not in (400, 401, 403, 404, 409, 419)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8183/iceberg")
    parser.add_argument("--writers", type=int, default=8)
    parser.add_argument("--per-writer", type=int, default=25)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.writers < 1 or args.per_writer < 1:
        parser.error("writer and request counts must be positive")
    if args.output.exists():
        parser.error("output already exists")
    parsed_url = urllib.parse.urlsplit(args.url)
    if (parsed_url.scheme not in ("http", "https") or not parsed_url.hostname
            or parsed_url.username or parsed_url.password or parsed_url.query or parsed_url.fragment):
        parser.error("URL must be an HTTP(S) catalog base without credentials, a query, or a fragment")
    base = args.url.rstrip("/")
    config = require_success(request("GET", base + "/v1/config"), "load config")
    prefix = config.get("overrides", {}).get("prefix", config.get("defaults", {}).get("prefix", ""))
    path = "/v1/" + (urllib.parse.quote(prefix, safe="") + "/" if prefix else "")
    namespace = "context_probe_" + uuid.uuid4().hex[:12]
    table = "t_" + uuid.uuid4().hex[:12]
    root = base + path + "namespaces"
    require_success(request("POST", root, {"namespace": [namespace]}), "create namespace")
    tables = root + "/" + urllib.parse.quote(namespace, safe="") + "/tables"
    schema = {"type": "struct", "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]}
    created = require_success(
        request("POST", tables, {"name": table, "schema": schema, "stage-create": False}),
        "create table",
    )
    table_uuid = created.get("metadata", {}).get("table-uuid")
    if not isinstance(table_uuid, str) or not table_uuid:
        raise RuntimeError("create-table response did not contain a table UUID")
    table_url = tables + "/" + urllib.parse.quote(table, safe="")
    barrier = threading.Barrier(args.writers)

    def writer(index):
        events = []
        barrier.wait()
        for seq in range(args.per_writer):
            key = f"probe.writer-{index}.{seq}"
            body = {"requirements": [], "updates": [{"action": "set-properties", "updates": {key: "1"}}]}
            outcome = request("POST", table_url, body)
            if outcome["status"] is not None and 200 <= outcome["status"] < 300:
                # Successful responses can contain vended storage credentials.
                outcome.pop("body", None)
            events.append({"writer": index, "sequence": seq, "key": key, "request": body, **outcome})
        return events

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.writers) as pool:
        futures = [pool.submit(writer, index) for index in range(args.writers)]
        events = [event for future in futures for event in future.result()]
    final_read = request("GET", table_url)
    final_metadata = None
    final_error = None
    if final_read["status"] is not None and 200 <= final_read["status"] < 300:
        try:
            final_metadata = json.loads(final_read["body"])["metadata"]
            if not isinstance(final_metadata, dict) or not isinstance(final_metadata.get("properties", {}), dict):
                raise ValueError("invalid table metadata")
            if table_uuid and final_metadata.get("table-uuid") != table_uuid:
                final_error = "table UUID changed during the run"
                final_metadata = None
        except (ValueError, KeyError, TypeError):
            final_error = "final read did not contain usable metadata"
            final_metadata = None
    else:
        final_error = "final read failed; unknown outcomes remain unresolved"
    counts = collections.Counter(str(event["status"]) for event in events)
    unknown_visible = None
    if final_metadata is not None:
        properties = final_metadata.get("properties", {})
        unknown_visible = [event["key"] for event in events if unknown_outcome(event) and properties.get(event["key"]) == "1"]
    result = {
        "catalog": base,
        "namespace": namespace,
        "table": table,
        "table_uuid": table_uuid,
        "writers": args.writers,
        "per_writer": args.per_writer,
        "status_counts": dict(counts),
        "unknown_updates_visible": unknown_visible,
        "final_read": {
            key: value for key, value in final_read.items()
            if key != "body" or final_read["status"] is None or not 200 <= final_read["status"] < 300
        },
        "final_error": final_error,
        "events": sorted(events, key=lambda event: event["started_ns"]),
        "final_metadata": final_metadata,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as target:
        json.dump(result, target, indent=2)
        target.write("\n")
    print(json.dumps({key: value for key, value in result.items() if key not in {"events", "final_metadata", "final_read"}}))


if __name__ == "__main__":
    main()

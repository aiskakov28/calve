<!-- Draft for review. Do not post upstream until the user requests it. -->

Proposed title: Concurrent Iceberg REST table commits sometimes return HTTP 500 with inactive request context

### What happened

On Nessie 0.109.0, we sometimes get HTTP 500 when two writers send updates to
different properties on the same Iceberg REST table at the same time. The
server log shows `ContextNotActiveException` when it tries to use the
request-scoped `CatalogProducers.objectIO` proxy to write table metadata.
This happens without injected faults, a network proxy, or client retries.
With just one writer, the same workload completed without a 500.

Normal conflicts are expected when writers overlap. The unexpected part here
is a commit failing because an internal request context is inactive. An
HTTP 500 by itself does not establish a violation of the Iceberg REST
specification.

We used two separate Docker Compose projects, each with fresh PostgreSQL and
S3-compatible storage volumes. Each run created a new table and sent 200
`set-properties` commits, with empty requirements and a different property
key for each request. Here are the results:

| Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 800 | 800 | 0 | 0 | 0 |
| 2 | 10 | 2,000 | 1,313 | 675 | 12 | 7 |

Each of the 12 HTTP 500 responses named a different metadata file. For each
one, we found exactly one matching server HTTP/500 stack trace between the
start and end of that request. All 12 included `ContextNotActiveException`,
`ClientProxy.writeObject`, and `TasksServiceImpl.finalResult`.

We also checked the final table state after each run. Every update that
received HTTP 200 was still there, and none of the updates rejected with
HTTP 409 appeared. The 12 updates that received HTTP 500 were absent. So
those commit outcomes remain **unknown**: their absence alone does not prove
rejection or data loss.

Here is one request from a two-writer run:

```http
POST /iceberg/v1/main/namespaces/context_probe_e5881ec3f1e8/tables/t_0887a58a029a
Content-Type: application/json

{"requirements":[],"updates":[{"action":"set-properties","updates":{"probe.writer-0.13":"1"}}]}
```

It returned HTTP 500 with `RuntimeException: Failed to write snapshot to:
s3://calve/nessie/context_probe_e5881ec3f1e8/t_0887a58a029a_6b3d3fc5-f4c0-45d5-acaf-34faa5279ad7/metadata/00000-f3bb529b-dfa0-417d-bbf3-906676349d0c.metadata.json`.
The matching server trace included these lines (shortened):

```text
at org.projectnessie.nessie.tasks.service.impl.TasksServiceImpl.finalResult(TasksServiceImpl.java:161)
Caused by: jakarta.enterprise.context.ContextNotActiveException: RequestScoped context was not active ... CatalogProducers
at org.projectnessie.catalog.files.api.CatalogProducers_ProducerMethod_objectIO_..._ClientProxy.writeObject(Unknown Source)
```

The empty `requirements` array and `set-properties` update follow the format
in the [Iceberg REST OpenAPI request schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3476)
and [update schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L2896-L2908).

### How to reproduce it

Save these two files in an otherwise empty directory. The credentials are
only for this local test. The setup does not expose ports on the host, and
the image digests pin the versions used.

`compose.yaml`:

```yaml
services:
  db:
    image: postgres@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929
    environment:
      POSTGRES_PASSWORD: local-test-only
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d postgres"]
      interval: 2s
      retries: 30
    volumes:
      - db-data:/var/lib/postgresql/data
  silo:
    image: pgsty/silo@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3
    command: server /data
    environment:
      MINIO_ROOT_USER: calve
      MINIO_ROOT_PASSWORD: local-test-only
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 2s
      retries: 30
    volumes:
      - silo-data:/data
  bucket:
    image: pgsty/silo@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3
    depends_on:
      silo: {condition: service_healthy}
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        mc alias set local http://silo:9000 calve local-test-only &&
        mc mb --ignore-existing local/calve
    restart: "no"
  nessie:
    image: ghcr.io/projectnessie/nessie@sha256:61a1a14e73d76ac3851e7bef627e4ef426f4f0cf71c352843908a5834acd19e1
    environment:
      nessie.version.store.type: JDBC2
      nessie.version.store.persist.jdbc.datasource: postgresql
      quarkus.datasource.postgresql.jdbc.url: jdbc:postgresql://db:5432/postgres
      quarkus.datasource.postgresql.username: postgres
      quarkus.datasource.postgresql.password: local-test-only
      nessie.catalog.default-warehouse: calve
      nessie.catalog.warehouses.calve.location: s3://calve/nessie/
      nessie.catalog.service.s3.default-options.endpoint: http://silo:9000/
      nessie.catalog.service.s3.default-options.region: local-01
      nessie.catalog.service.s3.default-options.path-style-access: "true"
      nessie.catalog.service.s3.default-options.access-key: urn:nessie-secret:quarkus:nessie.catalog.secrets.access-key
      nessie.catalog.secrets.access-key.name: calve
      nessie.catalog.secrets.access-key.secret: local-test-only
    depends_on:
      db: {condition: service_healthy}
      bucket: {condition: service_completed_successfully}
  client:
    image: python@sha256:2d44738ebf4ce5f63b9d9ea871e9c7cfdc0426f7b0c97cac915fd88ded1d86bb
    profiles: [manual]
    volumes:
      - ./repro.py:/work/repro.py:ro
      - ./out:/out
volumes:
  db-data:
  silo-data:
```

`repro.py` (Python standard library only):

```python
import argparse
import collections
import concurrent.futures
import http.client
import json
import pathlib
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = "http://nessie:19120/iceberg"

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

opener = urllib.request.build_opener(NoRedirect())

def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    result = {"status": None, "start_ns": str(time.time_ns())}
    try:
        try:
            response = opener.open(req, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result["status"] = response.status
            raw = response.read()
        try:
            result["body"] = json.loads(raw) if raw else {}
        except ValueError as error:
            result["error"] = f"Invalid JSON response ({type(error).__name__})"
            if not 200 <= result["status"] < 300:
                result["body"] = {"raw_body": raw.decode("utf-8", "replace")}
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    result["finish_ns"] = str(time.time_ns())
    return result

def must_succeed(result):
    assert not result.get("error") and result["status"] is not None and 200 <= result["status"] < 300, result
    return result["body"]

def save(path, output):
    # Replace only after the new JSON is complete, preserving earlier evidence.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as target:
        json.dump(output, target, indent=2)
    pathlib.Path(target.name).replace(path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--writers", type=int, required=True)
    parser.add_argument("--per-writer", type=int, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert args.writers > 0 and args.per_writer > 0
    # Reserve the output before making any requests; never reuse an old history.
    with args.out.open("x", encoding="utf-8") as target:
        json.dump({"validation": "not started"}, target)
    for _ in range(120):
        config = call("GET", "/v1/config")
        if config["status"] == 200 and not config.get("error"):
            break
        time.sleep(1)
    config = must_succeed(config)
    prefix = config.get("overrides", {}).get("prefix", config.get("defaults", {}).get("prefix", ""))
    route = "/v1/" + (urllib.parse.quote(prefix, safe="") + "/" if prefix else "")
    ns = "context_probe_" + uuid.uuid4().hex[:12]
    table = "t_" + uuid.uuid4().hex[:12]
    namespaces = route + "namespaces"
    must_succeed(call("POST", namespaces, {"namespace": [ns]}))
    tables = namespaces + "/" + ns + "/tables"
    schema = {"type": "struct", "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]}
    created = must_succeed(call("POST", tables, {"name": table, "schema": schema, "stage-create": False}))
    table_uuid = created["metadata"]["table-uuid"]
    table_path = tables + "/" + table
    barrier = threading.Barrier(args.writers)

    def writer(index):
        barrier.wait()
        events = []
        for seq in range(args.per_writer):
            key = f"probe.writer-{index}.{seq}"
            request = {"requirements": [], "updates": [{"action": "set-properties", "updates": {key: "1"}}]}
            result = call("POST", table_path, request)
            # Successful responses can include vended credentials: retain only errors.
            events.append({"writer": index, "sequence": seq, "key": key,
                           "request": request, "status": result["status"],
                           "error": result.get("body") if result["status"] is None or not 200 <= result["status"] < 300 else None,
                           "response_error": result.get("error"),
                           "start_ns": result["start_ns"], "finish_ns": result["finish_ns"]})
        return events

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.writers) as pool:
        groups = list(pool.map(writer, range(args.writers)))
    events = sorted((item for group in groups for item in group),
                    key=lambda event: int(event["start_ns"]))
    counts = dict(collections.Counter(str(e["status"]) for e in events))
    unknown = [e["key"] for e in events if e["status"] not in (200, 409) or e["response_error"]]
    output = {"namespace": ns, "table": table, "table_uuid": table_uuid,
              "status_counts": counts, "unknown_updates": unknown, "events": events,
              "validation": "pending"}
    save(args.out, output)
    try:
        result = call("GET", table_path)
        output["final_read"] = {k: v for k, v in result.items() if k != "body"}
        if result["status"] is None or not 200 <= result["status"] < 300:
            output["final_read"]["body"] = result.get("body")
        final = must_succeed(result)["metadata"]
        output["final_table_uuid"] = final["table-uuid"]
        properties = output["final_properties"] = final.get("properties", {})
        assert final["table-uuid"] == table_uuid, "final table UUID changed"
        assert all(properties.get(e["key"]) == "1" for e in events if e["status"] == 200 and not e["response_error"]), "acknowledged update missing"
        assert all(e["key"] not in properties for e in events if e["status"] == 409 and not e["response_error"]), "rejected update present"
        output["validation"] = "passed"
    except Exception as error:
        output["validation"] = "failed"
        output["validation_error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(args.out, output)
    print(json.dumps({k: v for k, v in output.items() if k not in ("events", "final_properties")}))

if __name__ == "__main__":
    main()
```

Run these commands from that directory in a POSIX shell, or in WSL with Docker
access. Each execution uses a new project name and output directory, so the
database and object-store volumes start empty and previous results stay
separate. If the output directory already exists, the commands stop before
starting Docker.

Each client run creates a new table and saves its request history before
checking the final table state. The exit trap saves the server log and stops
the stack even if a client command fails. Since the 500 does not occur in
every run, the commands below repeat the two-writer case five times:

```sh
(
set -e
project="nessie-context-$(date +%s)-$$"
mkdir -p out
mkdir "out/$project"
trap 'docker compose -p "$project" logs --no-color nessie > "out/$project/nessie-server.log"; docker compose -p "$project" stop' 0
docker compose -p "$project" up -d nessie
docker compose -p "$project" run --rm client python -B /work/repro.py --writers 1 --per-writer 200 --out "/out/$project/single-before.json"
for n in 1 2 3 4 5; do
  docker compose -p "$project" run --rm client python -B /work/repro.py --writers 2 --per-writer 100 --out "/out/$project/two-$n.json"
done
docker compose -p "$project" run --rm client python -B /work/repro.py --writers 1 --per-writer 200 --out "/out/$project/single-after.json"
)
```

Look for `"500"` in `status_counts`. Then find that response's metadata path
in `out/<project>/nessie-server.log`. The 409s are normal concurrent conflicts
and are counted separately.

If the script cannot fully read or parse a response, it keeps the received
status code and records the error in `response_error`. That commit stays in
`unknown_updates` and is left out of the success/conflict checks.

We also ran this setup and workload on a third fresh stack, using equivalent
Docker Compose commands. Both single-writer controls passed: 400 successful
requests out of 400. The five two-writer runs sent 1,000 requests in total:
619 returned HTTP 200, 365 returned HTTP 409, and 16 returned HTTP 500.
Four of those five runs had a 500.

Each of those 16 errors matched a different inactive `objectIO` server trace.
Every update with a successful response was still there in the final table
reads. After these live runs, we improved the script's response handling and
how it saves results. We checked those changes separately with simulated
catalog responses; the workload stayed the same.

### Nessie server type (docker/uber-jar/built from source) and version

We used the official Docker image `ghcr.io/projectnessie/nessie:0.109.0`,
pinned above to
`sha256:61a1a14e73d76ac3851e7bef627e4ef426f4f0cf71c352843908a5834acd19e1`.
The rest of the setup was Java 21.0.12.1, Quarkus 3.40.1, JDBC2 with
PostgreSQL 17.6, and Silo `RELEASE.2026-09-03T13-18-01Z` for S3-compatible
storage. Authentication and authorization were disabled for this local test.

### Client type (Ex: UI/Spark/pynessie ...) and version

The script above uses Python 3.13.18's standard library to send requests
directly to the Iceberg REST API. It sends each commit once and does not
follow HTTP redirects.

### Additional information

We checked existing issues and PRs on 2026-10-31 and did not find an exact
match. [Nessie #9852](https://github.com/projectnessie/nessie/issues/9852)
reports a different `ConfigProducers` request-scope exception during a
startup health check.

One possible explanation, **still unconfirmed**, is that an async callback
tries to write metadata without the request context it needs. Nessie's
[`objectIO` producer](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L230)
is request scoped, and a
[`CompletableFuture` continuation](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/catalog/service/impl/src/main/java/org/projectnessie/catalog/service/impl/CatalogServiceImpl.java#L676-L682)
can reach its metadata write from
[`TasksServiceImpl.finalResult`](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L159-L164).
Those three files are identical in 0.109.0 and the `main` commit linked above.
We have not run an unreleased build, and we do not yet know exactly where the
request context becomes unavailable.

Found by @aldusialn and @aiskakov28.

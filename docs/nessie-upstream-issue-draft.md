<!-- Draft for review. Do not post upstream until the user requests it. -->

Proposed title: Concurrent Iceberg REST table commits intermittently return HTTP 500 with inactive request context

### What happened

On Nessie 0.109.0, two clients committing distinct table properties to the
same Iceberg REST table intermittently receive HTTP 500. The server reports
`ContextNotActiveException` while accessing the request-scoped
`CatalogProducers.objectIO` proxy during a metadata write. This occurs without
injected faults, a network proxy, or client retries. The same workload with
one writer completed without a 500.

Expected: under contention, a commit can succeed or return an ordinary
conflict response, but it should not fail because an internal request context
is inactive. I am reporting the availability failure, not claiming that every
HTTP 500 violates the Iceberg REST specification.

Each run created a fresh table and sent 200 valid `set-properties` commits
with empty requirements and unique keys. Across two separate fresh Docker
Compose projects with their own PostgreSQL and S3-compatible storage volumes:

| Writers | Runs | Requests | HTTP 200 | HTTP 409 | HTTP 500 | Runs with 500 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 800 | 800 | 0 | 0 | 0 |
| 2 | 10 | 2,000 | 1,313 | 675 | 12 | 7 |

All 12 HTTP 500s reported distinct metadata locations. Each matched exactly
one server HTTP/500 trace in the request time window, with
`ContextNotActiveException`, `ClientProxy.writeObject`, and
`TasksServiceImpl.finalResult`. All acknowledged property updates remained
in their matching final table reads; none of the 409-rejected updates appeared.
The 12 HTTP 500 outcomes remain **unknown**. Their properties were absent in
the final reads, which does not prove the commits were rejected or establish
data loss.

One of the two-writer requests was:

```http
POST /iceberg/v1/main/namespaces/context_probe_e5881ec3f1e8/tables/t_0887a58a029a
Content-Type: application/json

{"requirements":[],"updates":[{"action":"set-properties","updates":{"probe.writer-0.13":"1"}}]}
```

It returned HTTP 500 with `RuntimeException: Failed to write snapshot to:
s3://calve/nessie/context_probe_e5881ec3f1e8/t_0887a58a029a_6b3d3fc5-f4c0-45d5-acaf-34faa5279ad7/metadata/00000-f3bb529b-dfa0-417d-bbf3-906676349d0c.metadata.json`.
The matching server trace contained these frames (abridged):

```text
at org.projectnessie.nessie.tasks.service.impl.TasksServiceImpl.finalResult(TasksServiceImpl.java:161)
Caused by: jakarta.enterprise.context.ContextNotActiveException: RequestScoped context was not active ... CatalogProducers
at org.projectnessie.catalog.files.api.CatalogProducers_ProducerMethod_objectIO_..._ClientProxy.writeObject(Unknown Source)
```

The empty `requirements` array and `set-properties` update have the shape
defined by the [Iceberg REST OpenAPI request schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L3460-L3476)
and [update schema](https://github.com/apache/iceberg/blob/ccb8bc435062171e64bc8b7e5f56e6aed9c5b934/open-api/rest-catalog-open-api.yaml#L2896-L2908).

### How to reproduce it

Save these two files in an otherwise empty directory. The credentials below
are local test values. The stack publishes no host ports. It uses immutable
image digests and creates a fresh table for every run.

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
import json
import pathlib
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
    start = str(time.time_ns())
    try:
        with opener.open(req, timeout=5) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return {"status": None, "error": str(error), "start_ns": start,
                "finish_ns": str(time.time_ns())}
    return {"status": status, "body": json.loads(raw) if raw else {},
            "start_ns": start, "finish_ns": str(time.time_ns())}

def must_succeed(result):
    assert result["status"] is not None and 200 <= result["status"] < 300, result
    return result["body"]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--writers", type=int, required=True)
    parser.add_argument("--per-writer", type=int, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert args.writers > 0 and args.per_writer > 0
    for _ in range(120):
        config = call("GET", "/v1/config")
        if config["status"] == 200:
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
                           "error": result.get("body") if result["status"] != 200 else None,
                           "transport_error": result.get("error"),
                           "start_ns": result["start_ns"], "finish_ns": result["finish_ns"]})
        return events

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.writers) as pool:
        groups = list(pool.map(writer, range(args.writers)))
    events = sorted((item for group in groups for item in group),
                    key=lambda event: int(event["start_ns"]))
    final = must_succeed(call("GET", table_path))["metadata"]
    assert final["table-uuid"] == table_uuid
    properties = final.get("properties", {})
    assert all(properties.get(e["key"]) == "1" for e in events if e["status"] == 200)
    assert all(e["key"] not in properties for e in events if e["status"] == 409)
    counts = dict(collections.Counter(str(e["status"]) for e in events))
    unknown = [e["key"] for e in events if e["status"] not in (200, 409)]
    output = {"namespace": ns, "table": table, "table_uuid": table_uuid,
              "status_counts": counts, "unknown_updates": unknown, "events": events,
              "final_properties": properties}
    with args.out.open("x", encoding="utf-8") as target:
        json.dump(output, target, indent=2)
    print(json.dumps({k: v for k, v in output.items() if k not in ("events", "final_properties")}))

if __name__ == "__main__":
    main()
```

Run from that directory in a POSIX shell (or WSL with Docker access). Use a
fresh project name each time, so the database and object-store volumes start
empty. Each command creates a new table and saves its raw request/status
history. A 500 is intermittent, so repeat the two-writer case:

```sh
project="nessie-context-$(date +%s)"
mkdir -p out
docker compose -p "$project" up -d nessie
docker compose -p "$project" run --rm client python -B /work/repro.py --writers 1 --per-writer 200 --out /out/single-before.json
for n in 1 2 3 4 5; do
  docker compose -p "$project" run --rm client python -B /work/repro.py --writers 2 --per-writer 100 --out "/out/two-$n.json"
done
docker compose -p "$project" run --rm client python -B /work/repro.py --writers 1 --per-writer 200 --out /out/single-after.json
docker compose -p "$project" logs --no-color nessie > out/nessie-server.log
docker compose -p "$project" stop
```

Look for `"500"` in `status_counts`; match that response's metadata path to
the corresponding block in `out/nessie-server.log`. The 409s are ordinary
concurrent conflicts and are separate from this issue.

I also ran the exact YAML and Python blocks above on a third fresh stack, using
equivalent Docker Compose commands: the two one-writer controls were 400/400
successful, and the five two-writer runs returned 619 HTTP 200, 365 HTTP 409,
and 16 HTTP 500 across 1,000 requests. Four of those five runs had a 500.
All 16 HTTP 500s again matched distinct inactive `objectIO` server traces;
acknowledged updates were retained in the matching final reads.

### Nessie server type (docker/uber-jar/built from source) and version

Official Docker image `ghcr.io/projectnessie/nessie:0.109.0`, pinned above to
`sha256:61a1a14e73d76ac3851e7bef627e4ef426f4f0cf71c352843908a5834acd19e1`;
Java 21.0.12.1, Quarkus 3.40.1, JDBC2 with PostgreSQL 17.6, and Silo
`RELEASE.2026-09-03T13-18-01Z` as S3-compatible storage. Authentication and
authorization were disabled in this local test.

### Client type (Ex: UI/Spark/pynessie ...) and version

Direct Iceberg REST requests from Python 3.13.18 standard library, using the
script above. No SDK retry logic or HTTP redirects on commits.

### Additional information

aiskakov28 first noticed the metadata-write 500 response in a broader catalog
sweep; this report adds the minimal two-writer reproduction and correlated
server traces. [Nessie #9852](https://github.com/projectnessie/nessie/issues/9852)
has a different `ConfigProducers` request-scope exception in a startup health
check. A search of existing issues and PRs on 2026-10-31 found no exact match.

Possible explanation, **unconfirmed**: Nessie's
[`objectIO` producer](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/servers/quarkus-catalog/src/main/java/org/projectnessie/server/catalog/CatalogProducers.java#L222-L230)
is request scoped, while a
[`CompletableFuture` continuation](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/catalog/service/impl/src/main/java/org/projectnessie/catalog/service/impl/CatalogServiceImpl.java#L676-L682)
can reach its metadata write from
[`TasksServiceImpl.finalResult`](https://github.com/projectnessie/nessie/blob/3a8733847ae0a1cf1c24ea694718fbf252912840/tasks/service/impl/src/main/java/org/projectnessie/nessie/tasks/service/impl/TasksServiceImpl.java#L159-L164).
Those three files are byte-for-byte unchanged between 0.109.0 and the checked
`main` commit, but an unreleased build was not run. The exact context boundary
that fails is not established.

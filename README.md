# Distributed Object Storage

A minimal but real distributed object-storage system built with Java 21 and
Spring Boot: objects are split into fixed-size chunks, every chunk is
replicated to two storage nodes, every byte is SHA-256-verified end to end,
any single storage node can die without making an object unreadable — and
since **milestone 2**, lost redundancy is rebuilt automatically: a
background repair worker detects missing and unreachable replicas and
re-creates them from a checksum-verified surviving copy, so that after
repair even a *second* node loss is survivable.

The system is deliberately small and honest about what it does not do yet —
see [Limitations](#limitations).

## Architecture

Two services, one database, no shared state between nodes:

```
                 PUT/GET/DELETE /api/objects/{key}
                              │
                              ▼
                    ┌──────────────────┐        ┌────────────────┐
                    │ metadata-service │───────▶│  PostgreSQL    │
                    │  (coordinator)   │        │  objects       │
                    └──────────────────┘        │  chunks        │
                       │      │      │          │  chunk_replicas│
        PUT/GET/DELETE │      │      │          └────────────────┘
        /chunks/{id}   ▼      ▼      ▼
                 ┌───────┐┌───────┐┌───────┐
                 │node-1 ││node-2 ││node-3 │   storage-node processes,
                 │ disk  ││ disk  ││ disk  │   each owning a local data dir
                 └───────┘└───────┘└───────┘
```

**metadata-service** owns the public API and all intelligence: chunking,
checksums, placement, replication, failover, and the PostgreSQL metadata
(schema managed by Flyway). **storage-node** is a deliberately dumb byte
store: `PUT/GET/DELETE /chunks/{uuid}` against a local directory, verifying
the declared SHA-256 on every write and reporting a freshly computed SHA-256
on every read. Nodes never talk to each other and hold no metadata.

### Write path (upload)

1. `PUT /api/objects/{key}` with the object bytes
   (`application/octet-stream`).
2. The object is split into fixed-size chunks (`storage.chunk-size-bytes`,
   default 1 MiB; the last chunk carries the remainder). SHA-256 is computed
   for the whole object and for every chunk.
3. Each chunk is placed **deterministically**: the node list is sorted by
   node id, `SHA-256(objectId:chunkIndex)` selects the primary slot, and the
   remaining replicas take the following slots ring-wise. Same input, same
   nodes — on any instance, with no coordination.
4. Every chunk is written to `storage.replication-factor` nodes (default
   **2**); the node verifies the declared checksum before the chunk becomes
   visible. **All replica writes must succeed.**
5. Only then is the metadata committed, in one transaction: object, chunks,
   and the replica locations that were *actually written*. A committed
   object is therefore always fully replicated. If any write fails, the
   upload aborts with `502`, already-written chunks are deleted best-effort,
   and no metadata exists.

### Read path (download)

1. `GET /api/objects/{key}` loads the object's chunk map from PostgreSQL —
   reads use the recorded replica locations, never the placement function.
   Replicas are recorded by node **id**, resolved against the configured
   node list at read time: reordering the list or moving a node to a new
   URL is safe while ids are preserved, but removing or renaming an id that
   still owns recorded replicas makes those replicas unreachable.
2. Each chunk is fetched from its replicas in recorded priority order. A
   replica fails the attempt if its node is unreachable **or** if the bytes
   do not hash to the chunk's recorded SHA-256 — a dead node and a corrupted
   replica are handled identically: try the next replica.
3. If no replica of some chunk yields intact bytes, the download fails with
   `502` (the metadata is intact; the read can succeed once a node returns).
4. The reassembled object is verified against the recorded object SHA-256
   and served with it in the `X-Object-Sha256` header. Body and header come
   from the **same metadata snapshot**: the plan is read once per download,
   so an object replaced under the same key mid-request can never pair one
   version's bytes with another version's checksum.

With replication factor 2, this is what makes **any single node loss
survivable** for reads.

### Delete path

Metadata is deleted first (the object disappears atomically), then chunk
bytes are deleted from the nodes best-effort. A node that is down during
delete orphans bytes; it can never resurrect the object.

### Failure and recovery (replica repair)

A background worker in metadata-service (`storage.repair.*`) restores
redundancy after a node loss. Every `storage.repair.interval` (default
30 s) it sweeps all chunks in pages and, for each chunk, probes every
recorded replica with a lightweight `HEAD /chunks/{id}`:

- **present** — the node is reachable and stores bytes under the chunk id;
- **missing** — the node is reachable but has lost the bytes (wiped disk,
  manual deletion);
- **unreachable** — the node is down (cached for the rest of the sweep, so
  a dead node costs one timeout, not one per chunk).

A chunk whose *present* count is below the replication factor is degraded
and gets repaired:

1. **Verify before:** a surviving copy is read and hashed against the
   chunk's recorded SHA-256. A copy that fails verification is never used
   as a repair source; if *no* copy verifies, the chunk is left untouched
   and logged — repair never invents data.
2. **Restore in place:** recorded nodes that are reachable but lost the
   bytes get the verified copy written back (no metadata change).
3. **Re-create elsewhere:** for replicas on unreachable nodes, new copies
   go to nodes chosen from the chunk's deterministic placement ring
   (skipping nodes that already hold one), until the replication factor is
   met or no live candidate remains.
4. **Verify after:** every written copy is read back and re-hashed. Only a
   copy proven by readback gets its replica row appended to the metadata —
   so recorded replicas never point at bytes that were not verified to
   exist.

The metadata append runs under a per-chunk row lock plus the existing
`(chunk_id, node_id)` / `(chunk_id, priority)` uniqueness constraints,
which makes repair **idempotent and concurrency-safe**: sweeps can run
repeatedly, and concurrent repairers (multiple threads or coordinator
instances) converge on exactly one recorded row per node — the loser
observes "already recorded" and adds nothing. Chunk writes themselves are
idempotent, and an object deleted mid-repair is detected at the record
step, with the freshly written copy removed best-effort.

Recovery semantics to know:

- Repair **maintains at least** the replication factor when enough nodes
  are live. With fewer live nodes than the factor, the deficit is logged
  and retried every sweep until capacity returns — copies are never piled
  onto one node to fake redundancy.
- Repair **never removes** replica rows. A lost node that returns with its
  disk intact leaves some chunks with more recorded replicas than the
  factor; that is tolerated (more read options), not trimmed.
- Detection is **presence-based**. At-rest bit rot on an otherwise healthy
  chunk is not found by the sweep (that would be a full-read scrub, which
  is future work) — it is detected by checksum wherever the bytes are
  actually read: downloads fail over past a rotted replica, and repair
  refuses it as a source.
- A **returning node is usable the moment its name resolves again**: the
  coordinator disables the JVM's negative DNS cache (10 s by default —
  a security property, so it cannot be overridden from the command line),
  which would otherwise keep a freshly restarted node invisible for
  seconds after it is healthy, failing writes placed on it.

### Data model

| Table | Contents |
|---|---|
| `objects` | id, unique `object_key`, size, chunk size, chunk count, object SHA-256 |
| `chunks` | id (the id used on the nodes), object FK, index, size, chunk SHA-256 |
| `chunk_replicas` | chunk FK, `node_id`, priority (read-preference order) |

## API

| Method | Path | Behavior |
|---|---|---|
| `PUT` | `/api/objects/{key}` | Upload (octet-stream body). `201` + metadata JSON; `409` if the key exists; `400` empty body/invalid key; `413` over the size cap; `502` if replicas cannot be placed |
| `GET` | `/api/objects/{key}` | Download. `200` bytes + `X-Object-Sha256`; `404`; `502` if some chunk has no intact replica |
| `GET` | `/api/objects/{key}/metadata` | Chunk map: sizes, checksums, and the nodes holding each replica |
| `DELETE` | `/api/objects/{key}` | `204`; removes metadata, best-effort removes chunk bytes |

Keys match `[A-Za-z0-9][A-Za-z0-9._-]{0,199}`. There is no overwrite in
milestone 1: delete first, then re-upload.

## Local development

Requirements: Java 21, Docker, Git.

```bash
cp .env.example .env                     # fill in local values

# build the storage-node image input, then start the infrastructure:
(cd services/storage-node && ./mvnw -DskipTests package)
docker compose up -d --wait              # metadata-db (5433), node-1..3 (9001..9003)

# run the coordinator on the host:
cd services/metadata-service
set -a; source ../../.env; set +a
./mvnw spring-boot:run                   # API on http://localhost:8080
```

Try it:

```bash
head -c 3000000 /dev/urandom > /tmp/blob
curl -sS -X PUT --data-binary @/tmp/blob -H 'Content-Type: application/octet-stream' \
     http://localhost:8080/api/objects/demo | python3 -m json.tool
curl -sS http://localhost:8080/api/objects/demo -o /tmp/blob.out
cmp /tmp/blob /tmp/blob.out && echo "byte-identical"
curl -sS http://localhost:8080/api/objects/demo/metadata | python3 -m json.tool
docker stop objectstore-node-2           # kill a node
curl -sS http://localhost:8080/api/objects/demo -o /dev/null -w '%{http_code}\n'   # still 200

# within ~30s the repair worker re-replicates node-2's chunks onto the
# surviving nodes — watch the extra replica appear per affected chunk:
sleep 35 && curl -sS http://localhost:8080/api/objects/demo/metadata | python3 -m json.tool

docker start objectstore-node-2
curl -sS -X DELETE http://localhost:8080/api/objects/demo -w '%{http_code}\n'
```

## Tests

Docker must be running — integration tests use Testcontainers.

```bash
(cd services/storage-node && ./mvnw test)      # node wire contract, checksums, presence probes,
                                               # traversal guard
(cd services/metadata-service && ./mvnw test)  # chunking, placement, and the coordinator against
                                               # real PostgreSQL + scriptable fake nodes
                                               # (down nodes, corrupted replicas, failed uploads),
                                               # plus the repair sweep: lost/missing replicas,
                                               # verified-source refusal, idempotency, and a
                                               # deterministic concurrent-repair race

# end-to-end: real PostgreSQL + three real storage-node containers + the
# metadata service, including genuine node losses (containers stopped) and
# automatic repair making a second node loss survivable:
(cd services/metadata-service && ./mvnw -DskipTests package)
(cd services/storage-node && ./mvnw -DskipTests package)
(cd integration-tests && ./mvnw test)
```

CI (`.github/workflows/ci.yml`) runs both service suites and the e2e suite
on every push to `main` and every pull request.

## Limitations

Deliberate boundaries — the system is honest about them rather than
pretending otherwise:

- **Repair restores replica counts, not everything.** The sweep detects
  missing and unreachable replicas, not silent at-rest corruption on
  otherwise healthy chunks (no full-read scrubbing yet — rot is caught by
  checksum at read time and routed around). Repair also never trims: a
  returned node can leave chunks above the replication factor. A window
  between a loss and the completing sweep still exists in which a second
  loss can make chunks unreadable; and if every replica of a chunk is lost
  or corrupt, repair refuses to guess — the data is gone and the logs say
  so.
- **Static membership.** Nodes are configuration, not discovery, and node
  ids are load-bearing: replicas are recorded against them. Reordering the
  list or changing a node's URL is safe while ids are preserved; adding
  nodes changes placement for *new* objects only; but **removing or
  renaming an id that still owns recorded replicas makes those replicas
  unreachable** — repair can re-create a chunk's copies elsewhere only
  while at least one of its recorded ids still resolves and serves intact
  bytes. There is no rebalancing or migration; dynamic membership is
  future work.
- **Modulo placement, not consistent hashing.** Changing the node count
  remaps future placements wholesale. Fine at this scale; consistent
  hashing comes later.
- **Writes need every replica.** An upload fails (502, nothing committed)
  if any placement-selected node is down. There is no write quorum or
  hinted handoff.
- **Whole objects in memory.** Upload and download buffer the full object
  in the coordinator (capped by `storage.max-object-size-bytes`, default
  64 MiB). No streaming, no multipart, no range reads.
- **Orphaned chunk bytes are possible** (failed upload cleanup, node down
  during delete, an object deleted mid-repair after the copy was written).
  Metadata is never wrong about what exists; nodes may hold garbage that a
  future scrubber should collect.
- **Single coordinator, no auth, plain HTTP.** metadata-service is one
  process and the trust boundary is the network.
- **Not S3.** No S3 API compatibility, signed URLs, versioning, or
  multipart. Redis and cloud deployment are also out of scope for now.

## Stack

Java 21 · Spring Boot · PostgreSQL 17 + Flyway · Docker / Docker Compose ·
Testcontainers · GitHub Actions

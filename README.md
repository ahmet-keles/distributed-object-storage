# Distributed Object Storage

A minimal but real distributed object-storage system built with Java 21 and
Spring Boot: objects are split into fixed-size chunks, every chunk is
replicated to two storage nodes, every byte is SHA-256-verified end to end,
and any single storage node can die without making an object unreadable.

This is **milestone 1**. It is deliberately small and honest about what it
does not do yet — see [Limitations](#limitations).

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
   reads use the recorded replica locations, never the placement function,
   so reconfiguring the node list cannot orphan existing objects.
2. Each chunk is fetched from its replicas in recorded priority order. A
   replica fails the attempt if its node is unreachable **or** if the bytes
   do not hash to the chunk's recorded SHA-256 — a dead node and a corrupted
   replica are handled identically: try the next replica.
3. If no replica of some chunk yields intact bytes, the download fails with
   `502` (the metadata is intact; the read can succeed once a node returns).
4. The reassembled object is verified against the recorded object SHA-256
   and served with it in the `X-Object-Sha256` header.

With replication factor 2, this is what makes **any single node loss
survivable** for reads.

### Delete path

Metadata is deleted first (the object disappears atomically), then chunk
bytes are deleted from the nodes best-effort. A node that is down during
delete orphans bytes; it can never resurrect the object.

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
docker start objectstore-node-2
curl -sS -X DELETE http://localhost:8080/api/objects/demo -w '%{http_code}\n'
```

## Tests

Docker must be running — integration tests use Testcontainers.

```bash
(cd services/storage-node && ./mvnw test)      # node wire contract, checksums, traversal guard
(cd services/metadata-service && ./mvnw test)  # chunking, placement, and the coordinator against
                                               # real PostgreSQL + scriptable fake nodes
                                               # (down nodes, corrupted replicas, failed uploads)

# end-to-end: real PostgreSQL + three real storage-node containers + the
# metadata service, including a genuine node-loss (container stopped):
(cd services/metadata-service && ./mvnw -DskipTests package)
(cd services/storage-node && ./mvnw -DskipTests package)
(cd integration-tests && ./mvnw test)
```

CI (`.github/workflows/ci.yml`) runs both service suites and the e2e suite
on every push to `main` and every pull request.

## Limitations

Deliberate milestone-1 boundaries — the system is honest about them rather
than pretending otherwise:

- **No repair or re-replication.** A lost node's replicas are not rebuilt;
  reads survive on the remaining replica, but redundancy is not restored,
  and a second loss can make objects unreadable.
- **Static membership.** Nodes are configuration, not discovery. Adding or
  removing nodes changes placement for *new* objects only (reads always use
  recorded locations); there is no rebalancing.
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
  during delete). Metadata is never wrong about what exists; nodes may hold
  garbage that a future scrubber should collect.
- **Single coordinator, no auth, plain HTTP.** metadata-service is one
  process and the trust boundary is the network.
- **Not S3.** No S3 API compatibility, signed URLs, versioning, or
  multipart. Redis and cloud deployment are also out of scope for now.

## Stack

Java 21 · Spring Boot · PostgreSQL 17 + Flyway · Docker / Docker Compose ·
Testcontainers · GitHub Actions

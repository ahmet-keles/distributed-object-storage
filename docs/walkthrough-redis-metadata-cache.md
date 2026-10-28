# Redis metadata cache walkthrough

1. The metadata endpoint often reads the same object's chunk and replica plan.
2. Redis stores that plan under `object-metadata:<object-key>`.
3. `ObjectStorageService.metadata` checks Redis before querying PostgreSQL.
4. On a miss, it loads the plan from PostgreSQL and serializes it as JSON.
5. Entries expire after 60 seconds by default; `METADATA_CACHE_TTL` changes this.
6. A successful upload or delete evicts the cached value for that key.
7. Successful background replica repairs also evict the object's cached plan.
8. `compose.yaml` starts Redis on localhost port 6379 for development.
9. Integration tests use real PostgreSQL and Redis Testcontainers.
10. Tests cover misses, the following cached read, and upload/delete eviction.

### Likely interview questions

**Q: What data is cached?** A: The detached object plan, including chunk and replica metadata.
**Q: How do writes keep the cache current?** A: A successful metadata save or delete evicts the key.
**Q: Why have a TTL if writes evict?** A: It bounds stale data if an eviction is missed or old keys remain.

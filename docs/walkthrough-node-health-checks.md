# Storage-node health checks walkthrough

1. The coordinator periodically checks `/actuator/health` on every configured storage node.
2. The default probe interval is five seconds and can be changed with `STORAGE_HEALTH_INTERVAL`.
3. New nodes start as `UNKNOWN` until the first probe completes.
4. A successful probe reports the node as `HEALTHY`.
5. A failed or unavailable probe reports the node as `DOWN`.
6. The first success after `DOWN` reports `RECOVERING`.
7. A second consecutive success reports `HEALTHY` again.
8. `GET /api/nodes/health` returns each node ID, status, and last-check time.
9. The endpoint does not return configured node URLs.
10. The repair sweep skips chunk probes for nodes already known to be `DOWN`.
11. Healthy and recovering nodes still receive per-chunk presence checks before repair changes metadata.
12. Integration tests cover healthy, down, recovery, endpoint output, and repair's use of known-down status.
13. **Q: Why use `RECOVERING`?** A: One healthy response can be brief; a second consecutive response confirms recovery.
14. **Q: Does node-level health prove every replica exists?** A: No, repair still checks chunk presence individually.
15. **Q: What does repair do with a known-down node?** A: It treats its replicas as unavailable and avoids another request for each chunk.

# Object-storage benchmark walkthrough

1. The benchmark creates a uniquely named object with a configurable payload size.
2. It times the upload request and computes upload throughput in MiB per second.
3. It times a full download and compares every returned byte with the uploaded payload.
4. It samples the metadata endpoint repeatedly and reports p50 and p95 latency.
5. It reads the object's recorded replicas directly and verifies each returned SHA-256.
6. The total replica bytes divided by object size is the measured storage overhead.
7. The script selects the storage node holding the most replicas of this object.
8. It stops that Docker container and confirms the object still downloads successfully.
9. It polls metadata and node HEAD endpoints until each chunk again has the requested number of present recorded replicas.
10. The time from a successful node stop to that recovery condition is reported.
11. A `finally` cleanup restarts the node and deletes the benchmark object.
12. README instructions cover service setup, command options, and a recorded local run.
13. **Q: What does replica overhead include?** A: The bytes stored on the replica nodes for this object, divided by its input size.
14. **Q: How does the script define recovery?** A: Each chunk has the configured number of reachable replicas recorded in metadata.
15. **Q: What happens if the benchmark fails during node loss?** A: Cleanup attempts to restart the stopped node and remove the generated object.

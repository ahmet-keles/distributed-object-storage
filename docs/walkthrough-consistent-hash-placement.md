# Consistent-hash placement walkthrough

1. Upload placement previously used a modulo slot, so adding a node reshuffled every slot.
2. The placement service now builds a SHA-256 ring from configured node ids.
3. Each physical node contributes 128 virtual points to spread the load.
4. A chunk hashes its stable object UUID and chunk index to a ring point.
5. Walking clockwise selects replica nodes in read-preference order.
6. Repeated virtual points for the same node are skipped.
7. The ring order is deterministic across service instances and config ordering.
8. Repair uses the same ring order to find distinct replacement nodes.
9. Adding a node affects ranges around its points, not all future chunk assignments.
10. Existing replicas stay where PostgreSQL records them; this does not rebalance stored data.
11. A deterministic 10,000-chunk test measures how many replica sets change after adding a node.

### Likely interview questions

**Q: Why use virtual nodes?** A: They give each physical node many ring ranges and balance load better.
**Q: How are replicas picked?** A: Start at the chunk's hash and walk clockwise, skipping duplicate physical nodes.
**Q: Does adding a node move existing bytes?** A: No; it changes future upload placement only. Existing-data rebalancing is separate work.

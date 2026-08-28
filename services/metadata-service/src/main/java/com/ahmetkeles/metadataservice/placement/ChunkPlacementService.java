package com.ahmetkeles.metadataservice.placement;

import com.ahmetkeles.metadataservice.config.StorageProperties;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Deterministic chunk placement over the configured node list.
 *
 * <p>The node list is sorted by node id, the chunk's identity
 * {@code objectId:chunkIndex} is hashed with SHA-256, and the first four
 * bytes select the primary slot; the remaining replicas take the following
 * slots ring-wise. The same input therefore always yields the same node set,
 * on any instance, with no coordination — while different chunks of the same
 * object spread across the cluster.
 *
 * <p>Placement decides where to WRITE. Reads never call this: they use the
 * replica locations recorded in the database. Reordering the node list or
 * changing a node's URL is therefore safe as long as node IDS are preserved
 * — but removing or renaming an id that still owns recorded replicas makes
 * those replicas unreachable (and, at replication factor 2, losing both ids
 * of a chunk makes the object unreadable). Membership is static in
 * milestone 1; dynamic membership, migration, and repair are future work.
 */
@Service
public class ChunkPlacementService {

    private final List<StorageProperties.Node> sortedNodes;
    private final int replicationFactor;

    public ChunkPlacementService(StorageProperties properties) {
        this.sortedNodes = properties.nodes().stream()
                .sorted(Comparator.comparing(StorageProperties.Node::id))
                .toList();
        this.replicationFactor = properties.replicationFactor();
    }

    /** Replica targets for one chunk, in priority (read-preference) order. */
    public List<StorageProperties.Node> replicasFor(UUID objectId,
                                                    int chunkIndex) {
        int nodeCount = sortedNodes.size();
        int primary = Math.floorMod(
                placementHash(objectId, chunkIndex), nodeCount);

        List<StorageProperties.Node> replicas = new ArrayList<>();
        for (int r = 0; r < replicationFactor; r++) {
            replicas.add(sortedNodes.get((primary + r) % nodeCount));
        }

        return replicas;
    }

    private static int placementHash(UUID objectId, int chunkIndex) {
        byte[] digest = sha256(
                (objectId + ":" + chunkIndex)
                        .getBytes(StandardCharsets.UTF_8));

        return ((digest[0] & 0xFF) << 24)
                | ((digest[1] & 0xFF) << 16)
                | ((digest[2] & 0xFF) << 8)
                | (digest[3] & 0xFF);
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }
}

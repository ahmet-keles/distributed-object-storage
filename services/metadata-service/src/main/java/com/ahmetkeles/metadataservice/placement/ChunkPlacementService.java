package com.ahmetkeles.metadataservice.placement;

import com.ahmetkeles.metadataservice.config.StorageProperties;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Deterministic chunk placement over a consistent-hash ring.
 *
 * <p>Each configured node owns virtual points on the ring. A chunk hashes to
 * one point, then walks clockwise to choose distinct physical nodes. Virtual
 * points spread load more evenly, while adding a node only takes the ranges
 * immediately preceding that node's points from their old owners.
 *
 * <p>Placement decides where to write new chunks. Reads use the replica
 * locations recorded in PostgreSQL, so changing membership never relocates
 * existing data automatically. Node ids must remain resolvable while they
 * own recorded replicas; repair can restore copies only when a valid source
 * replica remains available.
 */
@Service
public class ChunkPlacementService {

    private static final int VIRTUAL_NODES_PER_NODE = 128;

    private final NavigableMap<String, StorageProperties.Node> ring;
    private final int replicationFactor;
    private final int nodeCount;

    public ChunkPlacementService(StorageProperties properties) {
        this.replicationFactor = properties.replicationFactor();
        this.nodeCount = properties.nodes().size();
        this.ring = new TreeMap<>();
        properties.nodes().stream()
                .sorted(Comparator.comparing(StorageProperties.Node::id))
                .forEach(node -> {
                    for (int virtualNode = 0;
                            virtualNode < VIRTUAL_NODES_PER_NODE;
                            virtualNode++) {
                        String point = hash(node.id() + ":" + virtualNode);
                        ring.put(point, node);
                    }
                });
    }

    /** Replica targets for one chunk, in priority (read-preference) order. */
    public List<StorageProperties.Node> replicasFor(UUID objectId,
                                                    int chunkIndex) {
        return ringFor(objectId, chunkIndex).subList(0, replicationFactor);
    }

    /**
     * Every configured node in clockwise ring order for this chunk. Repair
     * walks this order and skips nodes already holding a recorded replica.
     */
    public List<StorageProperties.Node> ringFor(UUID objectId,
                                                int chunkIndex) {
        String chunkPoint = hash(objectId + ":" + chunkIndex);
        Map.Entry<String, StorageProperties.Node> cursor =
                ring.ceilingEntry(chunkPoint);
        if (cursor == null) {
            cursor = ring.firstEntry();
        }

        List<StorageProperties.Node> orderedNodes = new ArrayList<>();
        while (orderedNodes.size() < nodeCount) {
            StorageProperties.Node node = cursor.getValue();
            if (orderedNodes.stream().noneMatch(
                    existing -> existing.id().equals(node.id()))) {
                orderedNodes.add(node);
            }

            cursor = ring.higherEntry(cursor.getKey());
            if (cursor == null) {
                cursor = ring.firstEntry();
            }
        }
        return orderedNodes;
    }

    private static String hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            // A fixed-width lowercase hex string keeps unsigned ring order.
            return java.util.HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }
}

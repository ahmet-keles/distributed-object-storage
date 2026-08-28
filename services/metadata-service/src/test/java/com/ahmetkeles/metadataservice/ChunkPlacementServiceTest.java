package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.config.StorageProperties;
import com.ahmetkeles.metadataservice.placement.ChunkPlacementService;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkPlacementServiceTest {

    private final StorageProperties properties = new StorageProperties(
            1024, 2, 1_000_000,
            List.of(
                    new StorageProperties.Node("node-1", "http://n1"),
                    new StorageProperties.Node("node-2", "http://n2"),
                    new StorageProperties.Node("node-3", "http://n3")
            ));

    @Test
    void placementIsDeterministicAcrossInstances() {
        UUID objectId = UUID.randomUUID();

        ChunkPlacementService first = new ChunkPlacementService(properties);
        ChunkPlacementService second = new ChunkPlacementService(properties);

        for (int index = 0; index < 50; index++) {
            assertEquals(
                    first.replicasFor(objectId, index),
                    second.replicasFor(objectId, index),
                    "two instances must agree on chunk " + index);
        }
    }

    @Test
    void nodeListOrderDoesNotChangePlacement() {
        StorageProperties shuffled = new StorageProperties(
                1024, 2, 1_000_000,
                List.of(
                        new StorageProperties.Node("node-3", "http://n3"),
                        new StorageProperties.Node("node-1", "http://n1"),
                        new StorageProperties.Node("node-2", "http://n2")
                ));
        UUID objectId = UUID.randomUUID();

        assertEquals(
                new ChunkPlacementService(properties)
                        .replicasFor(objectId, 0),
                new ChunkPlacementService(shuffled)
                        .replicasFor(objectId, 0),
                "placement is over the id-sorted list, not config order");
    }

    @Test
    void everyChunkGetsReplicationFactorDistinctNodes() {
        ChunkPlacementService placement =
                new ChunkPlacementService(properties);
        UUID objectId = UUID.randomUUID();

        for (int index = 0; index < 100; index++) {
            List<StorageProperties.Node> replicas =
                    placement.replicasFor(objectId, index);

            assertEquals(2, replicas.size());
            assertEquals(2, Set.copyOf(replicas).size(),
                    "replicas of one chunk must land on distinct nodes");
        }
    }

    @Test
    void chunksOfOneObjectSpreadAcrossTheCluster() {
        ChunkPlacementService placement =
                new ChunkPlacementService(properties);
        UUID objectId = UUID.randomUUID();

        Set<String> usedNodes = new HashSet<>();
        for (int index = 0; index < 100; index++) {
            placement.replicasFor(objectId, index)
                    .forEach(node -> usedNodes.add(node.id()));
        }

        assertEquals(3, usedNodes.size(),
                "100 chunks over 3 nodes must use every node");
    }

    @Test
    void configurationRejectsUnsatisfiableReplication() {
        List<StorageProperties.Node> twoNodes = List.of(
                new StorageProperties.Node("node-1", "http://n1"),
                new StorageProperties.Node("node-2", "http://n2"));

        assertThrows(IllegalArgumentException.class, () ->
                new StorageProperties(1024, 3, 1_000_000, twoNodes));
        assertThrows(IllegalArgumentException.class, () ->
                new StorageProperties(1024, 1, 1_000_000, twoNodes));
        assertThrows(IllegalArgumentException.class, () ->
                new StorageProperties(1024, 2, 1_000_000, List.of(
                        new StorageProperties.Node("dup", "http://a"),
                        new StorageProperties.Node("dup", "http://b"))));
        assertTrue(true);
    }
}

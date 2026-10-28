package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.config.StorageProperties;
import com.ahmetkeles.metadataservice.placement.ChunkPlacementService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

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
    void addingNodeMovesOnlyASmallShareOfChunkReplicaSets() {
        List<StorageProperties.Node> tenNodes = IntStream
                .rangeClosed(1, 10)
                .mapToObj(index -> new StorageProperties.Node(
                        "node-%02d".formatted(index),
                        "http://n" + index))
                .toList();
        List<StorageProperties.Node> elevenNodes = new ArrayList<>(tenNodes);
        elevenNodes.add(new StorageProperties.Node("node-11", "http://n11"));
        ChunkPlacementService before = new ChunkPlacementService(
                new StorageProperties(1024, 2, 1_000_000, tenNodes));
        ChunkPlacementService after = new ChunkPlacementService(
                new StorageProperties(1024, 2, 1_000_000, elevenNodes));
        UUID objectId = UUID.fromString(
                "00112233-4455-6677-8899-aabbccddeeff");

        int changed = 0;
        int chunks = 10_000;
        for (int index = 0; index < chunks; index++) {
            List<String> oldNodes = before.replicasFor(objectId, index).stream()
                    .map(StorageProperties.Node::id)
                    .toList();
            List<String> newNodes = after.replicasFor(objectId, index).stream()
                    .map(StorageProperties.Node::id)
                    .toList();
            if (!oldNodes.equals(newNodes)) {
                changed++;
            }
        }

        assertTrue(changed < chunks * 35 / 100,
                "adding one node to a 10-node cluster should remap only a "
                        + "small share of replica sets; changed " + changed
                        + " of " + chunks);
    }

    @Test
    void ringOrderContainsEachConfiguredNodeOnce() {
        List<StorageProperties.Node> ordered =
                new ChunkPlacementService(properties)
                        .ringFor(UUID.randomUUID(), 0);

        assertEquals(3, ordered.size());
        assertEquals(3, ordered.stream()
                .map(StorageProperties.Node::id)
                .distinct()
                .count());
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

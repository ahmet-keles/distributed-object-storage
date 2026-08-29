package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.client.StorageNodeClient;
import com.ahmetkeles.metadataservice.repair.RepairReport;
import com.ahmetkeles.metadataservice.repair.RepairStore;
import com.ahmetkeles.metadataservice.repair.ReplicaRepairService;
import com.ahmetkeles.metadataservice.repository.StoredObjectRepository;
import com.ahmetkeles.metadataservice.service.ObjectPlan;
import com.ahmetkeles.metadataservice.service.ObjectStorageService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

/**
 * The repair sweep's whole contract, driven synchronously (the background
 * worker is disabled here — {@link ReplicaRepairWorkerIntegrationTest}
 * proves the scheduled path): detection of lost and missing replicas,
 * re-creation from a checksum-verified surviving copy, in-place restore,
 * refusal to propagate unverifiable bytes, capacity-aware catch-up, and
 * idempotent, concurrency-safe metadata recording.
 */
@SpringBootTest
class ReplicaRepairIntegrationTest {

    private static final int CHUNK_SIZE = 1024;
    private static final int TIMEOUT_SECONDS = 30;

    private static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    private static final FakeStorageNode node1 = new FakeStorageNode();
    private static final FakeStorageNode node2 = new FakeStorageNode();
    private static final FakeStorageNode node3 = new FakeStorageNode();

    private static final Map<String, FakeStorageNode> nodesById = Map.of(
            "node-1", node1, "node-2", node2, "node-3", node3);

    static {
        postgres.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);

        registry.add("storage.chunk-size-bytes", () -> CHUNK_SIZE);
        registry.add("storage.replication-factor", () -> 2);
        registry.add("storage.max-object-size-bytes", () -> 1_000_000);

        // Sweeps are driven by hand; a page size smaller than most objects'
        // chunk counts makes every sweep exercise the paged scan for real.
        registry.add("storage.repair.enabled", () -> false);
        registry.add("storage.repair.scan-page-size", () -> 3);

        registry.add("storage.nodes[0].id", () -> "node-1");
        registry.add("storage.nodes[0].base-url", node1::baseUrl);
        registry.add("storage.nodes[1].id", () -> "node-2");
        registry.add("storage.nodes[1].base-url", node2::baseUrl);
        registry.add("storage.nodes[2].id", () -> "node-3");
        registry.add("storage.nodes[2].base-url", node3::baseUrl);
    }

    @AfterAll
    static void stopNodes() {
        node1.stop();
        node2.stop();
        node3.stop();
    }

    @Autowired
    private ObjectStorageService objectService;

    @Autowired
    private ReplicaRepairService repairService;

    @MockitoSpyBean
    private StorageNodeClient nodeClient;

    @MockitoSpyBean
    private RepairStore repairStore;

    @Autowired
    private StoredObjectRepository repository;

    @BeforeEach
    void resetFailures() {
        nodesById.values().forEach(node -> {
            node.setDown(false);
            node.setCorrupt(false);
        });
    }

    /**
     * Sweeps are global, so each test starts from an empty metadata store —
     * a leftover degraded object from another test must not leak into this
     * test's report counts. Orphaned bytes on the fake nodes are harmless:
     * without metadata they are never scanned.
     */
    @AfterEach
    void clearMetadata() {
        repository.deleteAll();
    }

    @Test
    void sweepOnAHealthySystemChangesNothing() {
        String key = "repair-noop-" + UUID.randomUUID();
        ObjectPlan plan = objectService.upload(
                key, randomBytes(3 * CHUNK_SIZE + 100));
        int bytesHeldBefore = totalChunksHeld();

        RepairReport report = repairService.sweep();

        assertTrue(report.chunksScanned() >= plan.chunks().size(),
                "the sweep must have visited at least this object's chunks");
        assertEquals(0, report.chunksDegraded());
        assertEquals(0, report.replicasRestoredInPlace());
        assertEquals(0, report.replicasRecreated());
        assertEquals(0, report.chunksStillDegraded());
        assertEquals(bytesHeldBefore, totalChunksHeld(),
                "a healthy sweep must not write a single chunk");
        assertPlansEqual(plan, objectService.metadata(key));
    }

    @Test
    void replicasOnALostNodeAreRecreatedElsewhereFromAVerifiedCopy() {
        String key = "repair-loss-" + UUID.randomUUID();
        byte[] payload = randomBytes(4 * CHUNK_SIZE);
        ObjectPlan plan = objectService.upload(key, payload);

        String victimId = plan.chunks().get(0).nodeIds().get(0);
        List<ObjectPlan.ChunkPlan> affected = plan.chunks().stream()
                .filter(chunk -> chunk.nodeIds().contains(victimId))
                .toList();
        nodesById.get(victimId).setDown(true);

        RepairReport report = repairService.sweep();

        assertEquals(affected.size(), report.chunksDegraded());
        assertEquals(affected.size(), report.replicasRecreated(),
                "every chunk that had a replica on the lost node gets one "
                        + "new replica");
        assertEquals(0, report.replicasRestoredInPlace());
        assertEquals(0, report.chunksStillDegraded(),
                "two of three nodes are up — full redundancy is reachable");

        ObjectPlan repaired = objectService.metadata(key);
        for (ObjectPlan.ChunkPlan chunk : repaired.chunks()) {
            ObjectPlan.ChunkPlan original = plan.chunks()
                    .get(chunk.chunkIndex());

            if (!original.nodeIds().contains(victimId)) {
                assertEquals(original.nodeIds(), chunk.nodeIds(),
                        "chunks that never touched the lost node stay as "
                                + "they were");
                continue;
            }

            assertEquals(3, chunk.nodeIds().size());
            assertEquals(3, Set.copyOf(chunk.nodeIds()).size(),
                    "replicas must be on distinct nodes");
            assertTrue(chunk.nodeIds().containsAll(original.nodeIds()),
                    "repair appends replicas; it never removes recorded "
                            + "ones");

            String newNodeId = chunk.nodeIds().stream()
                    .filter(nodeId -> !original.nodeIds().contains(nodeId))
                    .findFirst().orElseThrow();
            byte[] slice = sliceOf(payload, chunk.chunkIndex());
            assertArrayEquals(slice,
                    nodesById.get(newNodeId)
                            .bytesOf(chunk.chunkId().toString()),
                    "the re-created replica must be the exact verified "
                            + "bytes");
        }

        // The object is fully redundant again among the two live nodes:
        // every chunk has two intact copies not on the dead node.
        for (ObjectPlan.ChunkPlan chunk : repaired.chunks()) {
            long liveCopies = chunk.nodeIds().stream()
                    .filter(nodeId -> !nodeId.equals(victimId))
                    .filter(nodeId -> nodesById.get(nodeId)
                            .holds(chunk.chunkId().toString()))
                    .count();
            assertEquals(2, liveCopies,
                    "chunk " + chunk.chunkIndex() + " must be back at the "
                            + "replication factor on live nodes");
        }
    }

    @Test
    void repairIsIdempotentAcrossSweeps() {
        String key = "repair-idem-" + UUID.randomUUID();
        ObjectPlan plan = objectService.upload(
                key, randomBytes(2 * CHUNK_SIZE));

        String victimId = plan.chunks().get(0).nodeIds().get(0);
        nodesById.get(victimId).setDown(true);

        RepairReport first = repairService.sweep();
        assertTrue(first.replicasRecreated() > 0, "test setup: the first "
                + "sweep must actually repair something");

        ObjectPlan afterFirst = objectService.metadata(key);
        int bytesHeldAfterFirst = totalChunksHeld();

        RepairReport second = repairService.sweep();

        assertEquals(0, second.chunksDegraded(),
                "everything is already back at the replication factor");
        assertEquals(0, second.replicasRecreated());
        assertEquals(0, second.replicasRestoredInPlace());
        assertPlansEqual(afterFirst, objectService.metadata(key));
        assertEquals(bytesHeldAfterFirst, totalChunksHeld(),
                "a repeated sweep must not write again");

        // The lost node coming back does not cause churn either: repair
        // never trims replicas, it only adds what redundancy requires.
        nodesById.get(victimId).setDown(false);
        RepairReport third = repairService.sweep();

        assertTrue(third.quiet());
        assertPlansEqual(afterFirst, objectService.metadata(key));
        assertEquals(bytesHeldAfterFirst, totalChunksHeld());
    }

    @Test
    void missingReplicaOnALiveNodeIsRestoredInPlace() {
        String key = "repair-restore-" + UUID.randomUUID();
        byte[] payload = randomBytes(2 * CHUNK_SIZE);
        ObjectPlan plan = objectService.upload(key, payload);

        ObjectPlan.ChunkPlan chunk = plan.chunks().get(0);
        FakeStorageNode holder = nodesById.get(chunk.nodeIds().get(0));
        holder.drop(chunk.chunkId().toString());
        assertFalse(holder.holds(chunk.chunkId().toString()));

        RepairReport report = repairService.sweep();

        assertEquals(1, report.chunksDegraded());
        assertEquals(1, report.replicasRestoredInPlace(),
                "a reachable node that lost the bytes gets them written "
                        + "back");
        assertEquals(0, report.replicasRecreated(),
                "no new replica location is needed");
        assertEquals(0, report.chunksStillDegraded());

        assertArrayEquals(sliceOf(payload, 0),
                holder.bytesOf(chunk.chunkId().toString()),
                "the restored copy must be the exact verified bytes");
        assertPlansEqual(plan, objectService.metadata(key),
                "restore in place must not touch metadata");
    }

    @Test
    void repairRefusesToActWithoutAChecksumVerifiedSource() {
        String key = "repair-nosource-" + UUID.randomUUID();
        byte[] payload = randomBytes(CHUNK_SIZE / 2);
        ObjectPlan plan = objectService.upload(key, payload);

        ObjectPlan.ChunkPlan chunk = plan.chunks().get(0);
        String chunkId = chunk.chunkId().toString();

        // One replica is bit-rotted at rest, the other's node is down: no
        // copy can be verified, so repair must not propagate anything.
        nodesById.get(chunk.nodeIds().get(0)).corruptStored(chunkId);
        nodesById.get(chunk.nodeIds().get(1)).setDown(true);

        int bytesHeldBefore = totalChunksHeld();
        RepairReport report = repairService.sweep();

        assertEquals(1, report.chunksDegraded());
        assertEquals(1, report.chunksWithoutVerifiedSource(),
                "the corrupt survivor must fail verification, not seed "
                        + "the repair");
        assertEquals(1, report.chunksStillDegraded());
        assertEquals(0, report.replicasRecreated());
        assertEquals(0, report.replicasRestoredInPlace());
        assertEquals(bytesHeldBefore, totalChunksHeld(),
                "no bytes may be written from an unverifiable source");
        assertPlansEqual(plan, objectService.metadata(key));

        // Once the intact replica's node returns, the object is readable
        // again — download fails over past the rotted copy.
        nodesById.get(chunk.nodeIds().get(1)).setDown(false);
        assertArrayEquals(payload, objectService.download(key).content());
    }

    @Test
    void repairWaitsForCapacityAndCatchesUpWhenANodeReturns() {
        String key = "repair-capacity-" + UUID.randomUUID();
        byte[] payload = randomBytes(CHUNK_SIZE / 2);
        ObjectPlan plan = objectService.upload(key, payload);

        ObjectPlan.ChunkPlan chunk = plan.chunks().get(0);
        String survivorId = chunk.nodeIds().get(0);
        String lostId = chunk.nodeIds().get(1);
        String spareId = nodesById.keySet().stream()
                .filter(nodeId -> !chunk.nodeIds().contains(nodeId))
                .findFirst().orElseThrow();

        // Both the second replica's node and the only spare are down: the
        // replication factor is out of reach and repair must say so rather
        // than pile copies onto the lone survivor.
        nodesById.get(lostId).setDown(true);
        nodesById.get(spareId).setDown(true);

        RepairReport constrained = repairService.sweep();

        assertEquals(1, constrained.chunksDegraded());
        assertEquals(0, constrained.replicasRecreated());
        assertEquals(1, constrained.chunksStillDegraded(),
                "with one live node the deficit is honest, not hidden");
        assertEquals(List.of(survivorId, lostId),
                objectService.metadata(key).chunks().get(0).nodeIds(),
                "no metadata may be recorded that bytes cannot back");

        // The spare returns: the next sweep restores full redundancy.
        nodesById.get(spareId).setDown(false);

        RepairReport catchUp = repairService.sweep();

        assertEquals(1, catchUp.replicasRecreated());
        assertEquals(0, catchUp.chunksStillDegraded());
        assertTrue(objectService.metadata(key).chunks().get(0).nodeIds()
                        .contains(spareId));
        assertArrayEquals(payload,
                nodesById.get(spareId).bytesOf(chunk.chunkId().toString()));
    }

    /**
     * Two sweeps repair the same degraded chunk at the same time. The gate:
     * both sweeps are held at their source read of that chunk until the
     * other arrives, so both provably assess it as degraded and both
     * proceed to write and record — the window the store's per-chunk lock
     * claims to make safe. Exactly one may append the replica row; the
     * other must observe ALREADY_RECORDED and add nothing.
     */
    @Test
    void concurrentSweepsRecordARepairedReplicaExactlyOnce()
            throws Exception {
        String key = "repair-race-" + UUID.randomUUID();
        byte[] payload = randomBytes(CHUNK_SIZE / 2);
        ObjectPlan plan = objectService.upload(key, payload);

        ObjectPlan.ChunkPlan chunk = plan.chunks().get(0);
        UUID chunkUuid = chunk.chunkId();
        String survivorId = chunk.nodeIds().get(0);
        nodesById.get(chunk.nodeIds().get(1)).setDown(true);

        CyclicBarrier bothAssessedDegraded = new CyclicBarrier(2);
        doAnswer(invocation -> {
            bothAssessedDegraded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return invocation.callRealMethod();
        }).when(nodeClient).getChunk(
                argThat(node -> node != null
                        && node.id().equals(survivorId)),
                eq(chunkUuid));

        List<RepairStore.RecordOutcome> outcomes =
                Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
            Object outcome = invocation.callRealMethod();
            if (chunkUuid.equals(invocation.getArgument(0))) {
                outcomes.add((RepairStore.RecordOutcome) outcome);
            }
            return outcome;
        }).when(repairStore).recordRepairedReplica(any(), anyString());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<RepairReport> sweepA =
                    executor.submit(repairService::sweep);
            Future<RepairReport> sweepB =
                    executor.submit(repairService::sweep);

            RepairReport reportA = sweepA.get(
                    TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RepairReport reportB = sweepB.get(
                    TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertEquals(0, reportA.chunksStillDegraded());
            assertEquals(0, reportB.chunksStillDegraded());
        } finally {
            executor.shutdownNow();
        }

        assertEquals(2, outcomes.size(),
                "both sweeps must have reached the record step");
        assertTrue(outcomes.contains(RepairStore.RecordOutcome.RECORDED));
        assertTrue(outcomes.contains(
                        RepairStore.RecordOutcome.ALREADY_RECORDED),
                "the second recorder must observe the first, not duplicate "
                        + "it — outcomes: " + outcomes);

        ObjectPlan repaired = objectService.metadata(key);
        List<String> nodeIds = repaired.chunks().get(0).nodeIds();
        assertEquals(3, nodeIds.size(),
                "exactly one replica row may be appended");
        assertEquals(3, Set.copyOf(nodeIds).size());

        String newNodeId = nodeIds.stream()
                .filter(nodeId -> !chunk.nodeIds().contains(nodeId))
                .findFirst().orElseThrow();
        assertArrayEquals(payload,
                nodesById.get(newNodeId).bytesOf(chunkUuid.toString()));
    }

    private void assertPlansEqual(ObjectPlan expected, ObjectPlan actual) {
        assertPlansEqual(expected, actual, "metadata must be unchanged");
    }

    private void assertPlansEqual(ObjectPlan expected, ObjectPlan actual,
                                  String message) {
        assertNotNull(actual);
        assertEquals(expected.chunks().size(), actual.chunks().size(),
                message);
        for (int i = 0; i < expected.chunks().size(); i++) {
            assertEquals(expected.chunks().get(i).nodeIds(),
                    actual.chunks().get(i).nodeIds(), message);
        }
    }

    private static byte[] sliceOf(byte[] payload, int chunkIndex) {
        return Arrays.copyOfRange(payload, chunkIndex * CHUNK_SIZE,
                Math.min((chunkIndex + 1) * CHUNK_SIZE, payload.length));
    }

    private static int totalChunksHeld() {
        return nodesById.values().stream()
                .mapToInt(FakeStorageNode::chunkCount).sum();
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}

package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.service.ObjectPlan;
import com.ahmetkeles.metadataservice.service.ObjectStorageService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one thing {@link ReplicaRepairIntegrationTest} cannot prove by
 * calling sweeps directly: the scheduled background worker actually runs
 * and repairs on its own. Separate class on purpose — this context runs
 * with the worker enabled at a tight interval, which would race the
 * hand-driven sweeps of every other repair test.
 */
@SpringBootTest
class ReplicaRepairWorkerIntegrationTest {

    private static final int CHUNK_SIZE = 1024;

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

        registry.add("storage.repair.enabled", () -> true);
        registry.add("storage.repair.interval", () -> "PT1S");

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

    @Test
    void backgroundWorkerRestoresALostReplicaWithoutAnyTrigger() {
        String key = "worker-" + UUID.randomUUID();
        byte[] payload = randomBytes(2 * CHUNK_SIZE);
        ObjectPlan plan = objectService.upload(key, payload);

        ObjectPlan.ChunkPlan chunk = plan.chunks().get(0);
        FakeStorageNode holder = nodesById.get(chunk.nodeIds().get(0));
        holder.drop(chunk.chunkId().toString());
        assertFalse(holder.holds(chunk.chunkId().toString()));

        // Nothing calls repair here: only the scheduled worker can bring
        // the dropped replica back.
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertTrue(
                        holder.holds(chunk.chunkId().toString()),
                        "the background worker must restore the replica"));

        assertArrayEquals(
                Arrays.copyOfRange(payload, 0, CHUNK_SIZE),
                holder.bytesOf(chunk.chunkId().toString()),
                "the restored copy must be the exact verified bytes");
        assertEquals(chunk.nodeIds(),
                objectService.metadata(key).chunks().get(0).nodeIds(),
                "an in-place restore must not change recorded locations");

        // This context stays cached (and sweeping) while other test classes
        // run; with the object gone those sweeps are quiet no-ops instead of
        // error noise once this class's fake nodes are stopped.
        objectService.delete(key);
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}

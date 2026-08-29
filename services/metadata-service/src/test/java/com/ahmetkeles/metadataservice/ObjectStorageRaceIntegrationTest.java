package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.service.MetadataStore;
import com.ahmetkeles.metadataservice.service.ObjectAlreadyExistsException;
import com.ahmetkeles.metadataservice.service.ObjectPlan;
import com.ahmetkeles.metadataservice.service.ObjectStorageService;
import com.ahmetkeles.metadataservice.repository.StoredObjectRepository;
import com.ahmetkeles.metadataservice.service.Checksums;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

/**
 * Latch-forced race proofs, deterministic rather than timing-dependent. The
 * seam is a spy on {@link MetadataStore}: each test arms a one-shot gate on
 * the metadata call it wants to interleave around, so the racing step
 * provably happens in the window the production code claims to be safe in.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObjectStorageRaceIntegrationTest {

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

        // These races are choreographed step by step; a background repair
        // sweep would be an uninvited third participant.
        registry.add("storage.repair.enabled", () -> false);

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

    @LocalServerPort
    private int port;

    @Autowired
    private ObjectStorageService service;

    @Autowired
    private StoredObjectRepository repository;

    @MockitoSpyBean
    private MetadataStore metadataStore;

    private final TestRestTemplate rest = new TestRestTemplate();

    /**
     * The download-consistency guarantee: the response body and the
     * X-Object-Sha256 header must come from the one plan the download read,
     * even when the object is deleted and re-created under the same key
     * while the request is in flight.
     *
     * <p>The download is parked immediately after it has loaded its plan
     * (spy gate on planFor). While it is parked, the object is deleted with
     * all nodes down — so the metadata vanishes but the old chunk bytes
     * survive as documented orphans — and a different object is uploaded
     * under the same key. The released download must then serve the OLD
     * bytes with the OLD checksum: a consistent pair from its own plan,
     * never old bytes with the new object's checksum (the pre-fix bug).
     */
    @Test
    void downloadServesBodyAndChecksumFromTheSamePlanSnapshot()
            throws Exception {
        String key = "swap-" + UUID.randomUUID();
        byte[] oldPayload = randomBytes(2 * CHUNK_SIZE);
        byte[] newPayload = randomBytes(2 * CHUNK_SIZE + 77);
        String oldSha = Checksums.sha256Hex(oldPayload);
        String newSha = Checksums.sha256Hex(newPayload);

        upload(key, oldPayload);

        CountDownLatch planLoaded = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(true);

        doAnswer(invocation -> {
            Object plan = invocation.callRealMethod();
            if (armed.compareAndSet(true, false)) {
                planLoaded.countDown();
                assertTrue(proceed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                        "the parked download was never released");
            }
            return plan;
        }).when(metadataStore).planFor(anyString());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<ResponseEntity<byte[]>> inFlight = executor.submit(() ->
                    rest.getForEntity(objectUrl(key), byte[].class));

            assertTrue(planLoaded.await(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "the download never loaded its plan");

            // Replace the object under the key while the download is parked.
            // Nodes are down during the delete so the old chunk bytes stay
            // behind as documented orphans and remain fetchable.
            nodesById.values().forEach(node -> node.setDown(true));
            assertEquals(HttpStatus.NO_CONTENT, rest.exchange(
                            objectUrl(key), HttpMethod.DELETE, null, Void.class)
                    .getStatusCode());
            nodesById.values().forEach(node -> node.setDown(false));
            upload(key, newPayload);

            proceed.countDown();
            ResponseEntity<byte[]> response =
                    inFlight.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertArrayEquals(oldPayload, response.getBody(),
                    "the parked download reads through its own plan");
            assertEquals(oldSha, response.getHeaders().getFirst(
                            "X-Object-Sha256"),
                    "the checksum header must describe the served bytes");
            assertNotEquals(newSha, response.getHeaders().getFirst(
                            "X-Object-Sha256"),
                    "pairing old bytes with the new object's checksum is "
                            + "exactly the bug this guards against");
        } finally {
            executor.shutdownNow();
        }

        // A fresh download observes the replacement, again as a consistent
        // pair.
        ResponseEntity<byte[]> fresh = rest.getForEntity(
                objectUrl(key), byte[].class);
        assertArrayEquals(newPayload, fresh.getBody());
        assertEquals(newSha,
                fresh.getHeaders().getFirst("X-Object-Sha256"));
    }

    /**
     * Two uploads of the SAME key race past the exists() pre-check — both
     * provably observe "absent" (spy gate holds each at a barrier until the
     * other has its answer) — then both write their chunks to the nodes and
     * race to commit. Exactly one may win; the loser must clean its bytes
     * off the nodes and surface as a conflict.
     */
    @Test
    void concurrentUploadsOfTheSameKeyCommitExactlyOnceAndCleanUpTheLoser()
            throws Exception {
        String key = "dup-race-" + UUID.randomUUID();
        byte[] payloadA = randomBytes(3 * CHUNK_SIZE);
        byte[] payloadB = randomBytes(3 * CHUNK_SIZE);
        int chunksHeldBefore = totalChunksHeld();

        CyclicBarrier bothPassedExistsCheck = new CyclicBarrier(2);
        AtomicBoolean armed = new AtomicBoolean(true);

        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (armed.get() && key.equals(invocation.getArgument(0))) {
                bothPassedExistsCheck.await(TIMEOUT_SECONDS,
                        TimeUnit.SECONDS);
                armed.set(false);
            }
            return result;
        }).when(metadataStore).exists(anyString());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Object> first = executor.submit(() -> outcome(
                    () -> service.upload(key, payloadA)));
            Future<Object> second = executor.submit(() -> outcome(
                    () -> service.upload(key, payloadB)));

            Object outcomeA = first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Object outcomeB = second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            ObjectPlan winner;
            Object loser;
            if (outcomeA instanceof ObjectPlan plan) {
                winner = plan;
                loser = outcomeB;
            } else {
                winner = assertInstanceOf(ObjectPlan.class, outcomeB,
                        "exactly one upload must commit; outcomes: "
                                + outcomeA + " / " + outcomeB);
                loser = outcomeA;
            }
            assertInstanceOf(ObjectAlreadyExistsException.class, loser,
                    "the losing upload must surface as a conflict");

            byte[] winnerPayload =
                    winner.sha256().equals(Checksums.sha256Hex(payloadA))
                            ? payloadA : payloadB;
            assertEquals(Checksums.sha256Hex(winnerPayload), winner.sha256());

            // Exactly one metadata object, and the nodes hold exactly the
            // winner's replicas: 3 chunks x 2 nodes on top of the baseline,
            // meaning every chunk the loser wrote has been cleaned up.
            assertEquals(1, repository.findAll().stream()
                    .filter(object -> object.getObjectKey().equals(key))
                    .count());
            assertEquals(chunksHeldBefore + 6, totalChunksHeld(),
                    "the loser's chunks must be deleted from the nodes");

            for (ObjectPlan.ChunkPlan chunk : winner.chunks()) {
                byte[] slice = Arrays.copyOfRange(winnerPayload,
                        chunk.chunkIndex() * CHUNK_SIZE,
                        Math.min((chunk.chunkIndex() + 1) * CHUNK_SIZE,
                                winnerPayload.length));
                assertEquals(2, chunk.nodeIds().size());
                for (String nodeId : chunk.nodeIds()) {
                    assertArrayEquals(slice, nodesById.get(nodeId)
                                    .bytesOf(chunk.chunkId().toString()),
                            "the winner's replicas must remain intact");
                }
            }

            // And the committed object downloads correctly.
            assertArrayEquals(winnerPayload,
                    service.download(key).content());
        } finally {
            executor.shutdownNow();
        }
    }

    private static Object outcome(java.util.concurrent.Callable<?> call) {
        try {
            return call.call();
        } catch (Exception exception) {
            return exception;
        }
    }

    private void upload(String key, byte[] payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        assertEquals(HttpStatus.CREATED, rest.exchange(
                        objectUrl(key), HttpMethod.PUT,
                        new HttpEntity<>(payload, headers), String.class)
                .getStatusCode(), "test setup: upload must succeed");
    }

    private String objectUrl(String key) {
        return "http://localhost:" + port + "/api/objects/" + key;
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

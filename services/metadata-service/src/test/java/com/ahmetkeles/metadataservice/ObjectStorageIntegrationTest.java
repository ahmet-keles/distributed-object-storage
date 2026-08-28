package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.api.ObjectController;
import com.ahmetkeles.metadataservice.api.ObjectMetadataResponse;
import com.ahmetkeles.metadataservice.repository.StoredObjectRepository;
import com.ahmetkeles.metadataservice.service.Checksums;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import org.testcontainers.containers.PostgreSQLContainer;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The coordinator's whole behavior against real PostgreSQL (Testcontainers)
 * and three in-process fake storage nodes whose failures are scriptable:
 * chunking, checksums, deterministic replica recording, replication on
 * write, failover on unreachable AND corrupted replicas, all-replicas-lost
 * refusal, atomic upload abort, and delete propagation.
 *
 * <p>Chunk size is shrunk to 1 KiB so small payloads exercise real
 * multi-chunk objects.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObjectStorageIntegrationTest {

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
    private StoredObjectRepository repository;

    private final TestRestTemplate rest = new TestRestTemplate();

    @BeforeEach
    void resetFailures() {
        nodesById.values().forEach(node -> {
            node.setDown(false);
            node.setCorrupt(false);
        });
    }

    @Test
    void uploadChunksReplicatesAndRecordsVerifiableMetadata() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(3 * CHUNK_SIZE + 512);

        ResponseEntity<ObjectMetadataResponse> response = upload(key, payload);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        ObjectMetadataResponse metadata = response.getBody();
        assertEquals(Checksums.sha256Hex(payload), metadata.sha256());
        assertEquals(4, metadata.chunkCount(), "3.5 KiB at 1 KiB chunks");
        assertEquals(payload.length, metadata.sizeBytes());

        for (ObjectMetadataResponse.ChunkMetadata chunk : metadata.chunks()) {
            assertEquals(2, chunk.nodes().size(),
                    "every chunk must be replicated to 2 nodes");
            assertEquals(2, Set.copyOf(chunk.nodes()).size(),
                    "replicas must be on distinct nodes");

            byte[] slice = Arrays.copyOfRange(payload,
                    chunk.chunkIndex() * CHUNK_SIZE,
                    Math.min((chunk.chunkIndex() + 1) * CHUNK_SIZE,
                            payload.length));
            assertEquals(Checksums.sha256Hex(slice), chunk.sha256());

            for (String nodeId : chunk.nodes()) {
                FakeStorageNode node = nodesById.get(nodeId);
                assertTrue(node.holds(chunk.chunkId()),
                        "node " + nodeId + " must hold chunk "
                                + chunk.chunkIndex());
                assertArrayEquals(slice, node.bytesOf(chunk.chunkId()),
                        "the bytes on the node must be the exact slice");
            }
        }
    }

    @Test
    void downloadReassemblesTheExactBytes() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(2 * CHUNK_SIZE + 100);
        upload(key, payload);

        ResponseEntity<byte[]> response = rest.getForEntity(
                objectUrl(key), byte[].class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertArrayEquals(payload, response.getBody());
        assertEquals(Checksums.sha256Hex(payload),
                response.getHeaders().getFirst(
                        ObjectController.OBJECT_SHA256_HEADER));
    }

    @Test
    void downloadSurvivesAnyOneNodeBeingDown() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(4 * CHUNK_SIZE);
        ObjectMetadataResponse metadata = upload(key, payload).getBody();

        // Every node that holds at least one replica gets its turn at being
        // the one dead node; the object must survive each single loss.
        Set<String> usedNodes = new HashSet<>();
        metadata.chunks().forEach(chunk -> usedNodes.addAll(chunk.nodes()));

        for (String nodeId : usedNodes) {
            nodesById.get(nodeId).setDown(true);

            ResponseEntity<byte[]> response = rest.getForEntity(
                    objectUrl(key), byte[].class);

            assertEquals(HttpStatus.OK, response.getStatusCode(),
                    "download must survive losing node " + nodeId);
            assertArrayEquals(payload, response.getBody());

            nodesById.get(nodeId).setDown(false);
        }
    }

    @Test
    void downloadFailsOverWhenAReplicaIsCorrupted() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(2 * CHUNK_SIZE);
        ObjectMetadataResponse metadata = upload(key, payload).getBody();

        // Corrupt every replica served by the first-priority node of chunk 0:
        // the checksum, not availability, must route around it.
        String primaryOfChunk0 = metadata.chunks().get(0).nodes().get(0);
        nodesById.get(primaryOfChunk0).setCorrupt(true);

        ResponseEntity<byte[]> response = rest.getForEntity(
                objectUrl(key), byte[].class);

        assertEquals(HttpStatus.OK, response.getStatusCode(),
                "a corrupted replica must be detected and skipped");
        assertArrayEquals(payload, response.getBody());
    }

    @Test
    void downloadFailsWhenEveryReplicaOfAChunkIsGone() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(CHUNK_SIZE + 1);
        ObjectMetadataResponse metadata = upload(key, payload).getBody();

        metadata.chunks().get(0).nodes().forEach(
                nodeId -> nodesById.get(nodeId).setDown(true));

        ResponseEntity<String> response = rest.getForEntity(
                objectUrl(key), String.class);

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        assertTrue(response.getBody().contains("storage_unavailable"));
    }

    @Test
    void failedUploadCommitsNothingAndCleansUpWrittenChunks() {
        String key = "objects-" + UUID.randomUUID();
        int chunksBefore = totalChunksHeld();

        node3.setDown(true);

        // 8 chunks x 2 replicas = 16 slots over 3 nodes: node-3 is
        // unavoidably part of some placement, so the upload must fail.
        ResponseEntity<String> response = rest.exchange(
                objectUrl(key), HttpMethod.PUT,
                octetStreamEntity(randomBytes(8 * CHUNK_SIZE)), String.class);

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        assertFalse(repository.existsByObjectKey(key),
                "a failed upload must record no metadata");
        assertEquals(chunksBefore, totalChunksHeld(),
                "chunks written before the failure must be cleaned up");
    }

    @Test
    void duplicateKeyIsRejectedWithConflict() {
        String key = "objects-" + UUID.randomUUID();
        upload(key, randomBytes(10));

        ResponseEntity<String> second = rest.exchange(
                objectUrl(key), HttpMethod.PUT,
                octetStreamEntity(randomBytes(10)), String.class);

        assertEquals(HttpStatus.CONFLICT, second.getStatusCode());
    }

    @Test
    void deleteRemovesMetadataAndChunkBytes() {
        String key = "objects-" + UUID.randomUUID();
        ObjectMetadataResponse metadata =
                upload(key, randomBytes(2 * CHUNK_SIZE)).getBody();

        ResponseEntity<Void> delete = rest.exchange(
                objectUrl(key), HttpMethod.DELETE, null, Void.class);

        assertEquals(HttpStatus.NO_CONTENT, delete.getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND,
                rest.getForEntity(objectUrl(key), byte[].class)
                        .getStatusCode());
        assertFalse(repository.existsByObjectKey(key));

        for (ObjectMetadataResponse.ChunkMetadata chunk : metadata.chunks()) {
            for (FakeStorageNode node : nodesById.values()) {
                assertFalse(node.holds(chunk.chunkId()),
                        "chunk bytes must be deleted from the nodes");
            }
        }
    }

    @Test
    void missingObjectAndInvalidRequestsAreRejected() {
        assertEquals(HttpStatus.NOT_FOUND,
                rest.getForEntity(objectUrl("no-such-object"), String.class)
                        .getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND, rest.exchange(
                        objectUrl("no-such-object"), HttpMethod.DELETE, null,
                        String.class).getStatusCode());

        assertEquals(HttpStatus.BAD_REQUEST, rest.exchange(
                        objectUrl("bad%20key"), HttpMethod.PUT,
                        octetStreamEntity(randomBytes(4)), String.class)
                .getStatusCode(), "keys outside the allowed charset are 400");

        assertEquals(HttpStatus.BAD_REQUEST, rest.exchange(
                        objectUrl("empty-" + UUID.randomUUID()),
                        HttpMethod.PUT, octetStreamEntity(new byte[0]),
                        String.class).getStatusCode(),
                "empty bodies are 400");

        assertEquals(HttpStatus.CONTENT_TOO_LARGE, rest.exchange(
                        objectUrl("big-" + UUID.randomUUID()), HttpMethod.PUT,
                        octetStreamEntity(randomBytes(1_000_001)),
                        String.class).getStatusCode());
    }

    @Test
    void checksumHeaderMatchesIndependentlyComputedDigest() {
        String key = "objects-" + UUID.randomUUID();
        byte[] payload = randomBytes(CHUNK_SIZE * 2 + 7);

        String uploadSha = upload(key, payload).getHeaders()
                .getFirst(ObjectController.OBJECT_SHA256_HEADER);

        assertEquals(Checksums.sha256Hex(payload), uploadSha);
        assertNotEquals(Checksums.sha256Hex(randomBytes(8)), uploadSha);
    }

    private ResponseEntity<ObjectMetadataResponse> upload(
            String key, byte[] payload) {
        ResponseEntity<ObjectMetadataResponse> response = rest.exchange(
                objectUrl(key), HttpMethod.PUT, octetStreamEntity(payload),
                ObjectMetadataResponse.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode(),
                "test setup: upload of " + key + " must succeed");
        return response;
    }

    private static HttpEntity<byte[]> octetStreamEntity(byte[] payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        return new HttpEntity<>(payload, headers);
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

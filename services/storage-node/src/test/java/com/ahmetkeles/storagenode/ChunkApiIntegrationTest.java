package com.ahmetkeles.storagenode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The node's full wire contract over real HTTP against a real (temporary)
 * data directory: byte round-trips, checksum enforcement on write, checksum
 * reporting on read, idempotent deletes, and rejection of non-UUID ids —
 * which is also the path-traversal guard.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ChunkApiIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("storage.node.data-dir",
                () -> tempDir.resolve("chunks").toString());
    }

    @LocalServerPort
    private int port;

    private final TestRestTemplate rest = new TestRestTemplate();

    @Test
    void storesAndReturnsChunkBytesWithFreshChecksum() {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(4096);

        ResponseEntity<Void> put = put(chunkId, bytes,
                ChunkStore.sha256Hex(bytes));
        assertEquals(HttpStatus.CREATED, put.getStatusCode());

        ResponseEntity<byte[]> get = rest.getForEntity(
                url(chunkId), byte[].class);
        assertEquals(HttpStatus.OK, get.getStatusCode());
        assertArrayEquals(bytes, get.getBody());
        assertEquals(ChunkStore.sha256Hex(bytes),
                get.getHeaders().getFirst(ChunkController.SHA256_HEADER),
                "the node must report the checksum of what it actually read");
    }

    @Test
    void overwritingAChunkIsIdempotent() {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(128);
        String sha = ChunkStore.sha256Hex(bytes);

        assertEquals(HttpStatus.CREATED, put(chunkId, bytes, sha).getStatusCode());
        assertEquals(HttpStatus.CREATED, put(chunkId, bytes, sha).getStatusCode());

        assertArrayEquals(bytes,
                rest.getForEntity(url(chunkId), byte[].class).getBody());
    }

    @Test
    void rejectsWriteWhoseBytesDoNotMatchDeclaredChecksum() {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(128);

        ResponseEntity<Void> response = put(chunkId, bytes,
                ChunkStore.sha256Hex(randomBytes(128)));

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND,
                rest.getForEntity(url(chunkId), byte[].class).getStatusCode(),
                "a rejected write must leave nothing behind");
    }

    @Test
    void rejectsWriteWithoutDeclaredChecksum() {
        ResponseEntity<Void> response = rest.exchange(
                url(UUID.randomUUID()), HttpMethod.PUT,
                new HttpEntity<>(randomBytes(16)), Void.class);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void missingChunkIs404() {
        assertEquals(HttpStatus.NOT_FOUND,
                rest.getForEntity(url(UUID.randomUUID()), byte[].class)
                        .getStatusCode());
    }

    @Test
    void headReportsPresenceWithoutABody() {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(256);

        assertEquals(HttpStatus.NOT_FOUND, rest.exchange(
                        url(chunkId), HttpMethod.HEAD, null, Void.class)
                .getStatusCode(), "a chunk not stored here probes 404");

        put(chunkId, bytes, ChunkStore.sha256Hex(bytes));

        ResponseEntity<byte[]> head = rest.exchange(
                url(chunkId), HttpMethod.HEAD, null, byte[].class);
        assertEquals(HttpStatus.OK, head.getStatusCode());
        assertTrue(head.getBody() == null || head.getBody().length == 0,
                "a probe must never carry the chunk bytes");
    }

    @Test
    void deleteRemovesTheChunkAndIsIdempotent() {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(64);
        put(chunkId, bytes, ChunkStore.sha256Hex(bytes));

        assertEquals(HttpStatus.NO_CONTENT, rest.exchange(
                url(chunkId), HttpMethod.DELETE, null, Void.class)
                .getStatusCode());
        assertEquals(HttpStatus.NOT_FOUND,
                rest.getForEntity(url(chunkId), byte[].class).getStatusCode());
        assertEquals(HttpStatus.NO_CONTENT, rest.exchange(
                url(chunkId), HttpMethod.DELETE, null, Void.class)
                .getStatusCode(), "deleting a missing chunk is a no-op");
    }

    @Test
    void rejectsNonUuidChunkIdsSoPathsCannotBeForged() throws Exception {
        byte[] bytes = randomBytes(16);
        HttpHeaders headers = new HttpHeaders();
        headers.set(ChunkController.SHA256_HEADER, ChunkStore.sha256Hex(bytes));

        ResponseEntity<Void> response = rest.exchange(
                "http://localhost:" + port + "/chunks/%2e%2e%2fescape",
                HttpMethod.PUT, new HttpEntity<>(bytes, headers), Void.class);

        assertTrue(response.getStatusCode().is4xxClientError(),
                "non-UUID ids must be rejected, got "
                        + response.getStatusCode());
        assertTrue(Files.notExists(tempDir.resolve("escape")),
                "nothing may be written outside the data directory");
    }

    private ResponseEntity<Void> put(UUID chunkId, byte[] bytes, String sha) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(ChunkController.SHA256_HEADER, sha);
        return rest.exchange(url(chunkId), HttpMethod.PUT,
                new HttpEntity<>(bytes, headers), Void.class);
    }

    private String url(UUID chunkId) {
        return "http://localhost:" + port + "/chunks/" + chunkId;
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    @Test
    void failedWriteLeavesNoTemporaryFileBehind() throws Exception {
        UUID chunkId = UUID.randomUUID();
        byte[] bytes = randomBytes(64);

        // Force the atomic move to fail deterministically: the chunk's final
        // path is pre-created as a non-empty DIRECTORY, which REPLACE_EXISTING
        // cannot replace.
        Path fanOut = tempDir.resolve("chunks")
                .resolve(chunkId.toString().substring(0, 2));
        Path target = fanOut.resolve(chunkId.toString());
        Files.createDirectories(target);
        Files.writeString(target.resolve("occupied"), "x");

        ResponseEntity<Void> response = put(chunkId, bytes,
                ChunkStore.sha256Hex(bytes));

        assertTrue(response.getStatusCode().is5xxServerError(),
                "the write must fail, got " + response.getStatusCode());

        try (var files = Files.list(fanOut)) {
            assertTrue(files.noneMatch(path ->
                            path.getFileName().toString().endsWith(".tmp")),
                    "a failed write must not leak its temporary file");
        }
    }
}

package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The full object lifecycle across real processes: chunked upload with
 * two-node replication, checksum-verified download, direct on-node byte
 * inspection, and delete propagation to metadata and nodes.
 */
@Timeout(300)
class ObjectLifecycleE2eTest {

    private static E2eStack stack;
    private static StoreClient client;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        client = new StoreClient();
    }

    @Test
    void uploadReplicatesDownloadVerifiesDeletePropagates() {
        String key = "lifecycle-" + UUID.randomUUID();
        byte[] payload = randomBytes(3 * E2eStack.CHUNK_SIZE + 1000);
        String payloadSha = StoreClient.sha256Hex(payload);

        // Upload: 201 with the object checksum and the full chunk map.
        HttpResponse<byte[]> upload = client.put(objectUrl(key), payload);
        assertEquals(201, upload.statusCode());
        assertEquals(payloadSha,
                upload.headers().firstValue("X-Object-Sha256").orElseThrow());

        JsonNode metadata = StoreClient.json(upload);
        assertEquals(4, metadata.get("chunkCount").asInt());
        assertEquals(payload.length, metadata.get("sizeBytes").asLong());
        assertEquals(payloadSha, metadata.get("sha256").asText());

        // Replication: every chunk is on two distinct nodes, and the bytes a
        // node actually serves are the exact slice with the recorded hash.
        for (JsonNode chunk : metadata.get("chunks")) {
            assertEquals(2, chunk.get("nodes").size(),
                    "chunk " + chunk.get("chunkIndex") + " must have 2 replicas");

            int index = chunk.get("chunkIndex").asInt();
            byte[] slice = Arrays.copyOfRange(payload,
                    index * E2eStack.CHUNK_SIZE,
                    Math.min((index + 1) * E2eStack.CHUNK_SIZE,
                            payload.length));
            assertEquals(StoreClient.sha256Hex(slice),
                    chunk.get("sha256").asText());

            for (JsonNode nodeId : chunk.get("nodes")) {
                HttpResponse<byte[]> onNode = client.get(
                        stack.nodeBaseUrl(nodeId.asText())
                                + "/chunks/" + chunk.get("chunkId").asText());
                assertEquals(200, onNode.statusCode(),
                        "replica must exist on node " + nodeId.asText());
                assertArrayEquals(slice, onNode.body(),
                        "node bytes must be the exact chunk slice");
            }
        }

        // Download: byte-identical, checksum in the header.
        HttpResponse<byte[]> download = client.get(objectUrl(key));
        assertEquals(200, download.statusCode());
        assertArrayEquals(payload, download.body());
        assertEquals(payloadSha, download.headers()
                .firstValue("X-Object-Sha256").orElseThrow());

        // Delete: metadata gone, chunk bytes gone from every replica.
        assertEquals(204, client.delete(objectUrl(key)).statusCode());
        assertEquals(404, client.get(objectUrl(key)).statusCode());

        for (JsonNode chunk : metadata.get("chunks")) {
            for (JsonNode nodeId : chunk.get("nodes")) {
                assertEquals(404, client.get(
                                stack.nodeBaseUrl(nodeId.asText()) + "/chunks/"
                                        + chunk.get("chunkId").asText())
                        .statusCode(), "chunk bytes must be deleted");
            }
        }
    }

    @Test
    void duplicateKeyIsRejectedAndOriginalSurvives() {
        String key = "dup-" + UUID.randomUUID();
        byte[] payload = randomBytes(E2eStack.CHUNK_SIZE / 2);

        assertEquals(201, client.put(objectUrl(key), payload).statusCode());
        assertEquals(409, client.put(objectUrl(key),
                randomBytes(64)).statusCode());

        HttpResponse<byte[]> download = client.get(objectUrl(key));
        assertEquals(200, download.statusCode());
        assertArrayEquals(payload, download.body(),
                "the original object must be untouched by the rejected write");
        assertTrue(client.delete(objectUrl(key)).statusCode() == 204);
    }

    private String objectUrl(String key) {
        return stack.apiBaseUrl() + "/api/objects/" + key;
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}

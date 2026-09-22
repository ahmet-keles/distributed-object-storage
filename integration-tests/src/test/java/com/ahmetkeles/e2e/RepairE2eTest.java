package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The recovery claim of milestone 2, proven the only way that counts:
 * losing a node (container genuinely stopped) degrades the object to one
 * live copy per affected chunk; the background repair worker then rebuilds
 * redundancy onto the surviving nodes — after which a SECOND node loss,
 * fatal under plain replication factor 2, leaves every chunk still
 * readable. Both nodes are restarted afterwards so the shared stack stays
 * intact for other test classes.
 */
@Timeout(600)
class RepairE2eTest {

    private static E2eStack stack;
    private static StoreClient client;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        client = new StoreClient();
    }

    @Test
    void repairedRedundancySurvivesASecondNodeLoss() {
        String key = "repair-" + UUID.randomUUID();
        byte[] payload = randomBytes(4 * E2eStack.CHUNK_SIZE);

        HttpResponse<byte[]> upload = client.put(objectUrl(key), payload);
        assertEquals(201, upload.statusCode());

        // The two nodes recorded for chunk 0 will be lost, in this order.
        JsonNode uploaded = StoreClient.json(upload);
        String firstLoss = uploaded.get("chunks").get(0)
                .get("nodes").get(0).asText();
        String secondLoss = uploaded.get("chunks").get(0)
                .get("nodes").get(1).asText();

        stack.stopNode(firstLoss);
        try {
            // Repair is done when every chunk records at least two replicas
            // on nodes other than the dead one — replica rows are only
            // written after the copy was stored and verified by readback.
            await().atMost(Duration.ofMinutes(4))
                    .pollInterval(Duration.ofSeconds(2))
                    .untilAsserted(() -> {
                        JsonNode metadata = fetchMetadata(key);
                        for (JsonNode chunk : metadata.get("chunks")) {
                            long liveReplicas = 0;
                            for (JsonNode nodeId : chunk.get("nodes")) {
                                if (!nodeId.asText().equals(firstLoss)) {
                                    liveReplicas++;
                                }
                            }
                            assertTrue(liveReplicas >= 2,
                                    "chunk " + chunk.get("chunkIndex")
                                            + " must be re-replicated, has "
                                            + liveReplicas
                                            + " live replicas");
                        }
                    });

            // The record is backed by bytes: every recorded replica on a
            // live node serves the exact chunk slice.
            JsonNode repaired = fetchMetadata(key);
            for (JsonNode chunk : repaired.get("chunks")) {
                int index = chunk.get("chunkIndex").asInt();
                byte[] slice = Arrays.copyOfRange(payload,
                        index * E2eStack.CHUNK_SIZE,
                        Math.min((index + 1) * E2eStack.CHUNK_SIZE,
                                payload.length));

                for (JsonNode nodeId : chunk.get("nodes")) {
                    if (nodeId.asText().equals(firstLoss)) {
                        continue;
                    }

                    HttpResponse<byte[]> onNode = client.get(
                            stack.nodeBaseUrl(nodeId.asText()) + "/chunks/"
                                    + chunk.get("chunkId").asText());
                    assertEquals(200, onNode.statusCode(),
                            "repaired replica must exist on node "
                                    + nodeId.asText());
                    assertArrayEquals(slice, onNode.body(),
                            "repaired replica must be byte-identical");
                }
            }

            // Second loss: with the first node still dead, stop the other
            // original holder of chunk 0. Without repair this object would
            // now be unreadable; with repair every chunk still has a live,
            // verified copy.
            stack.stopNode(secondLoss);
            try {
                HttpResponse<byte[]> download = client.get(objectUrl(key));
                assertEquals(200, download.statusCode(),
                        "the object must survive a second node loss after "
                                + "repair");
                assertArrayEquals(payload, download.body());
            } finally {
                stack.startNode(secondLoss);
            }
        } finally {
            stack.startNode(firstLoss);
        }

        // Leave the shared stack healthy: both restarted nodes serving.
        for (String nodeId : new String[]{firstLoss, secondLoss}) {
            await().atMost(Duration.ofMinutes(2)).ignoreExceptions()
                    .untilAsserted(() -> assertEquals(200, client.get(
                                    stack.nodeBaseUrl(nodeId)
                                            + "/actuator/health")
                            .statusCode()));
        }

        assertEquals(204, client.delete(objectUrl(key)).statusCode());
    }

    private JsonNode fetchMetadata(String key) {
        HttpResponse<byte[]> response = client.get(
                objectUrl(key) + "/metadata");
        assertEquals(200, response.statusCode());
        return StoreClient.json(response);
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

package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The availability claim of milestone 1: with every chunk on two nodes, any
 * single storage node can be down and every object stays fully readable.
 * The node is genuinely stopped (container process killed, port dead), not
 * simulated — and restarted afterwards so the shared stack stays intact for
 * other test classes.
 */
@Timeout(300)
class NodeLossE2eTest {

    private static E2eStack stack;
    private static StoreClient client;

    @BeforeAll
    static void startStack() {
        stack = E2eStack.get();
        client = new StoreClient();
    }

    @Test
    void everyObjectSurvivesLosingOneStorageNode() {
        String key = "node-loss-" + UUID.randomUUID();
        byte[] payload = randomBytes(4 * E2eStack.CHUNK_SIZE);

        HttpResponse<byte[]> upload = client.put(
                stack.apiBaseUrl() + "/api/objects/" + key, payload);
        assertEquals(201, upload.statusCode());

        // Pick a node that provably holds replicas of this object — with
        // 4 chunks x 2 replicas over 3 nodes, at least one node holds >= 3.
        JsonNode metadata = StoreClient.json(upload);
        String victim = null;
        for (JsonNode chunk : metadata.get("chunks")) {
            if (victim == null) {
                victim = chunk.get("nodes").get(0).asText();
            }
        }
        int replicasOnVictim = 0;
        for (JsonNode chunk : metadata.get("chunks")) {
            for (JsonNode nodeId : chunk.get("nodes")) {
                if (nodeId.asText().equals(victim)) {
                    replicasOnVictim++;
                }
            }
        }
        assertTrue(replicasOnVictim >= 1,
                "test setup: the victim node must hold replicas");

        stack.stopNode(victim);
        try {
            HttpResponse<byte[]> download = client.get(
                    stack.apiBaseUrl() + "/api/objects/" + key);

            assertEquals(200, download.statusCode(),
                    "download must survive losing node " + victim + " ("
                            + replicasOnVictim + " replicas)");
            assertArrayEquals(payload, download.body());
        } finally {
            stack.startNode(victim);
        }

        // The restarted node must be serving again before other test
        // classes use the shared stack.
        String healthUrl = stack.nodeBaseUrl(victim) + "/actuator/health";
        await().atMost(Duration.ofMinutes(2)).ignoreExceptions()
                .untilAsserted(() -> assertEquals(200,
                        client.get(healthUrl).statusCode()));

        assertEquals(204, client.delete(
                stack.apiBaseUrl() + "/api/objects/" + key).statusCode());
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}

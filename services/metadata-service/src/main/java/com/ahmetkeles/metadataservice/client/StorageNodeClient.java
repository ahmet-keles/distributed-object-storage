package com.ahmetkeles.metadataservice.client;

import com.ahmetkeles.metadataservice.config.StorageProperties;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.stereotype.Component;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.UUID;

import org.springframework.http.client.JdkClientHttpRequestFactory;

/**
 * HTTP client for the storage-node chunk API. Bounded timeouts are the
 * point: a dead node must fail fast so reads can move to the next replica
 * instead of hanging the download.
 */
@Component
public class StorageNodeClient {

    public static final String SHA256_HEADER = "X-Chunk-Sha256";

    private final RestClient restClient;

    public StorageNodeClient() {
        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(2))
                        .build());
        requestFactory.setReadTimeout(Duration.ofSeconds(10));

        this.restClient = RestClient.builder()
                .requestFactory(requestFactory)
                .build();
    }

    public void putChunk(StorageProperties.Node node, UUID chunkId,
                         byte[] bytes, String sha256) {
        try {
            restClient.put()
                    .uri(node.baseUrl() + "/chunks/" + chunkId)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header(SHA256_HEADER, sha256)
                    .body(bytes)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException exception) {
            throw new StorageNodeUnavailableException(
                    "Write of chunk " + chunkId + " to node " + node.id()
                            + " failed", exception);
        }
    }

    public byte[] getChunk(StorageProperties.Node node, UUID chunkId) {
        try {
            byte[] body = restClient.get()
                    .uri(node.baseUrl() + "/chunks/" + chunkId)
                    .retrieve()
                    .body(byte[].class);

            if (body == null) {
                throw new StorageNodeUnavailableException(
                        "Node " + node.id() + " returned an empty body for "
                                + "chunk " + chunkId, null);
            }

            return body;
        } catch (RestClientException exception) {
            throw new StorageNodeUnavailableException(
                    "Read of chunk " + chunkId + " from node " + node.id()
                            + " failed", exception);
        }
    }

    public void deleteChunk(StorageProperties.Node node, UUID chunkId) {
        try {
            restClient.delete()
                    .uri(node.baseUrl() + "/chunks/" + chunkId)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException exception) {
            throw new StorageNodeUnavailableException(
                    "Delete of chunk " + chunkId + " on node " + node.id()
                            + " failed", exception);
        }
    }
}

package com.ahmetkeles.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/** Thin HTTP client for the object API and for direct node inspection. */
final class StoreClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    HttpResponse<byte[]> put(String url, byte[] body) {
        return send(HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build());
    }

    HttpResponse<byte[]> get(String url) {
        return send(HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build());
    }

    HttpResponse<byte[]> delete(String url) {
        return send(HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .DELETE()
                .build());
    }

    static JsonNode json(HttpResponse<byte[]> response) {
        try {
            return MAPPER.readTree(response.body());
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Response body is not JSON", exception);
        }
    }

    static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private HttpResponse<byte[]> send(HttpRequest request) {
        try {
            return client.send(request,
                    HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Request to " + request.uri() + " failed", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        }
    }
}

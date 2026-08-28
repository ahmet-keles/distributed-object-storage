package com.ahmetkeles.metadataservice;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-memory stand-in for a storage node, implementing the same wire
 * contract, plus two failure levers the real node doesn't have: {@code down}
 * (every request answers 503 — an unreachable node) and {@code corrupt}
 * (GET serves flipped bytes — a bit-rotted replica the reader must detect
 * by checksum).
 */
final class FakeStorageNode {

    private final HttpServer server;
    private final Map<String, byte[]> chunks = new ConcurrentHashMap<>();
    private final AtomicBoolean down = new AtomicBoolean(false);
    private final AtomicBoolean corrupt = new AtomicBoolean(false);

    FakeStorageNode() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }

        server.createContext("/chunks/", this::handle);
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1" + ":" + server.getAddress().getPort();
    }

    void setDown(boolean value) {
        down.set(value);
    }

    void setCorrupt(boolean value) {
        corrupt.set(value);
    }

    boolean holds(String chunkId) {
        return chunks.containsKey(chunkId);
    }

    byte[] bytesOf(String chunkId) {
        return chunks.get(chunkId);
    }

    int chunkCount() {
        return chunks.size();
    }

    void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (down.get()) {
                exchange.sendResponseHeaders(503, -1);
                return;
            }

            String chunkId = exchange.getRequestURI().getPath()
                    .substring("/chunks/".length());

            switch (exchange.getRequestMethod()) {
                case "PUT" -> {
                    chunks.put(chunkId,
                            exchange.getRequestBody().readAllBytes());
                    exchange.sendResponseHeaders(201, -1);
                }
                case "GET" -> {
                    byte[] bytes = chunks.get(chunkId);
                    if (bytes == null) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    if (corrupt.get()) {
                        bytes = bytes.clone();
                        bytes[0] ^= 0x7F;
                    }
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
                case "DELETE" -> {
                    chunks.remove(chunkId);
                    exchange.sendResponseHeaders(204, -1);
                }
                default -> exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}

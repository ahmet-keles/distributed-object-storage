package com.ahmetkeles.metadataservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Static cluster topology and storage policy. Node membership is
 * configuration, not discovery: every node this service may read from or
 * write to is listed here, and the deterministic placement function is
 * computed over this list sorted by node id. Changing the list therefore
 * changes placement for new objects only — existing objects are read from
 * the replica locations recorded in the database, never re-derived.
 */
@ConfigurationProperties(prefix = "storage")
public record StorageProperties(
        int chunkSizeBytes,
        int replicationFactor,
        long maxObjectSizeBytes,
        List<Node> nodes
) {

    public record Node(String id, String baseUrl) {
    }

    public StorageProperties {
        if (chunkSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "storage.chunk-size-bytes must be positive");
        }

        if (maxObjectSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "storage.max-object-size-bytes must be positive");
        }

        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException(
                    "storage.nodes must list at least one storage node");
        }

        if (replicationFactor < 2) {
            throw new IllegalArgumentException(
                    "storage.replication-factor must be at least 2: a single "
                            + "copy cannot survive a node loss");
        }

        if (replicationFactor > nodes.size()) {
            throw new IllegalArgumentException(
                    "storage.replication-factor (" + replicationFactor
                            + ") cannot exceed the number of nodes ("
                            + nodes.size() + ")");
        }

        long distinctIds = nodes.stream().map(Node::id).distinct().count();
        if (distinctIds != nodes.size()) {
            throw new IllegalArgumentException(
                    "storage.nodes ids must be unique");
        }
    }
}

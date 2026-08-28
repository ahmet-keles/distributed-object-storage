package com.ahmetkeles.metadataservice.service;

import java.util.List;
import java.util.UUID;

/**
 * A read-only snapshot of an object's metadata, detached from JPA. Loading
 * it is the only database work a download or delete does; the node I/O that
 * follows runs outside any transaction, so slow or dead nodes never hold a
 * database connection hostage.
 */
public record ObjectPlan(
        UUID objectId,
        String objectKey,
        long sizeBytes,
        int chunkSizeBytes,
        String sha256,
        List<ChunkPlan> chunks
) {

    public record ChunkPlan(
            UUID chunkId,
            int chunkIndex,
            int sizeBytes,
            String sha256,
            List<String> nodeIds
    ) {
    }
}

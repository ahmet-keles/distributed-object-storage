package com.ahmetkeles.metadataservice.api;

import com.ahmetkeles.metadataservice.service.ObjectPlan;

import java.util.List;

public record ObjectMetadataResponse(
        String objectKey,
        long sizeBytes,
        int chunkSizeBytes,
        int chunkCount,
        String sha256,
        List<ChunkMetadata> chunks
) {

    public record ChunkMetadata(
            String chunkId,
            int chunkIndex,
            int sizeBytes,
            String sha256,
            List<String> nodes
    ) {
    }

    public static ObjectMetadataResponse from(ObjectPlan plan) {
        return new ObjectMetadataResponse(
                plan.objectKey(),
                plan.sizeBytes(),
                plan.chunkSizeBytes(),
                plan.chunks().size(),
                plan.sha256(),
                plan.chunks().stream()
                        .map(chunk -> new ChunkMetadata(
                                chunk.chunkId().toString(),
                                chunk.chunkIndex(),
                                chunk.sizeBytes(),
                                chunk.sha256(),
                                chunk.nodeIds()))
                        .toList()
        );
    }
}

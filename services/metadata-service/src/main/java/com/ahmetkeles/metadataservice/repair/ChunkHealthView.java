package com.ahmetkeles.metadataservice.repair;

import java.util.List;
import java.util.UUID;

/**
 * Detached snapshot of one chunk's metadata, taken by the sweep's paged
 * scan: everything repair needs to assess and rebuild the chunk without
 * holding a database transaction across node I/O.
 */
public record ChunkHealthView(
        UUID chunkId,
        UUID objectId,
        int chunkIndex,
        String sha256,
        List<String> replicaNodeIds
) {
}

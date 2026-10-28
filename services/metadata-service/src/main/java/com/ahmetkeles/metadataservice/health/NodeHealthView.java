package com.ahmetkeles.metadataservice.health;

import java.time.Instant;

/** Public status snapshot for one storage node. */
public record NodeHealthView(
        String nodeId,
        NodeHealthStatus status,
        Instant lastCheckedAt
) {
}

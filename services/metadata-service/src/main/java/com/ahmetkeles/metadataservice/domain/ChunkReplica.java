package com.ahmetkeles.metadataservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.util.UUID;

/**
 * One recorded copy of a chunk on one storage node. The database — not the
 * placement function — is the source of truth for reads: an object is always
 * read from where its chunks were actually written. The recorded {@code
 * nodeId} must still resolve against the configured node list at read time,
 * so node ids are load-bearing: removing or renaming an id that owns
 * replicas makes them unreachable until the id returns.
 */
@Entity
@Table(name = "chunk_replicas")
public class ChunkReplica {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "chunk_id", nullable = false)
    private Chunk chunk;

    @Column(name = "node_id", nullable = false, length = 64)
    private String nodeId;

    @Column(nullable = false)
    private int priority;

    protected ChunkReplica() {
    }

    ChunkReplica(UUID id, Chunk chunk, String nodeId, int priority) {
        this.id = id;
        this.chunk = chunk;
        this.nodeId = nodeId;
        this.priority = priority;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPriority() {
        return priority;
    }
}

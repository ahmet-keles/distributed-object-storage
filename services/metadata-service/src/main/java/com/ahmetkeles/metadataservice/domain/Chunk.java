package com.ahmetkeles.metadataservice.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "chunks")
public class Chunk {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "object_id", nullable = false)
    private StoredObject object;

    @Column(name = "chunk_index", nullable = false)
    private int chunkIndex;

    @Column(name = "size_bytes", nullable = false)
    private int sizeBytes;

    @Column(nullable = false, length = 64)
    private String sha256;

    @OneToMany(mappedBy = "chunk", cascade = CascadeType.ALL,
            orphanRemoval = true)
    @OrderBy("priority")
    private List<ChunkReplica> replicas = new ArrayList<>();

    protected Chunk() {
    }

    Chunk(UUID id, StoredObject object, int chunkIndex, int sizeBytes,
          String sha256) {
        this.id = id;
        this.object = object;
        this.chunkIndex = chunkIndex;
        this.sizeBytes = sizeBytes;
        this.sha256 = sha256;
    }

    public void addReplica(String nodeId, int priority) {
        replicas.add(new ChunkReplica(UUID.randomUUID(), this, nodeId,
                priority));
    }

    public UUID getId() {
        return id;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getSizeBytes() {
        return sizeBytes;
    }

    public String getSha256() {
        return sha256;
    }

    /** Replicas in read-preference order (placement order at write time). */
    public List<ChunkReplica> getReplicas() {
        return List.copyOf(replicas);
    }
}

package com.ahmetkeles.metadataservice.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OrderBy;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "objects")
public class StoredObject {

    @Id
    private UUID id;

    @Column(name = "object_key", nullable = false, length = 200)
    private String objectKey;

    @Column(name = "size_bytes", nullable = false)
    private long sizeBytes;

    @Column(name = "chunk_size_bytes", nullable = false)
    private int chunkSizeBytes;

    @Column(name = "chunk_count", nullable = false)
    private int chunkCount;

    @Column(nullable = false, length = 64)
    private String sha256;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @OneToMany(mappedBy = "object", cascade = CascadeType.ALL,
            orphanRemoval = true)
    @OrderBy("chunkIndex")
    private List<Chunk> chunks = new ArrayList<>();

    protected StoredObject() {
    }

    public StoredObject(
            UUID id,
            String objectKey,
            long sizeBytes,
            int chunkSizeBytes,
            String sha256
    ) {
        this.id = id;
        this.objectKey = objectKey;
        this.sizeBytes = sizeBytes;
        this.chunkSizeBytes = chunkSizeBytes;
        this.chunkCount = 0;
        this.sha256 = sha256;
        this.createdAt = Instant.now();
    }

    public Chunk addChunk(UUID chunkId, int chunkIndex, int sizeBytes,
                          String sha256) {
        Chunk chunk = new Chunk(chunkId, this, chunkIndex, sizeBytes, sha256);
        chunks.add(chunk);
        chunkCount = chunks.size();
        return chunk;
    }

    public UUID getId() {
        return id;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public String getSha256() {
        return sha256;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public List<Chunk> getChunks() {
        return List.copyOf(chunks);
    }
}

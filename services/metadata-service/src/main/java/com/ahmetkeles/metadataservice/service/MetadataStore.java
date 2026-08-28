package com.ahmetkeles.metadataservice.service;

import com.ahmetkeles.metadataservice.domain.Chunk;
import com.ahmetkeles.metadataservice.domain.ChunkReplica;
import com.ahmetkeles.metadataservice.domain.StoredObject;
import com.ahmetkeles.metadataservice.repository.StoredObjectRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * The transactional boundary around object metadata. Node I/O never happens
 * inside these methods: uploads write their chunks to the nodes first and
 * commit metadata last (so committed metadata always describes fully
 * replicated bytes), and downloads/deletes take a detached {@link ObjectPlan}
 * out of the transaction before touching any node.
 */
@Service
public class MetadataStore {

    private final StoredObjectRepository repository;

    public MetadataStore(StoredObjectRepository repository) {
        this.repository = repository;
    }

    public boolean exists(String objectKey) {
        return repository.existsByObjectKey(objectKey);
    }

    @Transactional
    public void save(StoredObject object) {
        repository.save(object);
    }

    @Transactional(readOnly = true)
    public Optional<ObjectPlan> planFor(String objectKey) {
        return repository.findByObjectKey(objectKey).map(MetadataStore::plan);
    }

    /**
     * Deletes the object's metadata, returning the plan of what was deleted
     * so the caller can clean the nodes up afterwards. Metadata goes first:
     * once this commits the object is gone atomically; chunk bytes whose
     * node-side delete then fails are orphaned, never resurrected.
     */
    @Transactional
    public Optional<ObjectPlan> deleteByKey(String objectKey) {
        Optional<StoredObject> object = repository.findByObjectKey(objectKey);

        object.ifPresent(repository::delete);

        return object.map(MetadataStore::plan);
    }

    private static ObjectPlan plan(StoredObject object) {
        return new ObjectPlan(
                object.getId(),
                object.getObjectKey(),
                object.getSizeBytes(),
                object.getChunkSizeBytes(),
                object.getSha256(),
                object.getChunks().stream()
                        .map(MetadataStore::chunkPlan)
                        .toList()
        );
    }

    private static ObjectPlan.ChunkPlan chunkPlan(Chunk chunk) {
        return new ObjectPlan.ChunkPlan(
                chunk.getId(),
                chunk.getChunkIndex(),
                chunk.getSizeBytes(),
                chunk.getSha256(),
                chunk.getReplicas().stream()
                        .map(ChunkReplica::getNodeId)
                        .toList()
        );
    }
}

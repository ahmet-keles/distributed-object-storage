package com.ahmetkeles.metadataservice.service;

import com.ahmetkeles.metadataservice.client.StorageNodeClient;
import com.ahmetkeles.metadataservice.client.StorageNodeUnavailableException;
import com.ahmetkeles.metadataservice.config.StorageProperties;
import com.ahmetkeles.metadataservice.domain.Chunk;
import com.ahmetkeles.metadataservice.domain.StoredObject;
import com.ahmetkeles.metadataservice.placement.ChunkPlacementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The object store's core workflow.
 *
 * <p><b>Upload</b> is write-through with metadata-last: chunks are cut,
 * hashed, placed deterministically, and every replica is written to its node
 * before a single metadata row is committed. If any replica write fails the
 * upload aborts, already-written chunks are deleted best-effort, and no
 * metadata exists — a committed object is therefore always fully replicated.
 *
 * <p><b>Download</b> is replica-failover with end-to-end verification: each
 * chunk is fetched from its recorded replicas in priority order, every
 * fetched chunk must hash to the SHA-256 recorded at upload (an unreachable
 * node and a corrupted replica are handled identically — try the next one),
 * and the reassembled object must hash to the recorded object checksum.
 *
 * <p><b>Delete</b> removes metadata first (atomic disappearance), then
 * deletes chunk bytes from the nodes best-effort; a failed node delete
 * orphans bytes, never resurrects the object.
 */
@Service
public class ObjectStorageService {

    private static final Logger log =
            LoggerFactory.getLogger(ObjectStorageService.class);

    private static final Pattern OBJECT_KEY =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,199}");

    private final MetadataStore metadataStore;
    private final ChunkPlacementService placementService;
    private final StorageNodeClient nodeClient;
    private final StorageProperties properties;
    private final Map<String, StorageProperties.Node> nodesById;

    public ObjectStorageService(
            MetadataStore metadataStore,
            ChunkPlacementService placementService,
            StorageNodeClient nodeClient,
            StorageProperties properties
    ) {
        this.metadataStore = metadataStore;
        this.placementService = placementService;
        this.nodeClient = nodeClient;
        this.properties = properties;
        this.nodesById = properties.nodes().stream()
                .collect(Collectors.toMap(
                        StorageProperties.Node::id, Function.identity()));
    }

    public ObjectPlan upload(String objectKey, byte[] content) {
        validateKey(objectKey);

        if (content == null || content.length == 0) {
            throw new InvalidObjectRequestException(
                    "Object body must not be empty");
        }

        if (content.length > properties.maxObjectSizeBytes()) {
            throw new ObjectTooLargeException(
                    content.length, properties.maxObjectSizeBytes());
        }

        if (metadataStore.exists(objectKey)) {
            throw new ObjectAlreadyExistsException(objectKey);
        }

        UUID objectId = UUID.randomUUID();
        List<byte[]> parts = Chunker.split(
                content, properties.chunkSizeBytes());

        StoredObject object = new StoredObject(
                objectId,
                objectKey,
                content.length,
                properties.chunkSizeBytes(),
                Checksums.sha256Hex(content)
        );

        record PendingWrite(StorageProperties.Node node, UUID chunkId) {
        }
        List<PendingWrite> written = new ArrayList<>();

        try {
            for (int index = 0; index < parts.size(); index++) {
                byte[] bytes = parts.get(index);
                String chunkSha = Checksums.sha256Hex(bytes);
                UUID chunkId = UUID.randomUUID();

                Chunk chunk = object.addChunk(
                        chunkId, index, bytes.length, chunkSha);

                List<StorageProperties.Node> replicas =
                        placementService.replicasFor(objectId, index);

                for (int priority = 0; priority < replicas.size();
                        priority++) {
                    StorageProperties.Node node = replicas.get(priority);
                    nodeClient.putChunk(node, chunkId, bytes, chunkSha);
                    written.add(new PendingWrite(node, chunkId));
                    chunk.addReplica(node.id(), priority);
                }
            }

            metadataStore.save(object);
        } catch (StorageNodeUnavailableException exception) {
            cleanUp(written, PendingWrite::node, PendingWrite::chunkId);
            throw new ObjectUploadException(objectKey, exception);
        } catch (DataIntegrityViolationException exception) {
            // Lost a concurrent-upload race on the unique object key after
            // the bytes were already placed: remove them and report the
            // conflict.
            cleanUp(written, PendingWrite::node, PendingWrite::chunkId);
            throw new ObjectAlreadyExistsException(objectKey);
        }

        return metadataStore.planFor(objectKey).orElseThrow();
    }

    public byte[] download(String objectKey) {
        validateKey(objectKey);

        ObjectPlan plan = metadataStore.planFor(objectKey)
                .orElseThrow(() -> new ObjectNotFoundException(objectKey));

        ByteArrayOutputStream assembled =
                new ByteArrayOutputStream((int) plan.sizeBytes());

        for (ObjectPlan.ChunkPlan chunk : plan.chunks()) {
            assembled.writeBytes(fetchChunk(plan.objectKey(), chunk));
        }

        byte[] content = assembled.toByteArray();

        // Defense in depth: every chunk already verified individually, so
        // this can only fire on a metadata bug — better a refused download
        // than silently serving bytes that don't match the recorded object.
        if (!Checksums.sha256Hex(content).equals(plan.sha256())) {
            throw new ObjectUnreadableException(objectKey, -1);
        }

        return content;
    }

    public ObjectPlan metadata(String objectKey) {
        validateKey(objectKey);

        return metadataStore.planFor(objectKey)
                .orElseThrow(() -> new ObjectNotFoundException(objectKey));
    }

    public void delete(String objectKey) {
        validateKey(objectKey);

        ObjectPlan plan = metadataStore.deleteByKey(objectKey)
                .orElseThrow(() -> new ObjectNotFoundException(objectKey));

        for (ObjectPlan.ChunkPlan chunk : plan.chunks()) {
            for (String nodeId : chunk.nodeIds()) {
                StorageProperties.Node node = nodesById.get(nodeId);

                if (node == null) {
                    log.warn("Chunk {} replica on unknown node {} cannot be "
                                    + "deleted; bytes are orphaned",
                            chunk.chunkId(), nodeId);
                    continue;
                }

                try {
                    nodeClient.deleteChunk(node, chunk.chunkId());
                } catch (StorageNodeUnavailableException exception) {
                    log.warn("Best-effort delete of chunk {} on node {} "
                                    + "failed; bytes are orphaned",
                            chunk.chunkId(), nodeId, exception);
                }
            }
        }
    }

    private byte[] fetchChunk(String objectKey, ObjectPlan.ChunkPlan chunk) {
        for (String nodeId : chunk.nodeIds()) {
            StorageProperties.Node node = nodesById.get(nodeId);

            if (node == null) {
                log.warn("Chunk {} lists unknown node {}; skipping replica",
                        chunk.chunkId(), nodeId);
                continue;
            }

            byte[] bytes;
            try {
                bytes = nodeClient.getChunk(node, chunk.chunkId());
            } catch (StorageNodeUnavailableException exception) {
                log.warn("Replica of chunk {} on node {} unavailable; "
                                + "trying next replica",
                        chunk.chunkId(), nodeId);
                continue;
            }

            if (!Checksums.sha256Hex(bytes).equals(chunk.sha256())) {
                log.warn("Replica of chunk {} on node {} failed checksum "
                                + "verification; trying next replica",
                        chunk.chunkId(), nodeId);
                continue;
            }

            return bytes;
        }

        throw new ObjectUnreadableException(objectKey, chunk.chunkIndex());
    }

    private <T> void cleanUp(
            List<T> written,
            Function<T, StorageProperties.Node> node,
            Function<T, UUID> chunkId
    ) {
        for (T write : written) {
            try {
                nodeClient.deleteChunk(node.apply(write),
                        chunkId.apply(write));
            } catch (StorageNodeUnavailableException exception) {
                log.warn("Cleanup of chunk {} on node {} after a failed "
                                + "upload did not succeed; bytes are orphaned",
                        chunkId.apply(write), node.apply(write).id());
            }
        }
    }

    private static void validateKey(String objectKey) {
        if (objectKey == null || !OBJECT_KEY.matcher(objectKey).matches()) {
            throw new InvalidObjectRequestException(
                    "Object key must match [A-Za-z0-9][A-Za-z0-9._-]{0,199}");
        }
    }
}

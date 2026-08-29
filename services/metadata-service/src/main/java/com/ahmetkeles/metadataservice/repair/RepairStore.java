package com.ahmetkeles.metadataservice.repair;

import com.ahmetkeles.metadataservice.domain.Chunk;
import com.ahmetkeles.metadataservice.domain.ChunkReplica;
import com.ahmetkeles.metadataservice.repository.ChunkRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * The transactional metadata side of repair. Same contract as the rest of
 * the metadata layer: node I/O never happens inside these methods — the
 * sweep takes detached snapshots out, writes bytes to nodes, and only then
 * comes back to record what was verifiably written.
 */
@Service
public class RepairStore {

    /** What happened to a metadata append for a freshly written replica. */
    public enum RecordOutcome {

        /** The replica row was added; the copy is now part of the object. */
        RECORDED,

        /** Another repairer recorded this node first; nothing to do. */
        ALREADY_RECORDED,

        /**
         * The chunk no longer exists (its object was deleted mid-repair);
         * the written bytes must be cleaned up by the caller.
         */
        CHUNK_GONE
    }

    private final ChunkRepository chunkRepository;

    public RepairStore(ChunkRepository chunkRepository) {
        this.chunkRepository = chunkRepository;
    }

    /**
     * One page of chunk snapshots, ordered by chunk id so a sweep visits
     * every chunk exactly once even while it appends replica rows. Page
     * size bounds memory; the per-chunk lazy loads inside are fine at that
     * granularity.
     */
    @Transactional(readOnly = true)
    public List<ChunkHealthView> chunkPage(int page, int size) {
        return chunkRepository
                .findAll(PageRequest.of(page, size, Sort.by("id")))
                .map(RepairStore::view)
                .getContent();
    }

    @Transactional(readOnly = true)
    public boolean chunkExists(UUID chunkId) {
        return chunkRepository.existsById(chunkId);
    }

    /**
     * Appends a replica row for a copy that was already written to the node
     * and verified by readback. The chunk row is taken {@code FOR UPDATE}
     * first, so concurrent repairers serialize here: the second one finds
     * the node already recorded and no-ops, and the next free priority is
     * assigned race-free. A chunk deleted mid-repair reports
     * {@link RecordOutcome#CHUNK_GONE} so the caller can remove the
     * now-orphaned bytes.
     */
    @Transactional
    public RecordOutcome recordRepairedReplica(UUID chunkId, String nodeId) {
        Chunk chunk = chunkRepository.lockById(chunkId).orElse(null);

        if (chunk == null) {
            return RecordOutcome.CHUNK_GONE;
        }

        List<ChunkReplica> replicas = chunk.getReplicas();

        if (replicas.stream()
                .anyMatch(replica -> replica.getNodeId().equals(nodeId))) {
            return RecordOutcome.ALREADY_RECORDED;
        }

        int nextPriority = replicas.stream()
                .mapToInt(ChunkReplica::getPriority)
                .max()
                .orElse(-1) + 1;

        chunk.addReplica(nodeId, nextPriority);

        return RecordOutcome.RECORDED;
    }

    private static ChunkHealthView view(Chunk chunk) {
        return new ChunkHealthView(
                chunk.getId(),
                chunk.getObject().getId(),
                chunk.getChunkIndex(),
                chunk.getSha256(),
                chunk.getReplicas().stream()
                        .map(ChunkReplica::getNodeId)
                        .toList()
        );
    }
}

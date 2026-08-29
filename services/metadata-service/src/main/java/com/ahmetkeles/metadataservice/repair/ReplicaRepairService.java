package com.ahmetkeles.metadataservice.repair;

import com.ahmetkeles.metadataservice.client.ReplicaProbe;
import com.ahmetkeles.metadataservice.client.StorageNodeClient;
import com.ahmetkeles.metadataservice.client.StorageNodeUnavailableException;
import com.ahmetkeles.metadataservice.config.RepairProperties;
import com.ahmetkeles.metadataservice.config.StorageProperties;
import com.ahmetkeles.metadataservice.placement.ChunkPlacementService;
import com.ahmetkeles.metadataservice.service.Checksums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Rebuilds lost redundancy. A sweep pages over every chunk, probes each
 * recorded replica (present / missing / unreachable), and for every chunk
 * below the replication factor:
 *
 * <ol>
 *   <li>fetches a surviving copy and verifies it against the chunk's
 *       recorded SHA-256 — repair only ever propagates proven bytes;</li>
 *   <li>rewrites the verified bytes to recorded nodes that are reachable
 *       but have lost them (restore in place, no metadata change);</li>
 *   <li>writes new copies to nodes chosen from the chunk's deterministic
 *       placement ring, reads each copy back, re-verifies its checksum,
 *       and only then appends the replica row.</li>
 * </ol>
 *
 * <p>The ordering is the invariant: bytes are on the node and verified
 * before the metadata says so, so recorded replicas never point at copies
 * that were not proven to exist. The metadata append itself is serialized
 * per chunk ({@code SELECT ... FOR UPDATE} plus unique constraints), which
 * makes concurrent sweeps — including on other coordinator instances —
 * converge to exactly one recorded row per node, and byte writes are
 * idempotent, so repair can run any number of times without changing the
 * outcome. A sweep is stateless; unreachable nodes are cached per sweep so
 * a dead node costs one timeout, not one per chunk.
 */
@Service
public class ReplicaRepairService {

    private static final Logger log =
            LoggerFactory.getLogger(ReplicaRepairService.class);

    private final RepairStore repairStore;
    private final StorageNodeClient nodeClient;
    private final ChunkPlacementService placementService;
    private final StorageProperties properties;
    private final RepairProperties repairProperties;
    private final Map<String, StorageProperties.Node> nodesById;

    public ReplicaRepairService(
            RepairStore repairStore,
            StorageNodeClient nodeClient,
            ChunkPlacementService placementService,
            StorageProperties properties,
            RepairProperties repairProperties
    ) {
        this.repairStore = repairStore;
        this.nodeClient = nodeClient;
        this.placementService = placementService;
        this.properties = properties;
        this.repairProperties = repairProperties;
        this.nodesById = properties.nodes().stream()
                .collect(Collectors.toMap(
                        StorageProperties.Node::id, Function.identity()));
    }

    /** Runs one full repair sweep over every chunk. Safe to run anytime. */
    public RepairReport sweep() {
        Counters counters = new Counters();
        Set<String> downNodes = new HashSet<>();

        for (int page = 0; ; page++) {
            List<ChunkHealthView> chunks = repairStore.chunkPage(
                    page, repairProperties.scanPageSize());

            if (chunks.isEmpty()) {
                break;
            }

            for (ChunkHealthView chunk : chunks) {
                counters.scanned++;
                repairChunk(chunk, downNodes, counters);
            }
        }

        return counters.report();
    }

    private void repairChunk(ChunkHealthView chunk, Set<String> downNodes,
                             Counters counters) {
        Map<String, ReplicaProbe> probes = probeReplicas(chunk, downNodes);

        int present = (int) probes.values().stream()
                .filter(probe -> probe == ReplicaProbe.PRESENT)
                .count();
        int target = properties.replicationFactor();

        if (present >= target) {
            return;
        }

        counters.degraded++;

        byte[] bytes = fetchVerifiedSource(chunk, probes, downNodes);
        present = (int) probes.values().stream()
                .filter(probe -> probe == ReplicaProbe.PRESENT)
                .count();

        if (bytes == null) {
            counters.withoutVerifiedSource++;
            counters.stillDegraded++;
            log.error("Chunk {} of object {} has no replica that passes "
                            + "checksum verification; repair cannot invent "
                            + "the data — statuses: {}",
                    chunk.chunkId(), chunk.objectId(), probes);
            return;
        }

        List<String> restoredTo = restoreInPlace(
                chunk, probes, bytes, downNodes, counters);
        present += restoredTo.size();

        boolean chunkGone = false;
        boolean recorded = false;

        if (present < target) {
            RecreateResult result = recreateOnNewNodes(
                    chunk, probes, bytes, downNodes, target - present,
                    counters);
            present += result.added;
            recorded = result.recordConfirmedChunk;
            chunkGone = result.chunkGone;
        }

        // A restore-in-place never touches metadata, so a concurrent object
        // delete could slip past it unnoticed: re-check and un-orphan.
        if (!restoredTo.isEmpty() && !recorded && !chunkGone
                && !repairStore.chunkExists(chunk.chunkId())) {
            chunkGone = true;
        }

        if (chunkGone) {
            // The object was deleted while we were repairing it: remove the
            // copies this sweep restored best-effort and count nothing —
            // there is nothing left to repair.
            for (String nodeId : restoredTo) {
                bestEffortDelete(nodesById.get(nodeId), chunk.chunkId());
            }
            counters.degraded--;
            counters.replicasRestoredInPlace -= restoredTo.size();
            return;
        }

        if (present < target) {
            counters.stillDegraded++;
            log.warn("Chunk {} of object {} is still at {}/{} replicas "
                            + "after repair: not enough reachable nodes",
                    chunk.chunkId(), chunk.objectId(), present, target);
        }
    }

    private Map<String, ReplicaProbe> probeReplicas(
            ChunkHealthView chunk, Set<String> downNodes) {
        Map<String, ReplicaProbe> probes = new LinkedHashMap<>();

        for (String nodeId : chunk.replicaNodeIds()) {
            StorageProperties.Node node = nodesById.get(nodeId);

            if (node == null || downNodes.contains(nodeId)) {
                probes.put(nodeId, ReplicaProbe.UNREACHABLE);
                continue;
            }

            ReplicaProbe probe = nodeClient.probeChunk(node, chunk.chunkId());

            if (probe == ReplicaProbe.UNREACHABLE) {
                downNodes.add(nodeId);
            }

            probes.put(nodeId, probe);
        }

        return probes;
    }

    /**
     * Reads a surviving copy and proves it against the recorded checksum.
     * Replicas that turn out corrupt or unreachable on read are demoted in
     * {@code probes} — they are not healthy, whatever the presence probe
     * said. Returns null when no copy verifies.
     */
    private byte[] fetchVerifiedSource(ChunkHealthView chunk,
                                       Map<String, ReplicaProbe> probes,
                                       Set<String> downNodes) {
        for (Map.Entry<String, ReplicaProbe> entry : probes.entrySet()) {
            if (entry.getValue() != ReplicaProbe.PRESENT) {
                continue;
            }

            String nodeId = entry.getKey();
            byte[] bytes;

            try {
                bytes = nodeClient.getChunk(
                        nodesById.get(nodeId), chunk.chunkId());
            } catch (StorageNodeUnavailableException exception) {
                downNodes.add(nodeId);
                entry.setValue(ReplicaProbe.UNREACHABLE);
                continue;
            }

            if (!Checksums.sha256Hex(bytes).equals(chunk.sha256())) {
                log.warn("Replica of chunk {} on node {} fails checksum "
                                + "verification and cannot seed a repair",
                        chunk.chunkId(), nodeId);
                entry.setValue(ReplicaProbe.MISSING);
                continue;
            }

            return bytes;
        }

        return null;
    }

    /** Rewrites the verified bytes to recorded nodes that lost their copy. */
    private List<String> restoreInPlace(ChunkHealthView chunk,
                                        Map<String, ReplicaProbe> probes,
                                        byte[] bytes,
                                        Set<String> downNodes,
                                        Counters counters) {
        List<String> restoredTo = new ArrayList<>();

        for (Map.Entry<String, ReplicaProbe> entry : probes.entrySet()) {
            if (entry.getValue() != ReplicaProbe.MISSING) {
                continue;
            }

            StorageProperties.Node node = nodesById.get(entry.getKey());

            if (node == null || !writeAndVerify(node, chunk, bytes,
                    downNodes)) {
                continue;
            }

            restoredTo.add(node.id());
            counters.replicasRestoredInPlace++;
        }

        return restoredTo;
    }

    private record RecreateResult(int added, boolean recordConfirmedChunk,
                                  boolean chunkGone) {
    }

    /**
     * Writes new copies to nodes that have none, walking the chunk's
     * placement ring so every repairer picks the same targets, and records
     * each copy only after it has been read back and re-verified.
     */
    private RecreateResult recreateOnNewNodes(ChunkHealthView chunk,
                                              Map<String, ReplicaProbe> probes,
                                              byte[] bytes,
                                              Set<String> downNodes,
                                              int needed,
                                              Counters counters) {
        int added = 0;
        boolean recordConfirmedChunk = false;

        for (StorageProperties.Node candidate
                : placementService.ringFor(chunk.objectId(),
                chunk.chunkIndex())) {
            if (added >= needed) {
                break;
            }

            if (probes.containsKey(candidate.id())
                    || downNodes.contains(candidate.id())) {
                continue;
            }

            if (!writeAndVerify(candidate, chunk, bytes, downNodes)) {
                continue;
            }

            switch (repairStore.recordRepairedReplica(
                    chunk.chunkId(), candidate.id())) {
                case RECORDED -> {
                    added++;
                    counters.replicasRecreated++;
                    recordConfirmedChunk = true;
                    log.info("Re-created replica of chunk {} (object {}) "
                                    + "on node {}",
                            chunk.chunkId(), chunk.objectId(),
                            candidate.id());
                }
                case ALREADY_RECORDED -> {
                    // A concurrent repairer recorded this node first. The
                    // copy is there and verified either way.
                    added++;
                    recordConfirmedChunk = true;
                }
                case CHUNK_GONE -> {
                    bestEffortDelete(candidate, chunk.chunkId());
                    return new RecreateResult(added, recordConfirmedChunk,
                            true);
                }
            }
        }

        return new RecreateResult(added, recordConfirmedChunk, false);
    }

    /**
     * Writes the chunk to the node and proves the write by reading it back
     * and hashing it — the node's own write-time verification is not taken
     * on faith. A copy that cannot be proven is deleted best-effort and
     * does not count.
     */
    private boolean writeAndVerify(StorageProperties.Node node,
                                   ChunkHealthView chunk,
                                   byte[] bytes,
                                   Set<String> downNodes) {
        try {
            nodeClient.putChunk(node, chunk.chunkId(), bytes, chunk.sha256());

            byte[] readBack = nodeClient.getChunk(node, chunk.chunkId());

            if (Checksums.sha256Hex(readBack).equals(chunk.sha256())) {
                return true;
            }

            log.warn("Node {} does not serve back the bytes just written "
                            + "for chunk {}; discarding the copy",
                    node.id(), chunk.chunkId());
            bestEffortDelete(node, chunk.chunkId());
            return false;
        } catch (StorageNodeUnavailableException exception) {
            log.warn("Writing chunk {} to node {} failed; node treated as "
                            + "down for this sweep",
                    chunk.chunkId(), node.id());
            downNodes.add(node.id());
            return false;
        }
    }

    private void bestEffortDelete(StorageProperties.Node node, UUID chunkId) {
        if (node == null) {
            return;
        }

        try {
            nodeClient.deleteChunk(node, chunkId);
        } catch (StorageNodeUnavailableException exception) {
            log.warn("Cleanup of repair copy of chunk {} on node {} failed; "
                    + "bytes are orphaned", chunkId, node.id());
        }
    }

    private static final class Counters {
        int scanned;
        int degraded;
        int replicasRestoredInPlace;
        int replicasRecreated;
        int withoutVerifiedSource;
        int stillDegraded;

        RepairReport report() {
            return new RepairReport(scanned, degraded,
                    replicasRestoredInPlace, replicasRecreated,
                    withoutVerifiedSource, stillDegraded);
        }
    }
}

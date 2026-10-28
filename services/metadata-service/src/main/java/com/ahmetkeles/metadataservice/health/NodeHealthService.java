package com.ahmetkeles.metadataservice.health;

import com.ahmetkeles.metadataservice.client.StorageNodeClient;
import com.ahmetkeles.metadataservice.config.StorageProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Periodically probes configured node health and publishes stable snapshots. */
@Service
public class NodeHealthService {

    private final StorageNodeClient nodeClient;
    private final List<StorageProperties.Node> nodes;
    private final Map<String, NodeHealthView> statusByNode =
            new ConcurrentHashMap<>();

    public NodeHealthService(
            StorageNodeClient nodeClient,
            StorageProperties properties
    ) {
        this.nodeClient = nodeClient;
        this.nodes = properties.nodes();
        for (StorageProperties.Node node : nodes) {
            statusByNode.put(node.id(), new NodeHealthView(
                    node.id(), NodeHealthStatus.UNKNOWN, null));
        }
    }

    @Scheduled(fixedDelayString = "${storage.health.interval:PT5S}")
    public void refresh() {
        for (StorageProperties.Node node : nodes) {
            boolean healthy = nodeClient.probeNode(node);
            Instant checkedAt = Instant.now();

            statusByNode.compute(node.id(), (nodeId, previous) -> {
                NodeHealthStatus next;
                if (!healthy) {
                    next = NodeHealthStatus.DOWN;
                } else if (previous.status() == NodeHealthStatus.DOWN
                        || previous.status() == NodeHealthStatus.RECOVERING) {
                    next = previous.status() == NodeHealthStatus.DOWN
                            ? NodeHealthStatus.RECOVERING
                            : NodeHealthStatus.HEALTHY;
                } else {
                    next = NodeHealthStatus.HEALTHY;
                }

                return new NodeHealthView(nodeId, next, checkedAt);
            });
        }
    }

    public List<NodeHealthView> snapshot() {
        return statusByNode.values().stream()
                .sorted(Comparator.comparing(NodeHealthView::nodeId))
                .toList();
    }

    public boolean isDown(String nodeId) {
        NodeHealthView status = statusByNode.get(nodeId);
        return status != null && status.status() == NodeHealthStatus.DOWN;
    }
}

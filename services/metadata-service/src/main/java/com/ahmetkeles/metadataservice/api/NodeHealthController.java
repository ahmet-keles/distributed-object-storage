package com.ahmetkeles.metadataservice.api;

import com.ahmetkeles.metadataservice.health.NodeHealthService;
import com.ahmetkeles.metadataservice.health.NodeHealthView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/nodes")
public class NodeHealthController {

    private final NodeHealthService nodeHealthService;

    public NodeHealthController(NodeHealthService nodeHealthService) {
        this.nodeHealthService = nodeHealthService;
    }

    @GetMapping("/health")
    public List<NodeHealthView> health() {
        return nodeHealthService.snapshot();
    }
}

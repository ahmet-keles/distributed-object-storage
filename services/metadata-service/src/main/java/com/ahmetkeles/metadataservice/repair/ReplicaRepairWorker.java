package com.ahmetkeles.metadataservice.repair;

import com.ahmetkeles.metadataservice.config.RepairProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background driver of {@link ReplicaRepairService}: one sweep, then
 * {@code storage.repair.interval} of pause, repeatedly (fixed delay, so
 * sweeps never overlap within an instance; across instances the store's
 * per-chunk locking keeps concurrent sweeps safe). A sweep that throws is
 * logged and the schedule continues — repair is a convergence loop, not a
 * one-shot job.
 */
@Component
public class ReplicaRepairWorker {

    private static final Logger log =
            LoggerFactory.getLogger(ReplicaRepairWorker.class);

    private final ReplicaRepairService repairService;
    private final RepairProperties properties;

    public ReplicaRepairWorker(ReplicaRepairService repairService,
                               RepairProperties properties) {
        this.repairService = repairService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${storage.repair.interval}")
    public void runSweep() {
        if (!properties.enabled()) {
            return;
        }

        try {
            RepairReport report = repairService.sweep();

            if (!report.quiet()) {
                log.info("Repair sweep: {}", report);
            }
        } catch (RuntimeException exception) {
            log.error("Repair sweep failed; will retry after the configured "
                    + "interval", exception);
        }
    }
}

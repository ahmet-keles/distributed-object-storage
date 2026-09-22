package com.ahmetkeles.metadataservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Replica-repair policy. {@code enabled} gates the background worker only —
 * the repair logic itself stays invocable (tests drive sweeps directly), and
 * {@code interval} is the pause between the end of one sweep and the start
 * of the next.
 */
@ConfigurationProperties(prefix = "storage.repair")
public record RepairProperties(
        boolean enabled,
        Duration interval,
        int scanPageSize
) {

    public RepairProperties {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException(
                    "storage.repair.interval must be a positive duration");
        }

        if (scanPageSize <= 0) {
            throw new IllegalArgumentException(
                    "storage.repair.scan-page-size must be positive");
        }
    }
}

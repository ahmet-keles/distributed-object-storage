package com.ahmetkeles.metadataservice.repair;

/**
 * What one repair sweep saw and did. "Degraded" counts chunks whose healthy
 * (present) replica count was below the replication factor when assessed;
 * "still degraded" counts the subset the sweep could not bring back to full
 * strength — because too few nodes were reachable, or because no surviving
 * copy passed checksum verification (those are also counted separately as
 * {@code chunksWithoutVerifiedSource}).
 */
public record RepairReport(
        int chunksScanned,
        int chunksDegraded,
        int replicasRestoredInPlace,
        int replicasRecreated,
        int chunksWithoutVerifiedSource,
        int chunksStillDegraded
) {

    /** True when the sweep neither found nor left anything degraded. */
    public boolean quiet() {
        return chunksDegraded == 0 && chunksStillDegraded == 0;
    }
}

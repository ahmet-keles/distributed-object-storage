package com.ahmetkeles.metadataservice.client;

/**
 * Result of probing one recorded replica on one storage node. A probe
 * answers presence, not integrity: PRESENT means the node stores bytes under
 * the chunk id, and only reading and hashing them proves they are the right
 * bytes — which the repair and download paths do before trusting a copy.
 */
public enum ReplicaProbe {

    /** The node is reachable and stores bytes under the chunk id. */
    PRESENT,

    /** The node is reachable but has no bytes under the chunk id. */
    MISSING,

    /** The node cannot be reached (or answered with a server error). */
    UNREACHABLE
}

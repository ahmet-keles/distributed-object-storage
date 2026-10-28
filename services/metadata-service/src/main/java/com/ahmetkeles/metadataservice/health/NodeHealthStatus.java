package com.ahmetkeles.metadataservice.health;

/** Reachability state of one configured storage node. */
public enum NodeHealthStatus {
    UNKNOWN,
    HEALTHY,
    DOWN,
    RECOVERING
}

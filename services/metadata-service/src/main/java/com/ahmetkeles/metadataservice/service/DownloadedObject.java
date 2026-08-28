package com.ahmetkeles.metadataservice.service;

/**
 * A downloaded object together with the exact plan its bytes were fetched
 * and verified against. Callers must take the checksum (and any other
 * metadata they expose) from {@link #plan()}, never from a second metadata
 * read: the object may be deleted or replaced under the same key between
 * reads, and a body paired with another version's checksum would be a lie.
 */
public record DownloadedObject(byte[] content, ObjectPlan plan) {
}

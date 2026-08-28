package com.ahmetkeles.metadataservice.service;

/**
 * Every recorded replica of some chunk was unavailable or failed its
 * checksum, so the object cannot currently be reassembled. The metadata is
 * intact; the read may succeed again once a node returns.
 */
public class ObjectUnreadableException extends RuntimeException {

    public ObjectUnreadableException(String objectKey, int chunkIndex) {
        super("Object " + objectKey + " is unreadable: no replica of chunk "
                + chunkIndex + " returned intact bytes");
    }
}

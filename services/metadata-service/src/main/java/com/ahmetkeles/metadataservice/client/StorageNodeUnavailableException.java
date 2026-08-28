package com.ahmetkeles.metadataservice.client;

/**
 * A storage-node operation failed for any reason — connection refused,
 * timeout, or a non-2xx response. Callers treat the node as unusable for
 * that operation and fail over (reads) or abort (writes).
 */
public class StorageNodeUnavailableException extends RuntimeException {

    public StorageNodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

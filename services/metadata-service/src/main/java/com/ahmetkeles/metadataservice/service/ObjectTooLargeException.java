package com.ahmetkeles.metadataservice.service;

public class ObjectTooLargeException extends RuntimeException {

    public ObjectTooLargeException(long size, long max) {
        super("Object of " + size + " bytes exceeds the configured maximum of "
                + max + " bytes");
    }
}

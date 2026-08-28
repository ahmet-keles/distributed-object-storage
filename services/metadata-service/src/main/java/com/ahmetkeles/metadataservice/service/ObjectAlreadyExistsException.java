package com.ahmetkeles.metadataservice.service;

public class ObjectAlreadyExistsException extends RuntimeException {

    public ObjectAlreadyExistsException(String objectKey) {
        super("Object already exists: " + objectKey
                + " (milestone 1 has no overwrite; delete it first)");
    }
}

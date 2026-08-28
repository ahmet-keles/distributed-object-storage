package com.ahmetkeles.metadataservice.service;

public class ObjectNotFoundException extends RuntimeException {

    public ObjectNotFoundException(String objectKey) {
        super("Object not found: " + objectKey);
    }
}

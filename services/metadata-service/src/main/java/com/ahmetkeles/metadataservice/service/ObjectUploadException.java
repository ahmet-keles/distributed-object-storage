package com.ahmetkeles.metadataservice.service;

/**
 * The upload could not place every replica of every chunk. Nothing was
 * committed to the metadata database; chunks already written to nodes were
 * deleted best-effort.
 */
public class ObjectUploadException extends RuntimeException {

    public ObjectUploadException(String objectKey, Throwable cause) {
        super("Upload of object " + objectKey + " failed: could not write "
                + "every replica; no metadata was recorded", cause);
    }
}

package com.ahmetkeles.storagenode;

public class ChecksumMismatchException extends RuntimeException {

    public ChecksumMismatchException(String expected, String actual) {
        super("Declared SHA-256 " + expected
                + " does not match received bytes (" + actual + ")");
    }
}

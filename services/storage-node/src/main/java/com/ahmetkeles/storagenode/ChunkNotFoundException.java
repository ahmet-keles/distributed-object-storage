package com.ahmetkeles.storagenode;

public class ChunkNotFoundException extends RuntimeException {

    public ChunkNotFoundException(String chunkId) {
        super("Chunk not found: " + chunkId);
    }
}

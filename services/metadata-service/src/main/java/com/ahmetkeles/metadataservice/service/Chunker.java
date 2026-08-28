package com.ahmetkeles.metadataservice.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fixed-size chunking: every chunk is exactly {@code chunkSizeBytes} long
 * except the last, which carries the remainder. Chunk boundaries are a pure
 * function of (content length, chunk size), so metadata alone is enough to
 * know each chunk's offset when reassembling.
 */
public final class Chunker {

    private Chunker() {
    }

    public static List<byte[]> split(byte[] content, int chunkSizeBytes) {
        if (chunkSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "chunkSizeBytes must be positive");
        }

        if (content.length == 0) {
            throw new IllegalArgumentException(
                    "content must not be empty");
        }

        List<byte[]> chunks = new ArrayList<>();

        for (int offset = 0; offset < content.length;
                offset += chunkSizeBytes) {
            int end = Math.min(offset + chunkSizeBytes, content.length);
            chunks.add(Arrays.copyOfRange(content, offset, end));
        }

        return chunks;
    }
}

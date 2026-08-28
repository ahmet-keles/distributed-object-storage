package com.ahmetkeles.metadataservice;

import com.ahmetkeles.metadataservice.service.Checksums;
import com.ahmetkeles.metadataservice.service.Chunker;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkerTest {

    @Test
    void splitsIntoFixedSizeChunksWithRemainderLast() {
        byte[] content = randomBytes(10);

        List<byte[]> chunks = Chunker.split(content, 4);

        assertEquals(3, chunks.size());
        assertEquals(4, chunks.get(0).length);
        assertEquals(4, chunks.get(1).length);
        assertEquals(2, chunks.get(2).length);
    }

    @Test
    void exactMultipleProducesNoShortChunk() {
        List<byte[]> chunks = Chunker.split(randomBytes(8), 4);

        assertEquals(2, chunks.size());
        assertEquals(4, chunks.get(0).length);
        assertEquals(4, chunks.get(1).length);
    }

    @Test
    void contentSmallerThanChunkSizeIsOneChunk() {
        assertEquals(1, Chunker.split(randomBytes(3), 4).size());
    }

    @Test
    void concatenatingChunksReproducesTheContent() {
        byte[] content = randomBytes(1000);

        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        Chunker.split(content, 64).forEach(reassembled::writeBytes);

        assertArrayEquals(content, reassembled.toByteArray());
        assertEquals(Checksums.sha256Hex(content),
                Checksums.sha256Hex(reassembled.toByteArray()));
    }

    @Test
    void rejectsEmptyContentAndNonPositiveChunkSize() {
        assertThrows(IllegalArgumentException.class,
                () -> Chunker.split(new byte[0], 4));
        assertThrows(IllegalArgumentException.class,
                () -> Chunker.split(randomBytes(4), 0));
    }

    static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }
}

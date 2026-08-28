package com.ahmetkeles.storagenode;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * The node's entire wire contract. Chunk ids are UUIDs (Spring rejects
 * anything else before it can touch the filesystem), bodies are raw bytes,
 * and the {@code X-Chunk-Sha256} header carries the checksum in both
 * directions: declared by the writer on PUT, freshly recomputed from disk by
 * this node on GET so the reader can detect a corrupted replica.
 */
@RestController
@RequestMapping("/chunks")
public class ChunkController {

    public static final String SHA256_HEADER = "X-Chunk-Sha256";

    private final ChunkStore chunkStore;

    public ChunkController(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
    }

    @PutMapping("/{chunkId}")
    public ResponseEntity<Void> put(
            @PathVariable UUID chunkId,
            @RequestHeader(SHA256_HEADER) String declaredSha256,
            @RequestBody byte[] body
    ) {
        chunkStore.put(chunkId, body, declaredSha256);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/{chunkId}")
    public ResponseEntity<byte[]> get(@PathVariable UUID chunkId) {
        byte[] bytes = chunkStore.get(chunkId);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(SHA256_HEADER, ChunkStore.sha256Hex(bytes))
                .body(bytes);
    }

    @DeleteMapping("/{chunkId}")
    public ResponseEntity<Void> delete(@PathVariable UUID chunkId) {
        chunkStore.delete(chunkId);
        return ResponseEntity.noContent().build();
    }
}

package com.ahmetkeles.storagenode;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Local-filesystem chunk store. Chunk ids are UUIDs assigned by
 * metadata-service; files fan out into two-character subdirectories of the
 * data directory so no single directory grows unboundedly.
 *
 * <p>Writes verify the caller's declared SHA-256 against the received bytes
 * before anything becomes visible, and land via a temp file plus atomic move,
 * so a torn upload can never leave a half-written chunk under its final name.
 */
@Service
public class ChunkStore {

    private final Path dataDir;

    public ChunkStore(@Value("${storage.node.data-dir}") String dataDir) {
        this.dataDir = Path.of(dataDir);

        try {
            Files.createDirectories(this.dataDir);
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Cannot create data directory " + dataDir, exception);
        }
    }

    /** Stores the chunk after verifying the declared SHA-256. Idempotent. */
    public void put(UUID chunkId, byte[] bytes, String declaredSha256) {
        String actual = sha256Hex(bytes);

        if (!actual.equalsIgnoreCase(declaredSha256)) {
            throw new ChecksumMismatchException(declaredSha256, actual);
        }

        Path target = pathFor(chunkId);

        try {
            Files.createDirectories(target.getParent());

            Path temp = Files.createTempFile(
                    target.getParent(), chunkId.toString(), ".tmp");
            try {
                Files.write(temp, bytes);
                Files.move(temp, target,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException exception) {
                // The write or the move failed: the chunk never became
                // visible (atomicity is untouched), so the temp file is
                // garbage — remove it best-effort before propagating.
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException suppressed) {
                    exception.addSuppressed(suppressed);
                }
                throw exception;
            }
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to store chunk " + chunkId, exception);
        }
    }

    public byte[] get(UUID chunkId) {
        Path path = pathFor(chunkId);

        if (!Files.exists(path)) {
            throw new ChunkNotFoundException(chunkId.toString());
        }

        try {
            return Files.readAllBytes(path);
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to read chunk " + chunkId, exception);
        }
    }

    /** Removes the chunk if present. Deleting a missing chunk is a no-op. */
    public void delete(UUID chunkId) {
        try {
            Files.deleteIfExists(pathFor(chunkId));
        } catch (IOException exception) {
            throw new UncheckedIOException(
                    "Failed to delete chunk " + chunkId, exception);
        }
    }

    /**
     * The UUID type — not the raw request string — is the path-traversal
     * guard: the controller parses the id before it ever reaches the
     * filesystem, so only canonical UUID file names can be formed here.
     */
    private Path pathFor(UUID chunkId) {
        String name = chunkId.toString();
        return dataDir.resolve(name.substring(0, 2)).resolve(name);
    }

    public static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }
}

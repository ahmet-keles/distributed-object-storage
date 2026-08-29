package com.ahmetkeles.metadataservice.repository;

import com.ahmetkeles.metadataservice.domain.Chunk;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;
import java.util.UUID;

public interface ChunkRepository extends JpaRepository<Chunk, UUID> {

    /**
     * Loads the chunk row under {@code SELECT ... FOR UPDATE}. Repair
     * serializes its metadata append on this lock, so two repairers (and a
     * concurrent object delete) cannot interleave inside the
     * check-then-record window.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from Chunk c where c.id = :id")
    Optional<Chunk> lockById(UUID id);
}

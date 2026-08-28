package com.ahmetkeles.metadataservice.repository;

import com.ahmetkeles.metadataservice.domain.StoredObject;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface StoredObjectRepository
        extends JpaRepository<StoredObject, UUID> {

    Optional<StoredObject> findByObjectKey(String objectKey);

    boolean existsByObjectKey(String objectKey);
}

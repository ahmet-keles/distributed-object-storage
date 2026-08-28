package com.ahmetkeles.metadataservice.api;

import com.ahmetkeles.metadataservice.service.DownloadedObject;
import com.ahmetkeles.metadataservice.service.ObjectPlan;
import com.ahmetkeles.metadataservice.service.ObjectStorageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/objects")
public class ObjectController {

    public static final String OBJECT_SHA256_HEADER = "X-Object-Sha256";

    private final ObjectStorageService objectStorageService;

    public ObjectController(ObjectStorageService objectStorageService) {
        this.objectStorageService = objectStorageService;
    }

    @PutMapping(path = "/{key}",
            consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<ObjectMetadataResponse> upload(
            @PathVariable String key,
            @RequestBody(required = false) byte[] body
    ) {
        ObjectPlan plan = objectStorageService.upload(key, body);

        return ResponseEntity.status(HttpStatus.CREATED)
                .header(OBJECT_SHA256_HEADER, plan.sha256())
                .body(ObjectMetadataResponse.from(plan));
    }

    @GetMapping("/{key}")
    public ResponseEntity<byte[]> download(@PathVariable String key) {
        // One service call, one plan: the body and the checksum header come
        // from the same snapshot. Re-reading metadata here could pair these
        // bytes with a different object that replaced the key mid-request.
        DownloadedObject downloaded = objectStorageService.download(key);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(OBJECT_SHA256_HEADER, downloaded.plan().sha256())
                .body(downloaded.content());
    }

    @GetMapping("/{key}/metadata")
    public ObjectMetadataResponse metadata(@PathVariable String key) {
        return ObjectMetadataResponse.from(
                objectStorageService.metadata(key));
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        objectStorageService.delete(key);
        return ResponseEntity.noContent().build();
    }
}

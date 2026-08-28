package com.ahmetkeles.metadataservice.api;

import com.ahmetkeles.metadataservice.service.InvalidObjectRequestException;
import com.ahmetkeles.metadataservice.service.ObjectAlreadyExistsException;
import com.ahmetkeles.metadataservice.service.ObjectNotFoundException;
import com.ahmetkeles.metadataservice.service.ObjectTooLargeException;
import com.ahmetkeles.metadataservice.service.ObjectUnreadableException;
import com.ahmetkeles.metadataservice.service.ObjectUploadException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class RestExceptionHandler {

    record ApiError(int status, String error, String message) {
    }

    @ExceptionHandler(ObjectNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(
            ObjectNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "object_not_found",
                exception.getMessage());
    }

    @ExceptionHandler(ObjectAlreadyExistsException.class)
    public ResponseEntity<ApiError> handleConflict(
            ObjectAlreadyExistsException exception) {
        return error(HttpStatus.CONFLICT, "object_already_exists",
                exception.getMessage());
    }

    @ExceptionHandler(InvalidObjectRequestException.class)
    public ResponseEntity<ApiError> handleInvalid(
            InvalidObjectRequestException exception) {
        return error(HttpStatus.BAD_REQUEST, "invalid_request",
                exception.getMessage());
    }

    @ExceptionHandler(ObjectTooLargeException.class)
    public ResponseEntity<ApiError> handleTooLarge(
            ObjectTooLargeException exception) {
        return error(HttpStatus.CONTENT_TOO_LARGE, "object_too_large",
                exception.getMessage());
    }

    /**
     * 502, not 500: the metadata service is healthy, a dependency (the
     * storage nodes) could not satisfy the request. Uploads may be retried
     * (nothing was committed); reads may succeed once a node returns.
     */
    @ExceptionHandler({
            ObjectUploadException.class,
            ObjectUnreadableException.class
    })
    public ResponseEntity<ApiError> handleStorageFailure(
            RuntimeException exception) {
        return error(HttpStatus.BAD_GATEWAY, "storage_unavailable",
                exception.getMessage());
    }

    private ResponseEntity<ApiError> error(
            HttpStatus status, String error, String message) {
        return ResponseEntity.status(status)
                .body(new ApiError(status.value(), error, message));
    }
}

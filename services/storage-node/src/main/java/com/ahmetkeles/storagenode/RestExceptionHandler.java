package com.ahmetkeles.storagenode;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class RestExceptionHandler {

    record ApiError(int status, String error, String message) {
    }

    @ExceptionHandler(ChunkNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(
            ChunkNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "chunk_not_found",
                exception.getMessage());
    }

    @ExceptionHandler(ChecksumMismatchException.class)
    public ResponseEntity<ApiError> handleChecksumMismatch(
            ChecksumMismatchException exception) {
        return error(HttpStatus.BAD_REQUEST, "checksum_mismatch",
                exception.getMessage());
    }

    @ExceptionHandler({
            MethodArgumentTypeMismatchException.class,
            MissingRequestHeaderException.class
    })
    public ResponseEntity<ApiError> handleBadRequest(Exception exception) {
        return error(HttpStatus.BAD_REQUEST, "invalid_request",
                "Chunk id must be a UUID and " + ChunkController.SHA256_HEADER
                        + " is required on writes");
    }

    private ResponseEntity<ApiError> error(
            HttpStatus status, String error, String message) {
        return ResponseEntity.status(status)
                .body(new ApiError(status.value(), error, message));
    }
}

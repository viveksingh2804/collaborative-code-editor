package com.example.backend.websocket;

public record OperationHistoryEntry(
        Operation operation,
        String deletedText,
        long version
) {
}
package com.example.backend.websocket;

public record Operation(
        String userId,
        int position,
        int deleteCount,
        String text,
        long baseVersion
) {
}
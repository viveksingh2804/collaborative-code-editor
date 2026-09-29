package com.example.backend.websocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final CodeWebSocketHandler codeWebSocketHandler;

    public WebSocketConfig(CodeWebSocketHandler codeWebSocketHandler) {
        this.codeWebSocketHandler = codeWebSocketHandler;
    }

    @Override
public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {

    registry.addHandler(codeWebSocketHandler, "/ws")
            .setAllowedOrigins(
                    "http://localhost:5173",
                    "http://localhost:5174"
            );
}
}
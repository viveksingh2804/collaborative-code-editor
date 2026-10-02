package com.example.backend.websocket;

import com.example.backend.session.CodeSession;
import com.example.backend.session.CodeSessionRepository;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CodeWebSocketHandler extends TextWebSocketHandler {

    private final CodeSessionRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Set<WebSocketSession>> rooms =
            new ConcurrentHashMap<>();

    private final Map<String, String> userRooms =
            new ConcurrentHashMap<>();

    public CodeWebSocketHandler(CodeSessionRepository repository) {
        this.repository = repository;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {

        System.out.println(
                "WebSocket connected: " + session.getId()
        );
    }

    @Override
    protected void handleTextMessage(
            WebSocketSession session,
            TextMessage message) throws Exception {

        JsonNode data =
                objectMapper.readTree(message.getPayload());

        if (!data.has("type")) {
            return;
        }

        String type = data.get("type").asText();

        // ==============================
        // JOIN SESSION
        // ==============================

        if (type.equals("join")) {

            String sessionCode =
                    data.get("sessionId").asText();

            userRooms.put(
                    session.getId(),
                    sessionCode
            );

            rooms.computeIfAbsent(
                    sessionCode,
                    key -> ConcurrentHashMap.newKeySet()
            ).add(session);

            CodeSession codeSession =
                    repository.findBySessionCode(sessionCode)
                            .orElseGet(() ->
                                    repository.save(
                                            new CodeSession(
                                                    sessionCode,
                                                    defaultCode()
                                            )
                                    )
                            );

            // Send existing code to newly joined user
            session.sendMessage(
                    new TextMessage(
                            createCodeMessage(
                                    codeSession.getCode()
                            )
                    )
            );

            broadcastUserCount(sessionCode);

            System.out.println(
                    "User " + session.getId()
                            + " joined session "
                            + sessionCode
            );

            return;
        }

        // ==============================
        // FULL CODE UPDATE
        // ==============================

        if (type.equals("code")) {

            String sessionCode =
                    userRooms.get(session.getId());

            if (sessionCode == null) {
                return;
            }

            if (!data.has("code")) {
                return;
            }

            String code =
                    data.get("code").asText();

            CodeSession codeSession =
                    repository.findBySessionCode(sessionCode)
                            .orElseGet(() ->
                                    repository.save(
                                            new CodeSession(
                                                    sessionCode,
                                                    code
                                            )
                                    )
                            );

            codeSession.setCode(code);

            repository.save(codeSession);

            broadcastCode(
                    sessionCode,
                    code,
                    session.getId()
            );

            return;
        }
    }

    // ==============================
    // BROADCAST CODE
    // ==============================

    private void broadcastCode(
            String sessionCode,
            String code,
            String senderId) {

        Set<WebSocketSession> users =
                rooms.get(sessionCode);

        if (users == null) {
            return;
        }

        String message =
                createCodeMessage(code);

        for (WebSocketSession user : users) {

            try {

                if (user.isOpen()
                        && !user.getId().equals(senderId)) {

                    user.sendMessage(
                            new TextMessage(message)
                    );
                }

            } catch (Exception e) {

                System.out.println(
                        "Broadcast error: "
                                + e.getMessage()
                );
            }
        }
    }

    // ==============================
    // USER COUNT
    // ==============================

    private void broadcastUserCount(
            String sessionCode) {

        Set<WebSocketSession> users =
                rooms.get(sessionCode);

        if (users == null) {
            return;
        }

        String message =
                "{\"type\":\"users\",\"count\":"
                        + users.size()
                        + "}";

        for (WebSocketSession user : users) {

            try {

                if (user.isOpen()) {

                    user.sendMessage(
                            new TextMessage(message)
                    );
                }

            } catch (Exception e) {

                System.out.println(
                        "User count error: "
                                + e.getMessage()
                );
            }
        }
    }

    // ==============================
    // DISCONNECT
    // ==============================

    @Override
    public void afterConnectionClosed(
            WebSocketSession session,
            org.springframework.web.socket.CloseStatus status) {

        String sessionCode =
                userRooms.remove(session.getId());

        if (sessionCode != null) {

            Set<WebSocketSession> users =
                    rooms.get(sessionCode);

            if (users != null) {

                users.remove(session);

                if (users.isEmpty()) {

                    rooms.remove(sessionCode);

                } else {

                    broadcastUserCount(sessionCode);
                }
            }
        }

        System.out.println(
                "WebSocket disconnected: "
                        + session.getId()
        );
    }

    // ==============================
    // JSON MESSAGE
    // ==============================

    private String createCodeMessage(
            String code) {

        return "{\"type\":\"code\",\"code\":"
                + objectMapper.valueToTree(code)
                + "}";
    }

    // ==============================
    // DEFAULT CODE
    // ==============================

    private String defaultCode() {

        return """
                public class Main {
                    public static void main(String[] args) {
                        System.out.println("Hello Collaborative Editor");
                    }
                }
                """;
    }
}

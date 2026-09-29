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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class CodeWebSocketHandler extends TextWebSocketHandler {

    private final CodeSessionRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Set<WebSocketSession>> rooms =
            new ConcurrentHashMap<>();

    private final Map<String, String> userRooms =
            new ConcurrentHashMap<>();

    // Operation queue for every room
    private final Map<String, ConcurrentLinkedQueue<Operation>> operationQueues =
            new ConcurrentHashMap<>();

    // One worker per room
    private final Map<String, ExecutorService> roomExecutors =
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

        String type = data.get("type").asText();

        // =========================
        // JOIN
        // =========================

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

            operationQueues.computeIfAbsent(
                    sessionCode,
                    key -> new ConcurrentLinkedQueue<>()
            );

            roomExecutors.computeIfAbsent(
                    sessionCode,
                    key -> Executors.newSingleThreadExecutor()
            );

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

            // Send current code to joining user
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

        // =========================
        // OPERATION
        // =========================

        if (type.equals("operation")) {

            String sessionCode =
                    userRooms.get(session.getId());

            if (sessionCode == null) {
                return;
            }

            JsonNode changes =
                    data.get("changes");

            ConcurrentLinkedQueue<Operation> queue =
                    operationQueues.get(sessionCode);

            if (queue == null) {
                return;
            }

            // Add every Monaco change to queue
            for (JsonNode change : changes) {

                Operation operation =
                        new Operation(
                                session.getId(),
                                change.get("position").asInt(),
                                change.get("deleteCount").asInt(),
                                change.get("text").asText()
                        );

                queue.add(operation);
            }

            // Process operations sequentially
            processQueue(sessionCode);

            return;
        }
    }

    // =========================
    // OPERATION QUEUE
    // =========================

    private void processQueue(String sessionCode) {

        ExecutorService executor =
                roomExecutors.get(sessionCode);

        if (executor == null) {
            return;
        }

        executor.submit(() -> {

            ConcurrentLinkedQueue<Operation> queue =
                    operationQueues.get(sessionCode);

            if (queue == null) {
                return;
            }

            Operation operation;

            while ((operation = queue.poll()) != null) {

                try {

                    applyOperation(
                            sessionCode,
                            operation
                    );

                } catch (Exception e) {

                    System.out.println(
                            "Operation error: "
                                    + e.getMessage()
                    );
                }
            }
        });
    }

    // =========================
    // APPLY OPERATION
    // =========================

    private void applyOperation(
            String sessionCode,
            Operation operation) {

        CodeSession codeSession =
                repository.findBySessionCode(sessionCode)
                        .orElse(null);

        if (codeSession == null) {
            return;
        }

        String code = codeSession.getCode();

        int position = operation.position();

        if (position < 0 || position > code.length()) {
            return;
        }

        int deleteCount =
                operation.deleteCount();

        int end =
                Math.min(
                        position + deleteCount,
                        code.length()
                );

        String newCode =
                code.substring(0, position)
                        + operation.text()
                        + code.substring(end);

        // Save updated code
        codeSession.setCode(newCode);
        repository.save(codeSession);

        // Broadcast complete updated state
        broadcastCode(
                sessionCode,
                newCode,
                operation.userId()
        );

        System.out.println(
                "Operation processed | session="
                        + sessionCode
                        + " | position="
                        + position
                        + " | delete="
                        + deleteCount
                        + " | text="
                        + operation.text()
        );
    }

    // =========================
    // BROADCAST CODE
    // =========================

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

    // =========================
    // USER COUNT
    // =========================

    private void broadcastUserCount(
            String sessionCode) {

        Set<WebSocketSession> users =
                rooms.get(sessionCode);

        if (users == null) {
            return;
        }

        int count = users.size();

        String message =
                "{\"type\":\"users\",\"count\":"
                        + count
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

    // =========================
    // DISCONNECT
    // =========================

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

                    ExecutorService executor =
                            roomExecutors.remove(sessionCode);

                    if (executor != null) {
                        executor.shutdown();
                    }

                    operationQueues.remove(
                            sessionCode
                    );

                } else {

                    broadcastUserCount(
                            sessionCode
                    );
                }
            }
        }

        System.out.println(
                "WebSocket disconnected: "
                        + session.getId()
        );
    }

    // =========================
    // HELPERS
    // =========================

    private String createCodeMessage(
            String code) {

        return "{\"type\":\"code\",\"code\":"
                + objectMapper.valueToTree(code)
                + "}";
    }

    private String defaultCode() {

        return """
                public class Main {
                    public static void main(String[] args) {
                        System.out.println("Hello Collaborative Editor");
                    }
                }
                """;
    }

    // =========================
    // OPERATION RECORD
    // =========================

    private record Operation(
            String userId,
            int position,
            int deleteCount,
            String text
    ) {}
}
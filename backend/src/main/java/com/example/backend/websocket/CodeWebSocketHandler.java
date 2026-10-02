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

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    // sessionId -> connected users
    private final Map<String, Set<WebSocketSession>> rooms =
            new ConcurrentHashMap<>();

    // websocketId -> sessionId
    private final Map<String, String> userRooms =
            new ConcurrentHashMap<>();

    // sessionId -> current version
    private final Map<String, Long> roomVersions =
            new ConcurrentHashMap<>();

    // sessionId -> operation queue
    private final Map<String, ConcurrentLinkedQueue<Operation>>
            operationQueues =
            new ConcurrentHashMap<>();

    // sessionId -> single executor
    private final Map<String, ExecutorService> roomExecutors =
            new ConcurrentHashMap<>();

    public CodeWebSocketHandler(
            CodeSessionRepository repository) {

        this.repository = repository;
    }

    @Override
    public void afterConnectionEstablished(
            WebSocketSession session) {

        System.out.println(
                "WebSocket connected: "
                        + session.getId()
        );
    }

    @Override
    protected void handleTextMessage(
            WebSocketSession session,
            TextMessage message) throws Exception {

        JsonNode data =
                objectMapper.readTree(
                        message.getPayload()
                );

        if (!data.has("type")) {
            return;
        }

        String type =
                data.get("type").asText();

        // =========================
        // JOIN SESSION
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

            CodeSession codeSession =
                    repository.findBySessionCode(
                            sessionCode
                    ).orElseGet(() ->
                            repository.save(
                                    new CodeSession(
                                            sessionCode,
                                            defaultCode()
                                    )
                            )
                    );

            // Initialize version
            roomVersions.putIfAbsent(
                    sessionCode,
                    0L
            );

            // Initialize operation queue
            operationQueues.computeIfAbsent(
                    sessionCode,
                    key -> new ConcurrentLinkedQueue<>()
            );

            // Initialize single room executor
            roomExecutors.computeIfAbsent(
                    sessionCode,
                    key -> Executors.newSingleThreadExecutor()
            );

            // Send current document
            session.sendMessage(
                    new TextMessage(
                            createCodeMessage(
                                    codeSession.getCode()
                            )
                    )
            );

            // Send current version
            session.sendMessage(
                    new TextMessage(
                            createVersionMessage(
                                    roomVersions.get(
                                            sessionCode
                                    )
                            )
                    )
            );

            broadcastUserCount(
                    sessionCode
            );

            System.out.println(
                    "User " + session.getId()
                            + " joined session "
                            + sessionCode
            );

            return;
        }

        // =========================
        // CODE OPERATION
        // =========================

        if (type.equals("operation")) {

            String sessionCode =
                    userRooms.get(
                            session.getId()
                    );

            if (sessionCode == null) {
                return;
            }

            int position =
                    data.get("position").asInt();

            int deleteCount =
                    data.get("deleteCount").asInt();

            String text =
                    data.has("text")
                            ? data.get("text").asText()
                            : "";

            long baseVersion =
                    data.has("baseVersion")
                            ? data.get("baseVersion").asLong()
                            : 0L;

            Operation operation =
                    new Operation(
                            session.getId(),
                            position,
                            deleteCount,
                            text,
                            baseVersion
                    );

            operationQueues
                    .get(sessionCode)
                    .offer(operation);

            ExecutorService executor =
                    roomExecutors.get(
                            sessionCode
                    );

            executor.submit(() ->
                    processOperation(
                            sessionCode
                    )
            );

            return;
        }

        // =========================
        // OLD FULL CODE SYNC
        // =========================

        if (type.equals("code")) {

            String sessionCode =
                    userRooms.get(
                            session.getId()
                    );

            if (sessionCode == null) {
                return;
            }

            if (!data.has("code")) {
                return;
            }

            String code =
                    data.get("code").asText();

            CodeSession codeSession =
                    repository.findBySessionCode(
                            sessionCode
                    ).orElseGet(() ->
                            repository.save(
                                    new CodeSession(
                                            sessionCode,
                                            code
                                    )
                            )
                    );

            codeSession.setCode(code);

            repository.save(codeSession);

            long newVersion =
                    roomVersions.merge(
                            sessionCode,
                            1L,
                            Long::sum
                    );

            broadcastCodeWithVersion(
                    sessionCode,
                    code,
                    newVersion,
                    session.getId()
            );

            return;
        }
    }

    // =========================
    // PROCESS OPERATION
    // =========================

    private void processOperation(
            String sessionCode) {

        ConcurrentLinkedQueue<Operation> queue =
                operationQueues.get(
                        sessionCode
                );

        if (queue == null) {
            return;
        }

        Operation operation;

        while ((operation = queue.poll()) != null) {

            try {

                CodeSession codeSession =
                        repository.findBySessionCode(
                                sessionCode
                        ).orElse(null);

                if (codeSession == null) {
                    continue;
                }

                String currentCode =
                        codeSession.getCode();

                int position =
                        operation.position();

                int deleteCount =
                        operation.deleteCount();

                String insertText =
                        operation.text();

                // Safety checks
                position =
                        Math.max(
                                0,
                                Math.min(
                                        position,
                                        currentCode.length()
                                )
                        );

                deleteCount =
                        Math.max(
                                0,
                                Math.min(
                                        deleteCount,
                                        currentCode.length()
                                                - position
                                )
                        );

                // Apply operation
                String newCode =
                        currentCode.substring(
                                0,
                                position
                        )
                        +
                        insertText
                        +
                        currentCode.substring(
                                position + deleteCount
                        );

                codeSession.setCode(
                        newCode
                );

                repository.save(
                        codeSession
                );

                // Increase version
                long newVersion =
                        roomVersions.merge(
                                sessionCode,
                                1L,
                                Long::sum
                        );

                broadcastOperation(
                        sessionCode,
                        operation,
                        newVersion
                );

            } catch (Exception e) {

                System.out.println(
                        "Operation processing error: "
                                + e.getMessage()
                );
            }
        }
    }

    // =========================
    // BROADCAST OPERATION
    // =========================

private void broadcastOperation(
        String sessionCode,
        Operation operation,
        long version) {

    Set<WebSocketSession> users =
            rooms.get(sessionCode);

    if (users == null) {
        return;
    }

    String message =
            "{"
                    + "\"type\":\"operation\","
                    + "\"position\":"
                    + operation.position()
                    + ","
                    + "\"deleteCount\":"
                    + operation.deleteCount()
                    + ","
                    + "\"text\":"
                    + objectMapper.valueToTree(
                            operation.text()
                    )
                    + ","
                    + "\"version\":"
                    + version
                    + ","
                    + "\"userId\":"
                    + objectMapper.valueToTree(
                            operation.userId()
                    )
                    + "}";

    for (WebSocketSession user : users) {

        try {

            if (user.isOpen()
                    && !user.getId().equals(
                            operation.userId()
                    )) {

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
    // FULL CODE BROADCAST
    // =========================

    private void broadcastCodeWithVersion(
            String sessionCode,
            String code,
            long version,
            String senderId) {

        Set<WebSocketSession> users =
                rooms.get(sessionCode);

        if (users == null) {
            return;
        }

        String message =
                "{"
                        + "\"type\":\"code\","
                        + "\"code\":"
                        + objectMapper
                        .valueToTree(code)
                        + ","
                        + "\"version\":"
                        + version
                        + "}";

        for (WebSocketSession user : users) {

            try {

                if (user.isOpen()
                        && !user.getId()
                        .equals(senderId)) {

                    user.sendMessage(
                            new TextMessage(
                                    message
                            )
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
                rooms.get(
                        sessionCode
                );

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
                            new TextMessage(
                                    message
                            )
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
    // VERSION MESSAGE
    // =========================

    private String createVersionMessage(
            long version) {

        return "{"
                + "\"type\":\"version\","
                + "\"version\":"
                + version
                + "}";
    }

    // =========================
    // CODE MESSAGE
    // =========================

    private String createCodeMessage(
            String code) {

        return "{"
                + "\"type\":\"code\","
                + "\"code\":"
                + objectMapper
                .valueToTree(code)
                + "}";
    }

    // =========================
    // DISCONNECT
    // =========================

    @Override
    public void afterConnectionClosed(
            WebSocketSession session,
            org.springframework.web.socket.CloseStatus status) {

        String sessionCode =
                userRooms.remove(
                        session.getId()
                );

        if (sessionCode != null) {

            Set<WebSocketSession> users =
                    rooms.get(
                            sessionCode
                    );

            if (users != null) {

                users.remove(session);

                if (users.isEmpty()) {

                    rooms.remove(
                            sessionCode
                    );

                    ExecutorService executor =
                            roomExecutors.remove(
                                    sessionCode
                            );

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
    // DEFAULT CODE
    // =========================

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
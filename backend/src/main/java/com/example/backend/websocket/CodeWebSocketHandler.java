package com.example.backend.websocket;

import com.example.backend.session.CodeSession;
import com.example.backend.session.CodeSessionRepository;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
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

    // Users in each room
    private final Map<String, Set<WebSocketSession>> rooms =
            new ConcurrentHashMap<>();

    // Which room each user belongs to
    private final Map<String, String> userRooms =
            new ConcurrentHashMap<>();

    // Operation queue for each room
    private final Map<String, ConcurrentLinkedQueue<Operation>> operationQueues =
            new ConcurrentHashMap<>();

    // One worker for each room
    private final Map<String, ExecutorService> roomExecutors =
            new ConcurrentHashMap<>();

    // Current version of every room
    private final Map<String, Long> roomVersions =
            new ConcurrentHashMap<>();

    // Applied operation history
    private final Map<String, List<AppliedOperation>> operationHistory =
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

        // ==========================================
        // JOIN
        // ==========================================

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

            roomVersions.computeIfAbsent(
                    sessionCode,
                    key -> 0L
            );

            operationHistory.computeIfAbsent(
                    sessionCode,
                    key -> new ArrayList<>()
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

            // Send current code + current version
            session.sendMessage(
                    new TextMessage(
                            createCodeMessage(
                                    codeSession.getCode(),
                                    roomVersions.get(sessionCode)
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

        // ==========================================
        // OPERATION
        // ==========================================

        if (type.equals("operation")) {

            String sessionCode =
                    userRooms.get(session.getId());

            if (sessionCode == null) {
                return;
            }

            long baseVersion =
                    data.has("baseVersion")
                            ? data.get("baseVersion").asLong()
                            : 0L;

            JsonNode changes =
                    data.get("changes");

            ConcurrentLinkedQueue<Operation> queue =
                    operationQueues.get(sessionCode);

            if (queue == null) {
                return;
            }

            for (JsonNode change : changes) {

                Operation operation =
                        new Operation(
                                session.getId(),
                                change.get("position").asInt(),
                                change.get("deleteCount").asInt(),
                                change.get("text").asText(),
                                baseVersion
                        );

                queue.add(operation);
            }

            processQueue(sessionCode);

            return;
        }
    }

    // ==========================================
    // PROCESS QUEUE
    // ==========================================

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

    // ==========================================
    // APPLY OPERATION
    // ==========================================

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

        // Transform against operations that happened
        // after this client's base version
        Operation transformed =
                transformOperation(
                        sessionCode,
                        operation
                );

        int position =
                transformed.position();

        if (position < 0 || position > code.length()) {
            return;
        }

        int deleteCount =
                transformed.deleteCount();

        int end =
                Math.min(
                        position + deleteCount,
                        code.length()
                );

        String newCode =
                code.substring(0, position)
                        + transformed.text()
                        + code.substring(end);

        codeSession.setCode(newCode);

        repository.save(codeSession);

        // Increment room version
        long newVersion =
                roomVersions.merge(
                        sessionCode,
                        1L,
                        Long::sum
                );

        // Save operation history
        operationHistory
                .computeIfAbsent(
                        sessionCode,
                        key -> new ArrayList<>()
                )
                .add(
                        new AppliedOperation(
                                transformed.userId(),
                                transformed.position(),
                                transformed.deleteCount(),
                                transformed.text(),
                                newVersion
                        )
                );

        // Send version acknowledgement to sender
        sendVersionAck(
                operation.userId(),
                newVersion
        );

        // Send updated code to other users
        broadcastOperation(
        sessionCode,
        transformed,
        operation.userId(),
        newVersion
);

        System.out.println(
                "Operation processed"
                        + " | session=" + sessionCode
                        + " | position=" + transformed.position()
                        + " | delete=" + transformed.deleteCount()
                        + " | text=" + transformed.text()
                        + " | version=" + newVersion
        );
    }

    // ==========================================
    // BASIC OT TRANSFORMATION
    // ==========================================

    private Operation transformOperation(
            String sessionCode,
            Operation operation) {

        List<AppliedOperation> history =
                operationHistory.get(sessionCode);

        if (history == null) {
            return operation;
        }

        int position =
                operation.position();

        for (AppliedOperation previous : history) {

            if (previous.version()
                    <= operation.baseVersion()) {

                continue;
            }

            int previousEnd =
                    previous.position()
                            + previous.deleteCount();

            int previousLength =
                    previous.text().length();

            int previousDelta =
                    previousLength
                            - previous.deleteCount();

            // Previous operation is before ours
            if (previousEnd <= position) {

                position += previousDelta;
            }

            // Both insert at same position
            else if (
                    previous.deleteCount() == 0
                            && previous.position() == position
            ) {

                // Deterministic ordering
                if (previous.userId()
                        .compareTo(operation.userId()) < 0) {

                    position += previousLength;
                }
            }
        }

        return new Operation(
                operation.userId(),
                position,
                operation.deleteCount(),
                operation.text(),
                operation.baseVersion()
        );
    }

    // ==========================================
    // BROADCAST CODE
    // ==========================================

   private void broadcastOperation(
        String sessionCode,
        Operation operation,
        String senderId,
        long version) {

    Set<WebSocketSession> users =
            rooms.get(sessionCode);

    if (users == null) {
        return;
    }

    String message =
            "{\"type\":\"operation\","
            + "\"position\":" + operation.position()
            + ",\"deleteCount\":" + operation.deleteCount()
            + ",\"text\":" + objectMapper.valueToTree(operation.text())
            + ",\"version\":" + version
            + "}";

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
                    "Operation broadcast error: "
                            + e.getMessage()
            );
        }
    }
}
    // ==========================================
    // VERSION ACK
    // ==========================================

    private void sendVersionAck(
            String userId,
            long version) {

        for (Set<WebSocketSession> users : rooms.values()) {

            for (WebSocketSession user : users) {

                if (user.getId().equals(userId)
                        && user.isOpen()) {

                    try {

                        user.sendMessage(
                                new TextMessage(
                                        "{\"type\":\"ack\",\"version\":"
                                                + version
                                                + "}"
                                )
                        );

                    } catch (Exception e) {

                        System.out.println(
                                "ACK error: "
                                        + e.getMessage()
                        );
                    }

                    return;
                }
            }
        }
    }

    // ==========================================
    // USER COUNT
    // ==========================================

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

    // ==========================================
    // DISCONNECT
    // ==========================================

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

                    roomVersions.remove(
                            sessionCode
                    );

                    operationHistory.remove(
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

    // ==========================================
    // JSON CODE MESSAGE
    // ==========================================

    private String createCodeMessage(
            String code,
            long version) {

        return "{\"type\":\"code\",\"code\":"
                + objectMapper.valueToTree(code)
                + ",\"version\":"
                + version
                + "}";
    }

    // ==========================================
    // DEFAULT CODE
    // ==========================================

    private String defaultCode() {

        return """
                public class Main {
                    public static void main(String[] args) {
                        System.out.println("Hello Collaborative Editor");
                    }
                }
                """;
    }

    // ==========================================
    // OPERATION RECORD
    // ==========================================

    private record Operation(
            String userId,
            int position,
            int deleteCount,
            String text,
            long baseVersion
    ) {}

    private record AppliedOperation(
            String userId,
            int position,
            int deleteCount,
            String text,
            long version
    ) {}
}
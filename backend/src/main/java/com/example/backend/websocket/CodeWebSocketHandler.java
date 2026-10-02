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
public class CodeWebSocketHandler
        extends TextWebSocketHandler {

    private final CodeSessionRepository repository;

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    // sessionId -> connected users
    private final Map<String, Set<WebSocketSession>> rooms =
            new ConcurrentHashMap<>();

    // websocketId -> sessionId
    private final Map<String, String> userRooms =
            new ConcurrentHashMap<>();

    // sessionId -> current document version
    private final Map<String, Long> roomVersions =
            new ConcurrentHashMap<>();

    // sessionId -> pending operations
    private final Map<String, ConcurrentLinkedQueue<Operation>>
            operationQueues =
            new ConcurrentHashMap<>();

    // sessionId -> single executor
    private final Map<String, ExecutorService> roomExecutors =
            new ConcurrentHashMap<>();

    // sessionId -> applied operation history
    //
    // history[0] = operation that changed
    // version 0 -> version 1
    //
    // history[1] = operation that changed
    // version 1 -> version 2
    private final Map<String, List<Operation>>
            operationHistory =
            new ConcurrentHashMap<>();

    public CodeWebSocketHandler(
            CodeSessionRepository repository) {

        this.repository = repository;
    }

    // ==========================================
    // CONNECTION
    // ==========================================

    @Override
    public void afterConnectionEstablished(
            WebSocketSession session) {

        System.out.println(
                "WebSocket connected: "
                        + session.getId()
        );
    }

    // ==========================================
    // MESSAGE HANDLER
    // ==========================================

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

        // ==========================================
        // JOIN SESSION
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
                    key ->
                            ConcurrentHashMap.newKeySet()
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
                    key ->
                            new ConcurrentLinkedQueue<>()
            );

            // Initialize operation history
            operationHistory.computeIfAbsent(
                    sessionCode,
                    key ->
                            new ArrayList<>()
            );

            // Initialize single executor
            roomExecutors.computeIfAbsent(
                    sessionCode,
                    key ->
                            Executors.newSingleThreadExecutor()
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

        // ==========================================
        // OPERATION
        // ==========================================

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

            ConcurrentLinkedQueue<Operation>
                    queue =
                    operationQueues.get(
                            sessionCode
                    );

            if (queue == null) {
                return;
            }

            queue.offer(operation);

            ExecutorService executor =
                    roomExecutors.get(
                            sessionCode
                    );

            if (executor != null) {

                executor.submit(() ->
                        processOperation(
                                sessionCode
                        )
                );
            }

            return;
        }

        // ==========================================
        // OLD FULL CODE SYNC
        // ==========================================

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

            repository.save(
                    codeSession
            );

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

    // ==========================================
    // PROCESS OPERATION
    // ==========================================

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

                long currentVersion =
                        roomVersions.getOrDefault(
                                sessionCode,
                                0L
                        );

                // ==================================
                // GET HISTORY
                // ==================================

                List<Operation> history =
                        operationHistory.computeIfAbsent(
                                sessionCode,
                                key ->
                                        new ArrayList<>()
                        );

                // ==================================
                // TRANSFORM OPERATION
                // ==================================

                Operation transformedOperation =
                        operation;

                /*
                 * Example:
                 *
                 * Client created operation at:
                 *
                 * baseVersion = 2
                 *
                 * Server is currently at:
                 *
                 * version = 4
                 *
                 * Therefore operations:
                 *
                 * history[2]
                 * history[3]
                 *
                 * happened after client's version.
                 *
                 * Transform incoming operation
                 * against both.
                 */

                synchronized (history) {

                    long baseVersion =
                            operation.baseVersion();

                    // Invalid future version
                    if (baseVersion >
                            currentVersion) {

                        System.out.println(
                                "Invalid base version: "
                                        + baseVersion
                                        + " current="
                                        + currentVersion
                        );

                        continue;
                    }

                    int startIndex =
                            (int) Math.max(
                                    0,
                                    baseVersion
                            );

                    int endIndex =
                            Math.min(
                                    (int) currentVersion,
                                    history.size()
                            );

                    for (
                            int i = startIndex;
                            i < endIndex;
                            i++
                    ) {

                        Operation applied =
                                history.get(i);

                        transformedOperation =
                                OperationTransformer.transform(
                                        transformedOperation,
                                        applied
                                );
                    }
                }

                // ==================================
                // CURRENT CODE
                // ==================================

                String currentCode =
                        codeSession.getCode();

                if (currentCode == null) {
                    currentCode = "";
                }

                // ==================================
                // SAFE POSITION
                // ==================================

                int position =
                        Math.max(
                                0,
                                Math.min(
                                        transformedOperation
                                                .position(),
                                        currentCode.length()
                                )
                        );

                // ==================================
                // SAFE DELETE COUNT
                // ==================================

                int deleteCount =
                        Math.max(
                                0,
                                Math.min(
                                        transformedOperation
                                                .deleteCount(),
                                        currentCode.length()
                                                - position
                                )
                        );

                // ==================================
                // INSERT TEXT
                // ==================================

                String insertText =
                        transformedOperation.text() == null
                                ? ""
                                : transformedOperation.text();

                // ==================================
                // APPLY OPERATION
                // ==================================

                String newCode =
                        currentCode.substring(
                                0,
                                position
                        )
                        + insertText
                        + currentCode.substring(
                                position + deleteCount
                        );

                codeSession.setCode(
                        newCode
                );

                repository.save(
                        codeSession
                );

                // ==================================
                // NEW VERSION
                // ==================================

                long newVersion =
                        roomVersions.merge(
                                sessionCode,
                                1L,
                                Long::sum
                        );

                // ==================================
                // CREATE APPLIED OPERATION
                // ==================================

                Operation appliedOperation =
                        new Operation(
                                transformedOperation.userId(),
                                position,
                                deleteCount,
                                insertText,
                                currentVersion
                        );

                // ==================================
                // SAVE TO HISTORY
                // ==================================

                synchronized (history) {

                    history.add(
                            appliedOperation
                    );
                }

                // ==================================
                // BROADCAST
                // ==================================

                broadcastOperation(
                        sessionCode,
                        appliedOperation,
                        newVersion
                );

                System.out.println(
                        "Operation applied."
                                + " Version = "
                                + newVersion
                                + " Position = "
                                + position
                                + " Delete = "
                                + deleteCount
                                + " Text = ["
                                + insertText
                                + "]"
                );

            } catch (Exception e) {

                System.out.println(
                        "Operation processing error: "
                                + e.getMessage()
                );

                e.printStackTrace();
            }
        }
    }

    // ==========================================
    // BROADCAST OPERATION
    // ==========================================

    private void broadcastOperation(
            String sessionCode,
            Operation operation,
            long version) {

        Set<WebSocketSession> users =
                rooms.get(
                        sessionCode
                );

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

                /*
                 * IMPORTANT:
                 *
                 * Sender ko uski own operation
                 * dobara nahi bhejni.
                 */

                if (user.isOpen()
                        && !user.getId().equals(
                                operation.userId()
                        )) {

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

    // ==========================================
    // FULL CODE BROADCAST
    // ==========================================

    private void broadcastCodeWithVersion(
            String sessionCode,
            String code,
            long version,
            String senderId) {

        Set<WebSocketSession> users =
                rooms.get(
                        sessionCode
                );

        if (users == null) {
            return;
        }

        String message =
                "{"
                        + "\"type\":\"code\","
                        + "\"code\":"
                        + objectMapper.valueToTree(
                                code
                        )
                        + ","
                        + "\"version\":"
                        + version
                        + "}";

        for (WebSocketSession user : users) {

            try {

                if (user.isOpen()
                        && !user.getId().equals(
                                senderId
                        )) {

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

    // ==========================================
    // USER COUNT
    // ==========================================

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

    // ==========================================
    // VERSION MESSAGE
    // ==========================================

    private String createVersionMessage(
            long version) {

        return "{"
                + "\"type\":\"version\","
                + "\"version\":"
                + version
                + "}";
    }

    // ==========================================
    // CODE MESSAGE
    // ==========================================

    private String createCodeMessage(
            String code) {

        return "{"
                + "\"type\":\"code\","
                + "\"code\":"
                + objectMapper.valueToTree(
                        code
                )
                + "}";
    }

    // ==========================================
    // DISCONNECT
    // ==========================================

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

                    operationHistory.remove(
                            sessionCode
                    );

                    roomVersions.remove(
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
}
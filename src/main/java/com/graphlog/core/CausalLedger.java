package com.graphlog.core;

import com.graphlog.interfaces.StateUpdater;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CausalLedger {

    private final Map<String, EventAtom> eventStoreById = new ConcurrentHashMap<>();
    private final Map<String, Integer> eventIdToGraphId = new ConcurrentHashMap<>();
    private final Map<Integer, String> graphIdToEventId = new ConcurrentHashMap<>();
    private final Map<Integer, List<Integer>> childrenAdjacencyList = new ConcurrentHashMap<>();
    private final Graph causalGraph;

    private final Map<String, StateUpdater<Map<String, Object>>> stateUpdater = new ConcurrentHashMap<>();

    private final String logFilePath;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private long totalEventsIngested = 0;
    private long totalCycleChecks = 0;
    private long totalCyclesPrevented = 0;

    public static class CausalLoopException extends Exception {
        public CausalLoopException(String message) {
            super(message);
        }
    }

    public static class UnknownParentException extends Exception {
        public UnknownParentException(String message) {
            super(message);
        }
    }

    public static class PersistenceException extends RuntimeException {
        public PersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public CausalLedger(String logFilePath, int initialGraphCapacity) {
        this.logFilePath = logFilePath;
        this.causalGraph = new Graph(initialGraphCapacity);

        Path logPath = Paths.get(logFilePath);
        Path parentDir = logPath.getParent();
        if (parentDir != null) {
            try {
                Files.createDirectories(parentDir);
            } catch (IOException e) {
                throw new PersistenceException("Failed to create log directory: " + parentDir, e);
            }
        }

        registerDefaultStateUpdaters();

        loadEventsFromLog();

        System.out.println("CausalLedger initialized: " + getStats());
    }

    public CausalLedger(String logFilePath) {
        this(logFilePath, 1000);
    }

    private void registerDefaultStateUpdaters() {
        stateUpdater.put("USER_CREATED", ((currentState, eventPayload, eventType) -> {
            Map<String, Object> newState = new HashMap<>();
            newState.put("userId", eventPayload.get("userId"));
            newState.put("username", eventPayload.get("username"));
            newState.put("isActive", true);
            newState.put("version", 1);
            newState.put("createdAt", eventPayload.get("timestamp"));
            System.out.println("Applying USER_CREATED: " + eventPayload + " -> " + newState);
            return newState;
        }));

        stateUpdater.put("USER_RENAMED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("username", payload.get("newUsername"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastModified", payload.get("timestamp"));
            System.out.println("Applying USER_RENAMED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("USER_DEACTIVATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("isActive", false);
            newState.put("deactivationReason", payload.get("reason"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("deactivatedAt", payload.get("timestamp"));
            System.out.println("Applying USER_DEACTIVATED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("USER_REACTIVATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("isActive", true);
            newState.remove("deactivationReason");
            newState.remove("deactivatedAt");
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("reactivatedAt", payload.get("timestamp"));
            System.out.println("Applying USER_REACTIVATED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("PRODUCT_ADDED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>();
            newState.put("productId", payload.get("productId"));
            newState.put("productName", payload.get("productName"));
            newState.put("price", payload.get("price"));
            newState.put("stock", payload.getOrDefault("stock", 0));
            newState.put("version", 1);
            newState.put("createdAt", payload.get("timestamp"));
            System.out.println("Applying PRODUCT_ADDED: " + payload + " -> " + newState);
            return newState;
        });

        stateUpdater.put("PRODUCT_UPDATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            if (payload.containsKey("productName")) {
                newState.put("productName", payload.get("productName"));
            }
            if (payload.containsKey("price")) {
                newState.put("price", payload.get("price"));
            }
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastModified", payload.get("timestamp"));
            System.out.println("Applying PRODUCT_UPDATED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("STOCK_INCREMENTED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            int currentStock = (Integer) currentState.getOrDefault("stock", 0);
            int amountIncremented = (Integer) payload.getOrDefault("amount", 0);
            newState.put("stock", currentStock + amountIncremented);
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastStockUpdate", payload.get("timestamp"));
            System.out.println("Applying STOCK_INCREMENTED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("STOCK_DECREMENTED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            int currentStock = (Integer) currentState.getOrDefault("stock", 0);
            int amountDecremented = (Integer) payload.getOrDefault("amount", 0);
            newState.put("stock", Math.max(0, currentStock - amountDecremented)); // Prevent negative stock
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastStockUpdate", payload.get("timestamp"));
            System.out.println("Applying STOCK_DECREMENTED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        // Order Service event handlers
        stateUpdater.put("ORDER_CREATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>();
            newState.put("orderId", payload.get("orderId"));
            newState.put("userId", payload.get("userId"));
            newState.put("status", "CREATED");
            newState.put("items", payload.get("items"));
            newState.put("totalAmount", payload.get("totalAmount"));
            newState.put("version", 1);
            newState.put("createdAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_CREATED: " + payload + " -> " + newState);
            return newState;
        });

        stateUpdater.put("ORDER_CONFIRMED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("status", "CONFIRMED");
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("confirmedAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_CONFIRMED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("ORDER_SHIPPED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("status", "SHIPPED");
            newState.put("trackingNumber", payload.get("trackingNumber"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("shippedAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_SHIPPED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdater.put("ORDER_CANCELLED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("status", "CANCELLED");
            newState.put("cancellationReason", payload.get("reason"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("cancelledAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_CANCELLED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });
    }

    public void registerStateUpdater(String eventType, StateUpdater<Map<String, Object>> updater) {
        stateUpdater.put(eventType, updater);
        System.out.println("Registered state updater for event type: " + eventType);
    }

    public Map<String, Object> getCurrentStateForEntity(String entityId) {
        rwLock.readLock().lock();
        try {
            List<String> allEventIdsInTopoOrder = getEventsInTopologicalOrder();
            if (allEventIdsInTopoOrder.isEmpty()) {
                System.err.println("No events in ledger to project state for entity: " + entityId);
                return Collections.emptyMap();
            }

            List<EventAtom> entityEventsInCausalOrder = new ArrayList<>();
            for (String eventId : allEventIdsInTopoOrder) {
                EventAtom event = eventStoreById.get(eventId);
                if (event != null && entityId.equals(event.getEntityId())) {
                    entityEventsInCausalOrder.add(event);
                }
            }

            if (entityEventsInCausalOrder.isEmpty()) {
                System.err.println("No events found for entity: " + entityId);
                return Collections.emptyMap();
            }

            System.out.println("\nProjecting state for entity: " + entityId +
                    " using " + entityEventsInCausalOrder.size() + " events in causal order.");

            Map<String, Object> currentState = new HashMap<>();

            for (EventAtom event : entityEventsInCausalOrder) {
                System.out.println("  Replaying event: " + event.getEventType() + " (ID: " + event.getEventId() + ")");

                StateUpdater<Map<String, Object>> updater = stateUpdater.get(event.getEventType());
                if (updater != null) {
                    try {
                        currentState = updater.apply(currentState, event.getPayload(), event.getEventType());
                    } catch (Exception e) {
                        System.err.println("    Error applying state updater for event type: " +
                                event.getEventType() + " - " + e.getMessage());
                    }
                } else {
                    System.err.println("    No state updater registered for event type: " +
                            event.getEventType() + " - Skipping event.");
                }
            }

            System.out.println("Final projected state for entity " + entityId + ": " + currentState);
            return Collections.unmodifiableMap(currentState);

        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Map<String, Object> getEntityStateUpToEvent(String entityId, String upToEventId) {
        rwLock.readLock().lock();
        try {
            List<String> allEventIdsInTopoOrder = getEventsInTopologicalOrder();
            if (allEventIdsInTopoOrder.isEmpty()) {
                return Collections.emptyMap();
            }

            int stopIndex = allEventIdsInTopoOrder.indexOf(upToEventId);
            if (stopIndex == -1) {
                System.err.println("Target event not found: " + upToEventId);
                return Collections.emptyMap();
            }

            List<EventAtom> entityEventsInCausalOrder = new ArrayList<>();
            for (int i = 0; i <= stopIndex; i++) {
                String eventId = allEventIdsInTopoOrder.get(i);
                EventAtom event = eventStoreById.get(eventId);
                if (event != null && entityId.equals(event.getEntityId())) {
                    entityEventsInCausalOrder.add(event);
                }
            }

            if (entityEventsInCausalOrder.isEmpty()) {
                return Collections.emptyMap();
            }

            System.out.println("\nProjecting historical state for entity: " + entityId +
                    " up to event: " + upToEventId);

            Map<String, Object> currentState = new HashMap<>();

            for (EventAtom event : entityEventsInCausalOrder) {
                StateUpdater<Map<String, Object>> updater = stateUpdater.get(event.getEventType());
                if (updater != null) {
                    try {
                        currentState = updater.apply(currentState, event.getPayload(), event.getEventType());
                    } catch (Exception e) {
                        System.err.println("Error applying state updater for event type: " +
                                event.getEventType() + " - " + e.getMessage());
                    }
                }
            }

            return Collections.unmodifiableMap(currentState);

        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String ingestEvent(String entityId, String eventType,
                              Map<String, Object> payload,
                              List<String> causalParentEventIds)
            throws CausalLoopException, UnknownParentException {

        rwLock.writeLock().lock();
        try {
            for (String parentId : causalParentEventIds) {
                if (!eventStoreById.containsKey(parentId)) {
                    throw new UnknownParentException("Parent event " + parentId + " not found in ledger");
                }
            }

            totalCycleChecks++;

            Graph tempGraph = new Graph(causalGraph);
            int tempNodeId = tempGraph.addNode();

            for (String parentId : causalParentEventIds) {
                int parentGraphId = eventIdToGraphId.get(parentId);
                tempGraph.addDirectedEdge(tempNodeId, parentGraphId);
            }

            if (tempGraph.hasCycle()) {
                totalCyclesPrevented++;
                throw new CausalLoopException(
                        String.format("Ingesting event of type '%s' for entity '%s' would create a causal loop. " +
                                        "This violates the fundamental principle of causality. Parent events: %s",
                                eventType, entityId, causalParentEventIds));
            }

            int newGraphNodeId = causalGraph.addNode();

            for (String parentId : causalParentEventIds) {
                int parentGraphId = eventIdToGraphId.get(parentId);
                causalGraph.addDirectedEdge(newGraphNodeId, parentGraphId);

                this.childrenAdjacencyList
                        .computeIfAbsent(parentGraphId, k -> new ArrayList<>())
                        .add(newGraphNodeId);
            }

            EventAtom newEvent = new EventAtom(entityId, eventType, payload, causalParentEventIds);
            String eventId = newEvent.getEventId();

            eventStoreById.put(eventId, newEvent);
            eventIdToGraphId.put(eventId, newGraphNodeId);
            graphIdToEventId.put(newGraphNodeId, eventId);

            appendEventToLog(newEvent);

            totalEventsIngested++;

            return eventId;

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void appendEventToLog(EventAtom event) {
        try {
            String jsonLine = event.toLogString() + System.lineSeparator();
            Files.write(Paths.get(logFilePath),
                    jsonLine.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new PersistenceException("Failed to append event to log: " + event.getEventId(), e);
        }
    }

    private void loadEventsFromLog() {
        Path logPath = Paths.get(logFilePath);

        if (!Files.exists(logPath)) {
            System.out.println("Log file does not exist, starting with empty ledger: " + logFilePath);
            return;
        }

        System.out.println("Loading events from log: " + logFilePath);

        try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                if (line.isEmpty()) {
                    continue;
                }

                try {
                    EventAtom event = EventAtom.fromLogString(line);

                    int graphNodeId = causalGraph.addNode();

                    eventStoreById.put(event.getEventId(), event);
                    eventIdToGraphId.put(event.getEventId(), graphNodeId);
                    graphIdToEventId.put(graphNodeId, event.getEventId());

                    for (String parentId : event.getCausalParentEventIds()) {
                        Integer parentGraphId = eventIdToGraphId.get(parentId);
                        if (parentGraphId == null) {
                            throw new PersistenceException(
                                    "Parent event " + parentId + " not found while loading event " + event.getEventId() +
                                            ". This suggests log corruption or events were not written in causal order.", null);
                        }
                        causalGraph.addDirectedEdge(graphNodeId, parentGraphId);

                        this.childrenAdjacencyList
                                .computeIfAbsent(parentGraphId, k -> new ArrayList<>())
                                .add(graphNodeId); // parentGraphId (Cause) now has loadedEvent (graphNodeId, Effect) as a child
                    }

                } catch (Exception e) {
                    throw new PersistenceException(
                            "Failed to load event from line " + lineNumber + " in log file: " + line, e);
                }
            }

            System.out.println("Successfully loaded " + eventStoreById.size() + " events from log");

        } catch (IOException e) {
            throw new PersistenceException("Failed to read log file: " + logFilePath, e);
        }
    }

    private static List<Integer> bfsShortestPath(int startNode, int endNode, int numTotalGraphNodes, Map<Integer, List<Integer>> adjacencyList) {
        if (startNode < 0 || startNode >= numTotalGraphNodes || endNode < 0 || endNode >= numTotalGraphNodes) {
            System.err.println("Error in bfsShortestPath: Start or end node out of bounds");
            return Collections.emptyList();
        }

        if (startNode == endNode) {
            return List.of(startNode);
        }

        Queue<Integer> queue = new LinkedList<>();
        Map<Integer, Integer> prev = new HashMap<>();
        boolean[] visited = new boolean[numTotalGraphNodes];

        queue.offer(startNode);
        visited[startNode] = true;

        while (!queue.isEmpty()) {
            int u = queue.poll();

            List<Integer> neighbours = adjacencyList.getOrDefault(u, Collections.emptyList());

            for (int v : neighbours) {
                if (!visited[v]) {
                    visited[v] = true;
                    prev.put(v, u); // Record that u is the predecessor of v in the path from startNode
                    queue.offer(v);

                    if (v == endNode) {
                        LinkedList<Integer> path = new LinkedList<>();
                        Integer current = endNode;
                        while (current != null) {
                            path.addFirst(current);
                            if (current.equals(startNode)) break;
                            current = prev.get(current);
                            if (current == null && !path.contains(startNode) && !path.getFirst().equals(startNode)) {
                                System.err.println("Error reconstructing path: predecessor not found before reaching start node.");
                                return Collections.emptyList();
                            }
                        }
                        return path;
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    public List<String> getShortestCausalPath(String startEventId, String endEventId) {
        rwLock.readLock().lock();
        try {
            Integer startGraphId = eventIdToGraphId.get(startEventId);
            Integer endGraphId = eventIdToGraphId.get(endEventId);

            if (startGraphId == null || endGraphId == null) {
                System.err.println("Error in getShortestCausalPath: Start or end event id not found");
                return Collections.emptyList();
            }

            List<Integer> shortestPathGraphIds = bfsShortestPath(
                    startGraphId,
                    endGraphId,
                    causalGraph.getNumVertices(),
                    this.childrenAdjacencyList
            );

            if (shortestPathGraphIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> resultEventIds = new ArrayList<>(shortestPathGraphIds.size());
            for (int graphId : shortestPathGraphIds) {
                String pathEventId = graphIdToEventId.get(graphId);
                if (pathEventId != null) {
                    resultEventIds.add(pathEventId);
                } else {
                    System.err.println("Error in getShortestCausalPath: Graph Id to event id mapping inconsistent for " + graphId);
                }
            }
            return resultEventIds;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getAllCommonCausalAncestors(String eventId1, String eventId2) {
        rwLock.readLock().lock();
        try {
            Integer graphId1 = eventIdToGraphId.get(eventId1);
            Integer graphId2 = eventIdToGraphId.get(eventId2);

            if (graphId1 == null || graphId2 == null) {
                if (graphId1 == null) System.err.println("Event ID not found for common ancestor query: " + eventId1);
                if (graphId2 == null) System.err.println("Event ID not found for common ancestor query: " + eventId2);
                return Collections.emptyList();
            }

            Set<Integer> ancestorGraphIds1 = causalGraph.getReachableVertices(graphId1);
            Set<Integer> ancestorGraphIds2 = causalGraph.getReachableVertices(graphId2);

            Set<Integer> commonAncestorsGraphIds = new HashSet<>(ancestorGraphIds1);
            commonAncestorsGraphIds.retainAll(ancestorGraphIds2);
            if (commonAncestorsGraphIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> commonAncestorsEventIds = new ArrayList<>(commonAncestorsGraphIds.size());
            for (Integer commonGraphId : commonAncestorsGraphIds) {
                String commonEventId = graphIdToEventId.get(commonGraphId);
                if (commonEventId != null) {
                    commonAncestorsEventIds.add(commonEventId);
                } else {
                    System.err.println("Error in getAllCommonCausalAncestors: graph id to event id mapping inconsistent for " + commonGraphId);
                }
            }
            return commonAncestorsEventIds;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getNearestCommonCausalAncestors(String eventId1, String eventId2) {
        rwLock.readLock().lock();
        try {
            Integer graphId1 = eventIdToGraphId.get(eventId1);
            Integer graphId2 = eventIdToGraphId.get(eventId2);

            if (graphId1 == null || graphId2 == null) {
                if (graphId1 == null) System.err.println("Event ID not found for nearest common ancestor query: " + eventId1);
                if (graphId2 == null) System.err.println("Event ID not found for nearest common ancestor query: " + eventId2);
                return Collections.emptyList();
            }

            Set<Integer> ancestors1 = causalGraph.getReachableVertices(graphId1);
            Set<Integer> ancestors2 = causalGraph.getReachableVertices(graphId2);

            Set<Integer> commonAncestorsGraphIds = new HashSet<>(ancestors1);
            commonAncestorsGraphIds.retainAll(ancestors2);

            if (commonAncestorsGraphIds.isEmpty()) {
                return Collections.emptyList();
            }

            if (commonAncestorsGraphIds.size() == 1) {
                String singleCommonAncestorEventId = graphIdToEventId.get(commonAncestorsGraphIds.iterator().next());
                return (singleCommonAncestorEventId != null) ? List.of(singleCommonAncestorEventId) : Collections.emptyList();
            }

            // A common ancestor 'A' is "nearest" if no other common ancestor 'B'
            // is a descendant of 'A' (in our Effect->Cause graph, means A is an ancestor of B).
            Set<Integer> nearestCommonAncestorsGraphIds = new HashSet<>();
            for (Integer candidateAncestor : commonAncestorsGraphIds) {
                boolean isNearest = true;
                // Check if any other common ancestor is a "more direct" cause of our events
                // than 'candidateAncestor'. This means, is any other common ancestor a
                // descendant of 'candidateAncestor' in the Effect->Cause graph
                // (i.e., 'candidateAncestor' is an ancestor of that other common ancestor).
                for (Integer otherCommonAncestor : commonAncestorsGraphIds) {
                    if (candidateAncestor.equals(otherCommonAncestor)) {
                        continue; // Don't compare with itself
                    }
                    // Is 'candidateAncestor' an ancestor of 'otherCommonAncestor'?
                    // To check this, get all ancestors of 'otherCommonAncestor'.
                    // If 'candidateAncestor' is in that set, then 'otherCommonAncestor' is "below"
                    // 'candidateAncestor' in the causal chain towards the original E1/E2, making 'candidateAncestor' not nearest.
                    if (causalGraph.getReachableVertices(otherCommonAncestor).contains(candidateAncestor)) {
                        isNearest = false;
                        break;
                    }
                }
                if (isNearest) {
                    nearestCommonAncestorsGraphIds.add(candidateAncestor);
                }
            }

            List<String> resultEventIds = new ArrayList<>(nearestCommonAncestorsGraphIds.size());
            for (int graphId : nearestCommonAncestorsGraphIds) {
                String nearestEventId = graphIdToEventId.get(graphId);
                if (nearestEventId != null) {
                    resultEventIds.add(nearestEventId);
                } else {
                    System.err.println("Error in getNearestCommonCausalAncestors: Mapping inconsistent for " + graphId);
                }
            }
            return resultEventIds;

        } finally {
            rwLock.readLock().unlock();
        }
    }

    public EventAtom getEvent(String eventId) {
        rwLock.readLock().lock();
        try {
            return eventStoreById.get(eventId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getEventAndCausalAncestryIds(String eventId) {
        rwLock.readLock().lock();
        try {
            if (!eventIdToGraphId.containsKey(eventId)) {
                return Collections.emptyList();
            }

            int startNodeId = eventIdToGraphId.get(eventId);
            Set<Integer> reachableGraphIds = causalGraph.getReachableVertices(startNodeId);

            List<String> result = new ArrayList<>();
            for (int graphId : reachableGraphIds) {
                String resultEventId = graphIdToEventId.get(graphId);
                if (resultEventId != null) {
                    result.add(resultEventId);
                }
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getEventAndCausalDescendantsId(String eventId) {
        rwLock.readLock().lock();
        try {
            Integer startNodeGraphId = eventIdToGraphId.get(eventId);
            if (startNodeGraphId == null) {
                // Event ID not found, return empty list
                System.err.println("Event ID not found for descendant query: " + eventId);
                return Collections.emptyList();
            }

            Set<Integer> reachableDescendantsGraphIds = new HashSet<>();
            Deque<Integer> stack = new ArrayDeque<>();

            stack.push(startNodeGraphId);

            while (!stack.isEmpty()) {
                int currentGraphId = stack.pop();

                reachableDescendantsGraphIds.add(currentGraphId);

                List<Integer> children = this.childrenAdjacencyList.getOrDefault(currentGraphId, Collections.emptyList());
                for (int childGraphId : children) {
                    if (!reachableDescendantsGraphIds.contains(childGraphId)) {
                        stack.push(childGraphId);
                    }
                }
            }

            // Convert Set<Integer> of graph IDs back to List<String> of event IDs
            List<String> resultEventIds = new ArrayList<>(reachableDescendantsGraphIds.size());
            for (int graphId : reachableDescendantsGraphIds) {
                String descEventId = graphIdToEventId.get(graphId);
                if (descEventId != null) {
                    resultEventIds.add(descEventId);
                }
            }
            return resultEventIds;

        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<String> getEventsInTopologicalOrder() {
        rwLock.readLock().lock();
        try {
            List<Integer> sortedGraphIds = causalGraph.topologicalSort();

            // Reverse to get cause->effect order (since our edges are effect->cause)
            Collections.reverse(sortedGraphIds);

            List<String> result = new ArrayList<>();
            for (int graphId : sortedGraphIds) {
                String eventId = graphIdToEventId.get(graphId);
                if (eventId != null) {
                    result.add(eventId);
                }
            }

            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean containsEvent(String eventId) {
        rwLock.readLock().lock();
        try {
            return eventStoreById.containsKey(eventId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getEventCount() {
        rwLock.readLock().lock();
        try {
            return eventStoreById.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String getStats() {
        rwLock.readLock().lock();
        try {
            return String.format(
                    "CausalLedger[events=%d, ingested=%d, cycleChecks=%d, cyclesPrevented=%d, graph=%s, logFile='%s']",
                    eventStoreById.size(), totalEventsIngested, totalCycleChecks, totalCyclesPrevented,
                    causalGraph.getGraphStats(), logFilePath);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<EventAtom> getEventsByEntity(String entityId) {
        rwLock.readLock().lock();
        try {
            return eventStoreById.values().stream()
                    .filter(event -> entityId.equals(event.getEntityId()))
                    .sorted(Comparator.comparing(EventAtom::getTimestamp))
                    .toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<EventAtom> getEventsByType(String eventType) {
        rwLock.readLock().lock();
        try {
            return eventStoreById.values().stream()
                    .filter(event -> eventType.equals(event.getEventType()))
                    .sorted(Comparator.comparing(EventAtom::getTimestamp))
                    .toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Integer getGraphIdForEventId(String eventId) {
        rwLock.readLock().lock();
        try {
            return eventIdToGraphId.get(eventId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String getEventIdForGraphId(int graphId) {
        rwLock.readLock().lock();
        try {
            return graphIdToEventId.get(graphId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<Integer> getChildrenGraphIds(int parentGraphId) {
        rwLock.readLock().lock();
        try {
            // Return a copy to prevent modification of the internal list
            return new ArrayList<>(childrenAdjacencyList.getOrDefault(parentGraphId, Collections.emptyList()));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        return getStats();
    }
}

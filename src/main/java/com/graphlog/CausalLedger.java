package com.graphlog;

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
    private final Graph causalGraph;

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

        loadEventsFromLog();

        System.out.println("CausalLedger initialized: " + getStats());
    }

    public CausalLedger(String logFilePath) {
        this(logFilePath, 1000);
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

    @Override
    public String toString() {
        return getStats();
    }
}

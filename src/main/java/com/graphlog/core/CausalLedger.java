package com.graphlog.core;

import com.graphlog.interfaces.StateUpdater;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CausalLedger {

    private static final String ENTITY_INDEX_FILENAME = "entity_to_event_ids.idx";
    private static final String CHILDREN_ADJ_FILENAME = "children_adjacency.idx";
    private static final String EVENT_TO_GRAPH_ID_FILENAME = "event_to_graph_id.idx";
    private static final String GRAPH_TO_EVENT_ID_FILENAME = "graph_to_event_id.idx";
    private static final String EVENT_TYPE_INDEX_FILENAME = "event_type_to_event_ids.idx";
    private static final String TRACE_ID_INDEX_FILENAME = "trace_id_to_event_ids.idx";

    private final Map<String, List<String>> eventTypeToEventIdsIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> eventIdToGraphId = new ConcurrentHashMap<>();
    private final Map<Integer, String> graphIdToEventId = new ConcurrentHashMap<>();
    private final Map<Integer, List<Integer>> childrenAdjacencyList = new ConcurrentHashMap<>();
    private final Map<String, List<String>> traceIdToEventIdsIndex = new ConcurrentHashMap<>();
    private final Map<String, String> latestEventIdByTraceId = new ConcurrentHashMap<>();

    private final Graph causalGraph;
    private final Map<String, List<String>> entityToEventIdsIndex  = new ConcurrentHashMap<>();
    private final VectorClockManager vcManager;
    private final String localNodeId = "GRAPH_NODE_01";

    private final Map<String, StateUpdater<Map<String, Object>>> stateUpdaters = new ConcurrentHashMap<>();

    private final RocksDB eventStoreDb;
    private final Path rocksDbPath;

    private final String logFilePath;
    private final Path dataDirectoryPath;
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
        this.vcManager = new VectorClockManager(this.localNodeId);

        Path logPath = Paths.get(logFilePath);
        this.dataDirectoryPath = (logPath.getParent() != null) ? logPath.getParent() : Paths.get(".");
        this.rocksDbPath = this.dataDirectoryPath.resolve("event_store_rocksdb");

        if (this.dataDirectoryPath != null) {
            try {
                Files.createDirectories(this.dataDirectoryPath);
                Files.createDirectories(this.rocksDbPath);
            } catch (IOException e) {
                throw new PersistenceException("Failed to create data directory: " + this.rocksDbPath, e);
            }
        }

        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        try {
            this.eventStoreDb = RocksDB.open(options, this.rocksDbPath.toString());
            System.out.println("RocksDB event store initialized at: " + this.rocksDbPath);
        } catch (RocksDBException e) {
            throw new PersistenceException("Failed to INitialize RocksDB event store", e);
        }

        registerDefaultStateUpdaters();
        loadStateFromPersistence();

        System.out.println("CausalLedger initialized: " + getStats());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            saveAllIndexes();
            if (this.eventStoreDb != null) {
                this.eventStoreDb.close();
                System.out.println("RocksDB event store closed");
            }
        }));
    }

    public CausalLedger(String logFilePath) {
        this(logFilePath, 1000);
    }

    public String ingestEvent(String traceId, String serviceName, String serviceVersion, String hostname,
                              String eventType, Map<String, Object> payload, List<String> manualParentEventIds)  throws CausalLoopException, UnknownParentException{

        if (traceId == null || traceId.trim().isEmpty()) {
            throw new IllegalArgumentException("traceid cannot be null or empty");
        }
        if (serviceName == null || serviceName.trim().isEmpty()) {
            throw new IllegalArgumentException("serviceName cannot be null or empty.");
        }
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new IllegalArgumentException("eventType cannot be null or empty.");
        }

        rwLock.writeLock().lock();
        try {

            List<String> finalParentEventIds = new ArrayList<>();
            if (manualParentEventIds != null && !manualParentEventIds.isEmpty()) {
                finalParentEventIds.addAll(manualParentEventIds);
            } else {
                String parentEventIdsFromTrace = this.latestEventIdByTraceId.get(traceId);
                if (parentEventIdsFromTrace != null) {
                    finalParentEventIds.add(parentEventIdsFromTrace);
                }
            }

            for (String parentId : finalParentEventIds) {
                if (!this.containsEvent(parentId)) {
                    throw new UnknownParentException("Parent event " + parentId + " not found in ledger");
                }
            }

            List<EventAtom> parentEvents = new ArrayList<>();
            for (String parentId : finalParentEventIds) {
                EventAtom parentEvent = this.getEvent(parentId);
                if (parentEvent == null) {
                    throw new PersistenceException("FATAL: Parent " + parentId + " found in index but not in RocksDB store.", null);
                }
                parentEvents.add(parentEvent);
            }

            totalCycleChecks++;
            int tempNodeId = causalGraph.getNumVertices();
            Map<Integer, List<Integer>> proposedEdges = new HashMap<>();
            if (!finalParentEventIds.isEmpty()) {
                List<Integer> parentGraphIds = new ArrayList<>();
                for (String parentId : finalParentEventIds) {
                    parentGraphIds.add(eventIdToGraphId.get(parentId));
                }
                proposedEdges.put(tempNodeId, parentGraphIds);
            }

            if (causalGraph.hasCycleWithProposedAdditions(tempNodeId, proposedEdges)) {
                totalCyclesPrevented++;
                throw new CausalLoopException("Ingesitng event would create a causal loop for traceId:" + traceId);
            }

            int newGraphNodeId = causalGraph.addNode();
            for (String parentId : finalParentEventIds) {
                Integer parentGraphId = eventIdToGraphId.get(parentId);
                if (parentGraphId != null) {
                    causalGraph.addDirectedEdge(newGraphNodeId, parentGraphId);
                    this.childrenAdjacencyList.computeIfAbsent(parentGraphId, k-> new ArrayList<>()).add(newGraphNodeId);
                }
            }

            EventAtom newEvent = vcManager.createEvent(traceId, serviceName, serviceVersion, hostname, eventType, payload, parentEvents);
            String newEventId = newEvent.getEventId();

            appendEventToLog(newEvent);
            try {
                String eventJson = newEvent.toLogString();
                eventStoreDb.put(newEventId.getBytes(StandardCharsets.UTF_8), eventJson.getBytes(StandardCharsets.UTF_8));
            } catch (RocksDBException e) {
                throw new PersistenceException("Failed to persist event " + newEventId + " to rocksDB after log write", e);
            }

            eventIdToGraphId.put(newEventId, newGraphNodeId);
            graphIdToEventId.put(newGraphNodeId, newEventId);
            entityToEventIdsIndex.computeIfAbsent(serviceName, k -> new ArrayList<>()).add(newEventId);
            eventTypeToEventIdsIndex.computeIfAbsent(eventType, k -> new ArrayList<>()).add(newEventId);
            traceIdToEventIdsIndex.computeIfAbsent(traceId, k -> new ArrayList<>()).add(newEventId);

            this.latestEventIdByTraceId.put(traceId, newEventId);

            totalEventsIngested++;
            return newEventId;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<EventAtom> getEventsByTraceId(String traceId) {
        rwLock.readLock().lock();
        try {
            List<String> eventIds = this.traceIdToEventIdsIndex.getOrDefault(traceId, Collections.emptyList());
            if (eventIds.isEmpty()) {
                return Collections.emptyList();
            }

            List<EventAtom> events = new ArrayList<>(eventIds.size());
            for (String eventId : eventIds) {
                EventAtom event = getEvent(eventId);
                if (event != null) {
                    events.add(event);
                }
            }

            return events;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void loadStateFromPersistence() {
        boolean indexesLoadedFromFiles = loadAllIndexes();

        List<EventAtom> eventsToProcess;

        if (!indexesLoadedFromFiles) {
            System.out.println("Indexes not found. Proceeding with full log processing for RocksDB hydration and index building");
            eventsToProcess = hydrateRocksDBAndCollectEvents();
        } else {
            System.out.println("Indexes loaded successfully. Skipping full log scan - using preloaded indexes for graph reconstruction");
            eventsToProcess = collectEventsFromLogOnly();
            ensureRocksDBIsAccessible();
        }

        rebuildInMemoryGraphStructures(eventsToProcess, indexesLoadedFromFiles);
    }

    private List<EventAtom> collectEventsFromLogOnly() {
        Path logPath = Paths.get(logFilePath);
        if (!Files.exists(logPath)) {
            System.out.println("Event log file doesn't exist: " + logFilePath);
            return Collections.emptyList();
        }

        System.out.println("Collecting events from log for graph reconstruction: " + logFilePath);
        List<EventAtom> events = new ArrayList<>();

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
                    events.add(event);
                } catch (Exception e) {
                    System.err.println("Corrupt event line " + lineNumber + " in log, skipping: " + line + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new PersistenceException("Failed during log processing for event collection", e);
        }

        System.out.println("Event collection completed. Events collected: " + events.size());
        return events;
    }

    private List<EventAtom> hydrateRocksDBAndCollectEvents() {
        Path logPath = Paths.get(logFilePath);

        if (!Files.exists(logPath)) {
            System.out.println("Event log file doesn't exist: " + logFilePath);
            return Collections.emptyList();
        }

        System.out.println("Processing event log for RocksDB hydration: "+ logFilePath);
        List<EventAtom> events = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)){
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
                    events.add(event);

                    byte[] eventIdBytes = event.getEventId().getBytes(StandardCharsets.UTF_8);
                    byte[] eventJsonBytes = line.getBytes(StandardCharsets.UTF_8);
                    eventStoreDb.put(eventIdBytes, eventJsonBytes);

                } catch (Exception e) {
                    System.err.println("Corrupt event line " + lineNumber + " in log during RocksDB hydration, skipping: " + line + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new PersistenceException("Failed during log processing/RocksDB hydration", e);
        }
        System.out.println("RocksDB hydration completed. Events processed: " + eventIdToGraphId.size());
        return events;
    }

    private void ensureRocksDBIsAccessible() {
        try {
            RocksIterator iterator = eventStoreDb.newIterator();
            iterator.seekToFirst();
            if (iterator.isValid()) {
                System.out.println("RocksDB is accessible and contains data");
            } else {
                System.out.println("RocksDB is accessible but appears empty");
            }
            iterator.close();
        } catch (Exception e) {
            System.err.println("warning: RocksDB accessibility check failed: " + e.getMessage());
        }
    }

    private void rebuildInMemoryGraphStructures(List<EventAtom> allEvents, boolean indexesWerePreLoaded) {
        if (!indexesWerePreLoaded) {
            // cold path

            causalGraph.clearGraph();
            eventIdToGraphId.clear();
            graphIdToEventId.clear();
            entityToEventIdsIndex.clear();
            eventTypeToEventIdsIndex.clear();
            traceIdToEventIdsIndex.clear();
            childrenAdjacencyList.clear();

            for (EventAtom event : allEvents) {
                int graphNodeId = causalGraph.addNode();
                eventIdToGraphId.put(event.getEventId(), graphNodeId);
                graphIdToEventId.put(graphNodeId, event.getEventId());
                entityToEventIdsIndex.computeIfAbsent(event.getServiceName(), k -> new ArrayList<>())
                        .add(event.getEventId());
                eventTypeToEventIdsIndex.computeIfAbsent(event.getEventType(), k -> new ArrayList<>())
                        .add(event.getEventId());
                traceIdToEventIdsIndex.computeIfAbsent(event.getTraceId(), k -> new ArrayList<>())
                        .add(event.getEventId());
            }

            for (EventAtom event : allEvents) {
                Integer effectGraphId = eventIdToGraphId.get(event.getEventId());
                for (String parentId : event.getCausalParentEventIds()) {
                    Integer causeGraphId = eventIdToGraphId.get(parentId);
                    if (causeGraphId != null) {
                        causalGraph.addDirectedEdge(effectGraphId, causeGraphId);
                        childrenAdjacencyList.computeIfAbsent(causeGraphId, k -> new ArrayList<>())
                                .add(effectGraphId);
                    }
                }
            }

            System.out.println("COLD PATH: Complete rebuild finished");

        } else {
            // warm path
            int maxGraphId = determineMaxGraphIdFromMappings();
            if (maxGraphId >= 0) {
                causalGraph.setNumVerticesAndEnsureCapacity(maxGraphId +1 );
            }

            causalGraph.clearEdges();
            for (EventAtom event : allEvents) {
                Integer effectGraphId = this.eventIdToGraphId.get(event.getEventId());
                if (effectGraphId != null) {
                    for (String parentId : event.getCausalParentEventIds()) {
                        Integer causeGraphId = this.eventIdToGraphId.get(parentId);
                        if (causeGraphId != null) {
                            causalGraph.addDirectedEdge(effectGraphId, causeGraphId);
                        }
                    }
                }
            }

            System.out.println("Graph structure rebuilt from pre-loaded indexes (" +
                    eventIdToGraphId.size() + " events, " +
                    childrenAdjacencyList.size() + " parent nodes with children). No log processing required.");
        }
    }

    private int determineMaxGraphIdFromMappings() {
        if (eventIdToGraphId.isEmpty()) {
            return -1;
        }
        return eventIdToGraphId.values().stream().max(Integer::compareTo).orElse(-1);
    }

    private Path getIndexFilePath(String fileName) {
        return this.dataDirectoryPath.resolve(fileName);
    }

    private <K, V> Map<K, V> loadIndexMap(String fileName) {
        Path indexFile = getIndexFilePath(fileName);
        if (Files.exists(indexFile)) {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new BufferedInputStream(Files.newInputStream(indexFile)))) {
                Object obj = ois.readObject();
                if (obj instanceof Map) {
                    System.out.println("Successfully loaded index from: " + fileName);
                    // Ensure the loaded map is of the expected mutable type for thread safety
                    return new ConcurrentHashMap<>((Map<K, V>) obj);
                } else {
                    System.err.println("Warning: Index file " + fileName + " does not contain a Map. Rebuilding index.");
                }
            } catch (IOException | ClassNotFoundException | ClassCastException e) {
                System.err.println("Warning: Failed to load index from " + fileName + " (" + e.getMessage() + "). Rebuilding index.");
            }
        }
        return null; // Indicate index was not loaded
    }

    private <K, V> void saveIndexMap(Map<K, V> mapToSave, String fileName) {
        Map<K, V> serializableMap = new HashMap<>(mapToSave);

        Path indexFile = getIndexFilePath(fileName);
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(indexFile)))){
            oos.writeObject(serializableMap);
            System.out.println("Successfully saved index to: " + fileName);
        } catch (IOException e) {
            System.err.println("Error: Failed to save index to " + fileName + " (" + e.getMessage() + "). ");
            e.printStackTrace();
        }
    }

    private boolean loadAllIndexes() {
        System.out.println("Attempting to load persisted indexes...");

        Map<String, List<String>> tempEntityIndex = loadIndexMap(ENTITY_INDEX_FILENAME);
        Map<Integer, List<Integer>> tempChildrenAdj = loadIndexMap(CHILDREN_ADJ_FILENAME);
        Map<String, Integer> tempEventToGraph = loadIndexMap(EVENT_TO_GRAPH_ID_FILENAME);
        Map<Integer, String> tempGraphToEvent = loadIndexMap(GRAPH_TO_EVENT_ID_FILENAME);
        Map<String, List<String>> tempEventTypeIndex = loadIndexMap(EVENT_TYPE_INDEX_FILENAME);
        Map<String, List<String>> tempTraceIdIndex = loadIndexMap(TRACE_ID_INDEX_FILENAME);

        if (tempEntityIndex != null && tempChildrenAdj != null &&
                tempEventToGraph != null && tempGraphToEvent != null && tempEventTypeIndex != null && tempTraceIdIndex != null) {

            this.entityToEventIdsIndex.clear();
            this.entityToEventIdsIndex.putAll(tempEntityIndex);

            this.childrenAdjacencyList.clear();
            this.childrenAdjacencyList.putAll(tempChildrenAdj);

            this.eventIdToGraphId.clear();
            this.eventIdToGraphId.putAll(tempEventToGraph);

            this.graphIdToEventId.clear();
            this.graphIdToEventId.putAll(tempGraphToEvent);

            this.eventTypeToEventIdsIndex.clear();
            this.eventTypeToEventIdsIndex.putAll(tempEventTypeIndex);

            this.traceIdToEventIdsIndex.clear();
            this.traceIdToEventIdsIndex.putAll(tempTraceIdIndex);

            System.out.println("All core indexes successfully loaded from disk.");
            return true;
        }
        System.out.println("One or more core indexes could not be loaded. Indexes will be rebuilt.");
        return false;
    }

    private void saveAllIndexes() {
        System.out.println("Attempting to save indexes on shutdown...");

        rwLock.writeLock().lock();
        try {
            saveIndexMap(this.entityToEventIdsIndex, ENTITY_INDEX_FILENAME);
            saveIndexMap(this.childrenAdjacencyList, CHILDREN_ADJ_FILENAME);
            saveIndexMap(this.eventIdToGraphId, EVENT_TO_GRAPH_ID_FILENAME);
            saveIndexMap(this.graphIdToEventId, GRAPH_TO_EVENT_ID_FILENAME);
            saveIndexMap(this.eventTypeToEventIdsIndex, EVENT_TYPE_INDEX_FILENAME);
            saveIndexMap(this.traceIdToEventIdsIndex, TRACE_ID_INDEX_FILENAME);
            System.out.println("Indexes saved.");
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void registerDefaultStateUpdaters() {
        stateUpdaters.put("USER_CREATED", ((currentState, eventPayload, eventType) -> {
            Map<String, Object> newState = new HashMap<>();
            newState.put("userId", eventPayload.get("userId"));
            newState.put("username", eventPayload.get("username"));
            newState.put("isActive", true);
            newState.put("version", 1);
            newState.put("createdAt", eventPayload.get("timestamp"));
            System.out.println("Applying USER_CREATED: " + eventPayload + " -> " + newState);
            return newState;
        }));

        stateUpdaters.put("USER_RENAMED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("username", payload.get("newUsername"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastModified", payload.get("timestamp"));
            System.out.println("Applying USER_RENAMED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("USER_DEACTIVATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("isActive", false);
            newState.put("deactivationReason", payload.get("reason"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("deactivatedAt", payload.get("timestamp"));
            System.out.println("Applying USER_DEACTIVATED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("USER_REACTIVATED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("isActive", true);
            newState.remove("deactivationReason");
            newState.remove("deactivatedAt");
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("reactivatedAt", payload.get("timestamp"));
            System.out.println("Applying USER_REACTIVATED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("PRODUCT_ADDED", (currentState, payload, type) -> {
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

        stateUpdaters.put("PRODUCT_UPDATED", (currentState, payload, type) -> {
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

        stateUpdaters.put("STOCK_INCREMENTED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            int currentStock = (Integer) currentState.getOrDefault("stock", 0);
            int amountIncremented = (Integer) payload.getOrDefault("amount", 0);
            newState.put("stock", currentStock + amountIncremented);
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("lastStockUpdate", payload.get("timestamp"));
            System.out.println("Applying STOCK_INCREMENTED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("STOCK_DECREMENTED", (currentState, payload, type) -> {
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
        stateUpdaters.put("ORDER_CREATED", (currentState, payload, type) -> {
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

        stateUpdaters.put("ORDER_CONFIRMED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("status", "CONFIRMED");
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("confirmedAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_CONFIRMED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("ORDER_SHIPPED", (currentState, payload, type) -> {
            Map<String, Object> newState = new HashMap<>(currentState);
            newState.put("status", "SHIPPED");
            newState.put("trackingNumber", payload.get("trackingNumber"));
            newState.put("version", (Integer) currentState.getOrDefault("version", 0) + 1);
            newState.put("shippedAt", payload.get("timestamp"));
            System.out.println("Applying ORDER_SHIPPED: " + payload + " to " + currentState + " -> " + newState);
            return newState;
        });

        stateUpdaters.put("ORDER_CANCELLED", (currentState, payload, type) -> {
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
        stateUpdaters.put(eventType, updater);
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
                EventAtom event = getEvent(eventId);
                if (event != null && entityId.equals(event.getServiceName())) {
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

                StateUpdater<Map<String, Object>> updater = stateUpdaters.get(event.getEventType());
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
                EventAtom event = getEvent(eventId);
                if (event != null && entityId.equals(event.getServiceName())) {
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
                StateUpdater<Map<String, Object>> updater = stateUpdaters.get(event.getEventType());
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
            byte[] eventIdBytes = eventId.getBytes(StandardCharsets.UTF_8);
            byte[] eventJsonBytes;

            try {
                eventJsonBytes = eventStoreDb.get(eventIdBytes);
            } catch (RocksDBException e) {
                System.err.println("RocksDB error getting event " + eventId + ": " + e.getMessage());
                return null;
            }

            if (eventJsonBytes == null) {
                return null;
            }

            try {
                String eventJson = new String(eventJsonBytes, StandardCharsets.UTF_8);
                return EventAtom.fromLogString(eventJson);
            } catch (Exception e) {
                System.err.println("Error deserializing event " + eventId + " from RocksDB: " + e.getMessage());
                return null;
            }
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
            return eventIdToGraphId.containsKey(eventId);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public String getStats() {
        rwLock.readLock().lock();
        try {
            return String.format(
                    "CausalLedger[events=%d, ingested=%d, cycleChecks=%d, cyclesPrevented=%d, graph=%s, nodeId='%s', vectorClock='%s', logFile='%s']",
                    eventIdToGraphId.size(), totalEventsIngested, totalCycleChecks, totalCyclesPrevented,
                    causalGraph.getGraphStats(), localNodeId, vcManager.getCurrentClock(), logFilePath);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<EventAtom> getEventsByEntity(String entityId) {
        rwLock.readLock().lock();
        try {
            List<String> eventIdsForEntity = this.entityToEventIdsIndex.getOrDefault(entityId, Collections.emptyList());
            if (eventIdsForEntity.isEmpty()) {
                return Collections.emptyList();
            }

            List<EventAtom> events = new ArrayList<>(eventIdsForEntity.size());
            for (String eventId : eventIdsForEntity) {
                EventAtom event = getEvent(eventId);
                if (event != null) {
                    events.add(event);
                }
            }
            return events;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public List<EventAtom> getEventsByType(String eventType) {
        rwLock.readLock().lock();
        try {
            List<String> eventIdsForType = this.eventTypeToEventIdsIndex.getOrDefault(eventType, Collections.emptyList());
            if (eventIdsForType.isEmpty()) {
                return Collections.emptyList();
            }

            List<EventAtom> events = new ArrayList<>(eventIdsForType.size());
            for (String eventId : eventIdsForType) {
                EventAtom event = getEvent(eventId);
                if (event != null) {
                    events.add(event);
                }
            }

            return events.stream()
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

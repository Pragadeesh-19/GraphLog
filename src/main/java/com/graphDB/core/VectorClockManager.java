package com.graphDB.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class VectorClockManager {

    private final String localNodeId;
    private final VectorClock localClock;
    private final Map<String, VectorClock> nodeClocks;

    public VectorClockManager(String localNodeId) {
        if (localNodeId == null || localNodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("Local node ID cannot be null or empty.");
        }

        this.localNodeId = localNodeId;
        this.localClock = new VectorClock();
        this.nodeClocks = new ConcurrentHashMap<>();

        this.localClock.tick(this.localNodeId);
        this.nodeClocks.put(localNodeId, new VectorClock(localClock));
    }

    public synchronized EventAtom createEvent(
            String traceId, String serviceName, String serviceVersion, String hostname,
            String eventType, Map<String, Object> payload,
            List<EventAtom> parentEvents) {

        if (traceId == null || traceId.trim().isEmpty()) {
            throw new IllegalArgumentException("Trace ID cannot be null or empty");
        }
        if (serviceName == null || serviceName.trim().isEmpty()) {
            throw new IllegalArgumentException("Service name cannot be null or empty");
        }
        if (eventType == null || eventType.trim().isEmpty()) {
            throw new IllegalArgumentException("Event type cannot be null or empty");
        }

        VectorClock newClock = new VectorClock(this.localClock);

        List<String> parentEventIds = new ArrayList<>();
        if (parentEvents != null && !parentEvents.isEmpty()) {
            for (EventAtom parent : parentEvents) {
                newClock.update(parent.getVectorClock());
                parentEventIds.add(parent.getEventId());
            }
        }

        newClock.tick(this.localNodeId);
        this.localClock.update(newClock);
        this.nodeClocks.put(localNodeId, new VectorClock(localClock));

        return new EventAtom(
                this.localNodeId,
                traceId,
                serviceName,
                serviceVersion,
                hostname,
                eventType,
                payload,
                parentEventIds,
                newClock
        );
    }

    /**
     * Process an incoming event from another node
     * Updates local vector clock according to vector clock rules
     * This method is for future distributed node communication
     *
     * @param eventFromOtherNode The event received from another distributed node
     */
    public synchronized void receiveEvent(EventAtom eventFromOtherNode) {
        if (eventFromOtherNode == null) {
            throw new IllegalArgumentException("Cannot receive null event");
        }
        if (eventFromOtherNode.getVectorClock() == null) {
            throw new IllegalArgumentException("Cannot receive event without vector clock");
        }

        this.localClock.update(eventFromOtherNode.getVectorClock(), this.localNodeId);

        this.nodeClocks.put(localNodeId, new VectorClock(localClock));
        this.nodeClocks.put(eventFromOtherNode.getNodeId(),
                new VectorClock(eventFromOtherNode.getVectorClock()));
    }

    /**
     * Get current local vector clock (returns a copy for thread safety)
     *
     * @return A copy of the current local vector clock
     */
    public synchronized VectorClock getCurrentClock() {
        return new VectorClock(localClock);
    }

    /**
     * Get the last known vector clock for a specific node
     *
     * @param nodeId The node ID to get the clock for
     * @return A copy of the node's last known vector clock, or empty clock if unknown
     */
    public synchronized VectorClock getNodeClock(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        VectorClock clock = nodeClocks.get(nodeId);
        return clock != null ? new VectorClock(clock) : new VectorClock();
    }

    /**
     * Get all known nodes
     *
     * @return A copy of the set of known node IDs
     */
    public synchronized Set<String> getKnownNodes() {
        return new HashSet<>(nodeClocks.keySet());
    }

    /**
     * Compare two events and determine their causal relationship
     *
     * @param event1 The first event to compare
     * @param event2 The second event to compare
     * @return The causal relationship between the events
     */
    public static CausalRelationship compareCausalOrder(EventAtom event1, EventAtom event2) {

        if (event1 == null || event2 == null) {
            return CausalRelationship.UNDEFINED;
        }

        return event1.getCausalRelationshipWith(event2);
    }

    /**
     * Check if we can deliver an event (all its causal dependencies are satisfied)
     * This is useful for implementing causal message delivery
     *
     * @param event The event to check for delivery
     * @param deliveredEvents Set of events that have already been delivered
     * @return true if the event can be safely delivered
     */
    public boolean canDeliver(EventAtom event, Set<EventAtom> deliveredEvents) {
        if (event == null) {
            return false;
        }
        if (deliveredEvents == null) {
            deliveredEvents = Collections.emptySet();
        }

        for (String parentId : event.getCausalParentEventIds()) {
            boolean found = deliveredEvents.stream()
                    .anyMatch(delivered -> delivered.getEventId().equals(parentId));

            if (!found) {
                return false;
            }
        }

        return true;
    }

    /**
     * Clear all vector clock state and reinitialize
     * Useful for testing scenarios
     */
    public synchronized void reset() {
        localClock.clear();
        localClock.tick(localNodeId);
        nodeClocks.clear();
        nodeClocks.put(localNodeId, new VectorClock(localClock));
    }

    /**
     * Get a snapshot of the current state for debugging
     *
     * @return A map containing current state information
     */
    public synchronized Map<String, Object> getDebugState() {
        Map<String, Object> state = new HashMap<>();
        state.put("localNodeId", localNodeId);
        state.put("localClock", localClock.getClock());
        state.put("knownNodes", new HashSet<>(nodeClocks.keySet()));
        state.put("nodeClocks", nodeClocks.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getClock()
                )));
        return state;
    }

    @Override
    public synchronized String toString() {
        return String.format("VectorClockManager{localNodeId='%s', localClock=%s, knownNodes=%s}",
                localNodeId, localClock, nodeClocks.keySet());
    }
}

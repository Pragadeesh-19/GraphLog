package com.graphlog.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class VectorClock {

    private final Map<String, Long> clock;

    public VectorClock() {
        this.clock = new ConcurrentHashMap<>();
    }

    @JsonCreator
    public VectorClock(@JsonProperty("clock") Map<String, Long> clock) {
        this.clock = new ConcurrentHashMap<>(clock != null ? clock : new HashMap<>());
    }

    public VectorClock(VectorClock other) {
        this.clock = new ConcurrentHashMap<>(other != null
                ? other.clock
                : new ConcurrentHashMap<>());
    }

    /**
     * Increments the logical timestamp for the specified node.
     * This is called when a node performs a local event.
     *
     * @param nodeId The node performing the local event
     * @return The new timestamp value for this node
     */
    public synchronized long tick(String nodeId) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("NodeId cannot be null or empty");
        }

        long newValue = clock.getOrDefault(nodeId, 0L) + 1;
        clock.put(nodeId, newValue);
        return newValue;
    }

    /**
     * Updates this Vector Clock by taking the maximum of each node's timestamp
     * between this clock and the other clock, then increments the current node's timestamp.
     *
     * This is called when receiving a message/event from another node.
     *
     * @param other The Vector Clock from the received message
     * @param currentNodeId The ID of the node receiving the message
     */
    public synchronized void update(VectorClock other, String currentNodeId) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot update with null Vector Clock");
        }
        if (currentNodeId == null || currentNodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("Current node ID cannot be null or empty");
        }

        // Take the maximum timestamp for each node
        for (Map.Entry<String, Long> entry : other.clock.entrySet()) {
            String nodeId = entry.getKey();
            long otherTimestamp = entry.getValue();
            long currentTimestamp = this.clock.getOrDefault(nodeId, 0L);
            this.clock.put(nodeId, Math.max(currentTimestamp, otherTimestamp));
        }

        tick(currentNodeId);
    }

    /**
     * Updates this Vector Clock by taking the maximum of each node's timestamp
     * between this clock and the other clock, WITHOUT incrementing any node's timestamp.
     * This is a utility method for scenarios where you want to merge clocks without
     * treating it as a local event.
     *
     * @param other The Vector Clock to merge with
     */
    public synchronized void update(VectorClock other) {
        if (other == null) {
            throw new IllegalArgumentException("Cannot update with null Vector Clock");
        }

        // Take the maximum timestamp for each node
        for (Map.Entry<String, Long> entry : other.clock.entrySet()) {
            String nodeId = entry.getKey();
            long otherTimestamp = entry.getValue();
            long currentTimestamp = this.clock.getOrDefault(nodeId, 0L);
            this.clock.put(nodeId, Math.max(currentTimestamp, otherTimestamp));
        }
    }

    /**
     * Determines if this Vector Clock happens before the other Vector Clock.
     *
     * VC1 happens before VC2 if:
     * - For all nodes, VC1[node] <= VC2[node]
     * - For at least one node, VC1[node] < VC2[node]
     *
     * @param other The other Vector Clock to compare against
     * @return true if this Vector Clock happens before the other
     */
    public boolean happensBefore(VectorClock other) {
        if (other == null) {
            return false;
        }

        boolean hasStrictlySmaller = false;

        // Get all unique node IDs from both clocks
        Set<String> allNodes = new HashSet<>(this.clock.keySet());
        allNodes.addAll(other.clock.keySet());

        for (String nodeId : allNodes) {
            long thisTimestamp = this.clock.getOrDefault(nodeId, 0L);
            long otherTimestamp = other.clock.getOrDefault(nodeId, 0L);

            if (thisTimestamp > otherTimestamp) {
                // This violates the "less than or equal" condition
                return false;
            } else if (thisTimestamp < otherTimestamp) {
                // Found at least one strictly smaller timestamp
                hasStrictlySmaller = true;
            }
        }

        return hasStrictlySmaller;
    }

    /**
     * Determines if this Vector Clock happens after the other Vector Clock.
     *
     * @param other The other Vector Clock to compare against
     * @return true if this Vector Clock happens after the other
     */
    public boolean happensAfter(VectorClock other) {
        return other != null && other.happensBefore(this);
    }

    /**
     * Determines if this Vector Clock is concurrent with the other Vector Clock.
     * Two Vector Clocks are concurrent if neither happens before the other.
     *
     * @param other The other Vector Clock to compare against
     * @return true if the Vector Clocks are concurrent
     */
    public boolean isConcurrentWith(VectorClock other) {
        return other != null && !this.happensBefore(other) && !other.happensBefore(this);
    }

    /**
     * Gets the timestamp for a specific node.
     *
     * @param nodeId The node ID to get the timestamp for
     * @return The timestamp for the node, or 0 if the node is not in the clock
     */
    public long getTimestamp(String nodeId) {
        return clock.getOrDefault(nodeId, 0L);
    }

    /**
     * Gets all node IDs currently tracked in this Vector Clock.
     *
     * @return Set of node IDs
     */
    public Set<String> getNodeIds() {
        return new HashSet<>(clock.keySet());
    }

    /**
     * Gets a copy of the internal clock map.
     * Used by Jackson for JSON serialization.
     *
     * @return Copy of the internal clock map
     */
    @JsonProperty("clock")
    public Map<String, Long> getClock() {
        return new HashMap<>(clock);
    }

    /**
     * Checks if this Vector Clock is empty (no nodes tracked).
     *
     * @return true if no nodes are tracked in this Vector Clock
     */
    public boolean isEmpty() {
        return clock.isEmpty();
    }

    /**
     * Gets the number of nodes tracked in this Vector Clock.
     *
     * @return Number of nodes tracked
     */
    public int size() {
        return clock.size();
    }

    /**
     * Creates a deep copy of this Vector Clock.
     *
     * @return A new Vector Clock with the same timestamps
     */
    public VectorClock copy() {
        return new VectorClock(this);
    }

    /**
     * Clears all timestamps in this Vector Clock.
     */
    public synchronized void clear() {
        clock.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorClock that = (VectorClock) o;
        return Objects.equals(clock, that.clock);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clock);
    }

    @Override
    public String toString() {
        return "VectorClock{" + clock + '}';
    }

}

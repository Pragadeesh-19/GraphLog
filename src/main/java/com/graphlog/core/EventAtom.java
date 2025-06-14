package com.graphlog.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;

import java.time.Instant;
import java.util.*;

@Data
public class EventAtom {

    private final String eventId;
    private final Instant timestamp;
    private final String nodeId;
    private final String entityId;
    private final String eventType;
    private final Map<String, Object> payload;
    private final List<String> causalParentEventIds;
    private final VectorClock vectorClock;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // primary constructor for vector clock aware creation
    public EventAtom(String nodeId, String entityId, String eventType, Map<String, Object> payload, List<String> causalParentEventIds, VectorClock vectorClock) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.nodeId = nodeId;
        this.entityId = entityId;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds));
        this.vectorClock = (vectorClock != null) ? new VectorClock(vectorClock) : new VectorClock();
    }

    // Legacy constructor for backward compatibility, defaults to single node
    public EventAtom(String entityId, String eventType, Map<String, Object> payload, List<String> causalParentEventIds) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.nodeId = "default-node"; // Default node for single-node operation
        this.entityId = entityId;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload != null ? payload : Collections.emptyMap()));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds != null ? causalParentEventIds : Collections.emptyList()));
        this.vectorClock = new VectorClock(); // Empty vector clock for backward compatibility
    }

    @JsonCreator
    public EventAtom(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("entityId") String entityId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("payload") Map<String, Object> payload,
            @JsonProperty("causalParentEventIds") List<String> causalParentEventIds,
            @JsonProperty("vectorClock") VectorClock vectorClock) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.nodeId = nodeId != null ? nodeId : "default-node";
        this.entityId = entityId;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload != null ? payload : new HashMap<>()));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds != null ? causalParentEventIds : new ArrayList<>()));
        this.vectorClock = vectorClock != null ? new VectorClock(vectorClock) : new VectorClock();
    }

    public VectorClock getVectorClock() {
        return new VectorClock(this.vectorClock);
    }

    /**
     * Determines if this event happens before the other event in causal order.
     * Event A happens before Event B if A's vector clock is less than or equal to B's vector clock
     * for all nodes, and strictly less for at least one node.
     *
     * @param other The other EventAtom to compare against
     * @return true if this event causally happens before the other event
     */
    public boolean happensBefore(EventAtom other) {
        if (other == null || other.vectorClock == null) {
            return false;
        }
        return this.vectorClock.happensBefore(other.vectorClock);
    }

    /**
     * Determines if this event happens after the other event in causal order.
     * Event A happens after Event B if B happens before A.
     *
     * @param other The other EventAtom to compare against
     * @return true if this event causally happens after the other event
     */
    public boolean happensAfter(EventAtom other) {
        if (other == null) {
            return false;
        }
        return other.happensBefore(this);
    }

    /**
     * Determines if this event is concurrent with the other event.
     * Two events are concurrent if neither happens before the other.
     *
     * @param other The other EventAtom to compare against
     * @return true if this event is concurrent with the other event
     */
    public boolean isConcurrentWith(EventAtom other) {
        if (other == null) {
            return false;
        }
        return !this.happensBefore(other) && !other.happensBefore(this);
    }

    /**
     * Determines if this event is causally related to the other event.
     * Two events are causally related if one happens before the other.
     *
     * @param other The other EventAtom to compare against
     * @return true if this event is causally related to the other event
     */
    public boolean isCausallyRelatedTo(EventAtom other) {
        if (other == null) {
            return false;
        }
        return this.happensBefore(other) || other.happensBefore(this);
    }

    /**
     * Gets the causal relationship between this event and another event.
     *
     * @param other The other EventAtom to compare against
     * @return CausalRelationship enum indicating the relationship
     */
    public CausalRelationship getCausalRelationshipWith(EventAtom other) {
        if (other == null) {
            return CausalRelationship.UNDEFINED;
        }
        if (this.vectorClock.equals(other.vectorClock)) {
            return CausalRelationship.IDENTICAL;
        }
        if (this.happensBefore(other)) {
            return CausalRelationship.CAUSES;
        }
        if (this.happensAfter(other)) {
            return CausalRelationship.CAUSED_BY;
        }
        return CausalRelationship.CONCURRENT;
    }


    public String toLogString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize EventAtom to Json", e);
        }
    }

    public static EventAtom fromLogString(String jsonString) {
        try {
            return OBJECT_MAPPER.readValue(jsonString, EventAtom.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize EventAtom from JSON: " + jsonString, e);
        }
    }

    @Override
    public String toString() {
        return "EventAtom{" +
                "eventId='" + eventId + '\'' +
                ", timestamp=" + timestamp +
                ", nodeId='" + nodeId + '\'' +
                ", entityId='" + entityId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", payload=" + payload +
                ", causalParentEventIds=" + causalParentEventIds +
                ", vectorClock=" + vectorClock +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventAtom eventAtom = (EventAtom) o;
        return Objects.equals(eventId, eventAtom.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }
}

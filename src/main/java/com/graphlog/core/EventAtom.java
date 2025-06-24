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

public class EventAtom {

    private final String eventId;
    private final Instant timestamp;
    private final String nodeId;
    private final String serviceName;
    private final String traceId;
    private final String serviceVersion;
    private final String hostname;
    private final String eventType;
    private final Map<String, Object> payload;
    private final List<String> causalParentEventIds;
    private final VectorClock vectorClock;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // primary constructor for vector clock aware creation
    public EventAtom(String nodeId, String traceId, String serviceName, String serviceVersion, String hostname, String eventType, Map<String, Object> payload, List<String> causalParentEventIds, VectorClock vectorClock) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.nodeId = nodeId;
        this.traceId = traceId;
        this.serviceName = serviceName;
        this.serviceVersion = serviceVersion;
        this.hostname = hostname;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds));
        this.vectorClock = (vectorClock != null) ? new VectorClock(vectorClock) : new VectorClock();
    }

    // Legacy constructor for backward compatibility, defaults to single node
    public EventAtom(String nodeId, String entityId, String eventType, Map<String, Object> payload, List<String> causalParentEventIds, VectorClock vectorClock) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.nodeId = nodeId;
        this.traceId = "unknown-trace"; // Default for legacy
        this.serviceName = entityId; // Map old entityId to serviceName
        this.serviceVersion = "unknown-version"; // Default
        this.hostname = "unknown-hostname"; // Default
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload != null ? payload : Collections.emptyMap()));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds != null ? causalParentEventIds : Collections.emptyList()));
        this.vectorClock = (vectorClock != null) ? new VectorClock(vectorClock) : new VectorClock();
    }

    @JsonCreator
    public EventAtom(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("serviceName") String serviceName,
            @JsonProperty("traceId") String traceId,
            @JsonProperty("serviceVersion") String serviceVersion,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("payload") Map<String, Object> payload,
            @JsonProperty("causalParentEventIds") List<String> causalParentEventIds,
            @JsonProperty("vectorClock") VectorClock vectorClock,
            @JsonProperty("entityId") String entityId) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.nodeId = nodeId != null ? nodeId : "default-node";
        this.traceId = traceId != null ? traceId : "unknown-trace";
        this.serviceName = serviceName != null ? serviceName : (entityId != null ? entityId : "unknown-service");
        this.serviceVersion = serviceVersion != null ? serviceVersion : "unknown-version";
        this.hostname = hostname != null ? hostname : "unknown-hostname";
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload != null ? payload : new HashMap<>()));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds != null ? causalParentEventIds : new ArrayList<>()));
        this.vectorClock = vectorClock != null ? new VectorClock(vectorClock) : new VectorClock();
    }

    public String getEventId() { return eventId; }
    public Instant getTimestamp() { return timestamp; }
    public String getNodeId() { return nodeId; }
    public String getServiceName() { return serviceName; }
    public String getTraceId() { return traceId; }
    public String getServiceVersion() { return serviceVersion; }
    public String getHostname() { return hostname; }
    public String getEventType() { return eventType; }
    public Map<String, Object> getPayload() { return payload; }
    public List<String> getCausalParentEventIds() { return causalParentEventIds; }

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
                ", serviceName='" + serviceName + '\'' +
                ", traceId='" + traceId + '\'' +
                ", serviceVersion='" + serviceVersion + '\'' +
                ", hostname='" + hostname + '\'' +
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

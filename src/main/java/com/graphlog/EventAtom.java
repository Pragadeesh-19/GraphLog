package com.graphlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.*;

public class EventAtom {

    private final String eventId;
    private final Instant timestamp;
    private final String entityId;
    private final String eventType;
    private final Map<String, Object> payload;
    private final List<String> causalParentEventIds;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public EventAtom(String entityId, String eventType, Map<String, Object> payload, List<String> causalParentEventIds) {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.entityId = entityId;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds));
    }

    @JsonCreator
    public EventAtom(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("entityId") String entityId,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("payload") Map<String, Object> payload,
            @JsonProperty("causalParentEventIds") List<String> causalParentEventIds) {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.entityId = entityId;
        this.eventType = eventType;
        this.payload = Collections.unmodifiableMap(new HashMap<>(payload != null ? payload : new HashMap<>()));
        this.causalParentEventIds = Collections.unmodifiableList(new ArrayList<>(causalParentEventIds != null ? causalParentEventIds : new ArrayList<>()));
    }


    public String getEventId() {
        return eventId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEventType() {
        return eventType;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public List<String> getCausalParentEventIds() {
        return causalParentEventIds;
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
                ", entityId='" + entityId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", payload=" + payload +
                ", causalParentEventIds=" + causalParentEventIds +
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

package com.graphlog.tools;

import com.graphlog.core.EventAtom;

public class TestDeserialization {

    public static void main(String[] args) {
        String jsonLine49998 = "{\"eventId\":\"e3b03877-4e60-482c-8246-2686e1d02b58\",\"timestamp\":\"2025-06-14T08:41:19.140945200Z\",\"nodeId\":\"GRAPH_NODE_01\",\"entityId\":\"entity-99\",\"eventType\":\"ENTITY_UPDATED_499\",\"payload\":{\"value\":499,\"entitySuffix\":99,\"timestamp\":\"2025-06-14T08:41:19.133389200Z\"},\"causalParentEventIds\":[\"457f8cd8-afa7-4743-97ec-24bc6b8e7013\"],\"vectorClock\":{\"clock\":{\"GRAPH_NODE_01\":50001},\"nodeIds\":[\"GRAPH_NODE_01\"],\"empty\":false}} - Failed to deserialize EventAtom from JSON: {\"eventId\":\"e3b03877-4e60-482c-8246-2686e1d02b58\",\"timestamp\":\"2025-06-14T08:41:19.140945200Z\",\"nodeId\":\"GRAPH_NODE_01\",\"entityId\":\"entity-99\",\"eventType\":\"ENTITY_UPDATED_499\",\"payload\":{\"value\":499,\"entitySuffix\":99,\"timestamp\":\"2025-06-14T08:41:19.133389200Z\"},\"causalParentEventIds\":[\"457f8cd8-afa7-4743-97ec-24bc6b8e7013\"],\"vectorClock\":{\"clock\":{\"GRAPH_NODE_01\":50001},\"nodeIds\":[\"GRAPH_NODE_01\"],\"empty\":false}}";
        try {
            EventAtom event = EventAtom.fromLogString(jsonLine49998);
            System.out.println("Successfully deserialized: " + event);
        } catch (Exception e) {
            System.err.println("Deserialization FAILED for line:");
            System.err.println(jsonLine49998);
            e.printStackTrace();
        }
    }
}

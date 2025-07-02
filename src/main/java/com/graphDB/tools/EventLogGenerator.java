package com.graphDB.tools;

import com.graphDB.core.CausalLedger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public class EventLogGenerator {

    private static final String TEST_LOG_PATH = "data_large/events.log";
    private static final int NUM_ENTITIES = 100;
    private static final int EVENTS_PER_ENTITY_CHAIN = 500; // 100 * 500 = 50,000 events

    public static void main(String[] args) {
        System.out.println("Generating large event log at: " + TEST_LOG_PATH);

        try {
            Path path = Paths.get(TEST_LOG_PATH);
            if (Files.exists(path)) Files.delete(path); // Clean previous
            Path parentDir = path.getParent();
            if (parentDir != null) Files.createDirectories(parentDir);

            // Delete old .idx files in that directory too
            Files.deleteIfExists(parentDir.resolve("entity_to_event_ids.idx"));
            Files.deleteIfExists(parentDir.resolve("children_adjacency.idx"));
            Files.deleteIfExists(parentDir.resolve("event_to_graph_id.idx"));
            Files.deleteIfExists(parentDir.resolve("graph_to_event_id.idx"));
            Files.deleteIfExists(parentDir.resolve("event_type_to_event_ids.idx"));
            Files.deleteIfExists(parentDir.resolve("trace_id_to_event_ids.idx"));

        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        CausalLedger ledger = new CausalLedger(TEST_LOG_PATH, NUM_ENTITIES * EVENTS_PER_ENTITY_CHAIN);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_ENTITIES; i++) {
            String entityId = "entity-" + i; // this becomes traceId
            String lastEventIdInChain = null;

            for (int j = 0; j < EVENTS_PER_ENTITY_CHAIN; j++) {
                String eventType = (j == 0) ? "ENTITY_CREATED" : "ENTITY_UPDATED_" + j;

                try {
                    String newEventId = ledger.ingestEvent(
                            entityId,                    // traceId
                            "EntityService",             // serviceName
                            "1.0.0",                     // serviceVersion
                            "localhost",                 // hostname
                            eventType,                   // eventType
                            Map.of("value", j, "entitySuffix", i, "timestamp", Instant.now().toString()), // payload
                            Collections.emptyList()      // manualParentEventIds - EMPTY to test automatic linking
                    );

                    lastEventIdInChain = newEventId;

                    if ((j + 1) % 1000 == 0) { // Log progress
                        System.out.println("Entity " + i + ": Ingested " + (j + 1) + " events. Last ID: " + lastEventIdInChain);
                    }
                } catch (Exception e) {
                    System.err.println("Error ingesting event for entity " + entityId + ": " + e.getMessage());
                    e.printStackTrace();
                    // Continue processing other events
                }
            }

            if ((i + 1) % 10 == 0) {
                System.out.println("Completed generation for " + (i + 1) + " entities.");
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Finished generating events. Total time: " + (endTime - startTime) + " ms");
        System.out.println("Final Ledger Stats: " + ledger.getStats());

        System.out.println("Generator finished. Exiting to allow shutdown hook to save indexes.");
    }
}

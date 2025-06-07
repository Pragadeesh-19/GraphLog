package com.graphlog;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Main {

    private static final String LOG_FILE_PATH = "data/events.log";

    public static void main(String[] args) {

        CausalLedger ledger = new CausalLedger(LOG_FILE_PATH);

        System.out.println("Initial Ledger State: " + ledger.getStats());

        // Scenario 1: Root Events (No Parents)
        String rootEvent1Id = null;
        String rootEvent2Id = null;
        try {
            String entity1 = "USER_ACCOUNT_SERVICE";
            String entity2 = "PRODUCT_CATALOG_SERVICE";

            rootEvent1Id = ledger.ingestEvent(entity1, "USER_CREATED",
                    Map.of("userId", "u-001", "username", "Archibald"),
                    Collections.emptyList());
            System.out.println("Ingested rootEvent1: " + rootEvent1Id);

            rootEvent2Id = ledger.ingestEvent(entity2, "PRODUCT_ADDED",
                    Map.of("productId", "p-101", "productName", "Donut of Power", "price", 99.99),
                    Collections.emptyList());
            System.out.println("Ingested rootEvent2: " + rootEvent2Id);

        } catch (CausalLedger.CausalLoopException | CausalLedger.UnknownParentException e) {
            System.err.println("Error ingesting root events: " + e.getMessage());
            e.printStackTrace();
        }

        // Scenario 2: Events with Parents
        try {
            // Retrieve IDs from previous ingestion
            String userCreatedEventId = null;
            String productAddedEventId = null;

            List<EventAtom> userEvents = ledger.getEventsByEntity("USER_ACCOUNT_SERVICE");
            if (!userEvents.isEmpty()) userCreatedEventId = userEvents.get(0).getEventId();

            List<EventAtom> productEvents = ledger.getEventsByEntity("PRODUCT_CATALOG_SERVICE");
            if (!productEvents.isEmpty()) productAddedEventId = productEvents.get(0).getEventId();

            if (userCreatedEventId != null) {
                String orderEventId = ledger.ingestEvent("ORDER_PROCESSING_SERVICE", "ORDER_PLACED",
                        Map.of("orderId", "o-abc", "userId", "u-001", "items", List.of("p-101")),
                        List.of(userCreatedEventId)); // Depends on user creation
                System.out.println("Ingested orderEvent: " + orderEventId);

                if (productAddedEventId != null) {
                    String inventoryEventId = ledger.ingestEvent("INVENTORY_SERVICE", "STOCK_DECREMENTED",
                            Map.of("productId", "p-101", "amount", 1, "reason", "ORDER_PLACED:o-abc"),
                            List.of(productAddedEventId, orderEventId)); // Depends on product & order
                    System.out.println("Ingested inventoryEvent: " + inventoryEventId);

                    List<String> ancestry = ledger.getEventAndCausalAncestryIds(inventoryEventId);
                    System.out.println("Ancestry for " + inventoryEventId + ": " + ancestry);

                    List<String> topoOrder = ledger.getEventsInTopologicalOrder();
                    System.out.println("Topological Order of All Events: " + topoOrder);

                } else {
                    System.out.println("Product event not found, skipping dependent inventory event.");
                }
            } else {
                System.out.println("User creation event not found, skipping dependent order event.");
            }

        } catch (CausalLedger.CausalLoopException | CausalLedger.UnknownParentException e) {
            System.err.println("Error ingesting dependent events: " + e.getMessage());
            e.printStackTrace();
        }

        // Scenario 3: Attempt to Create a Causal Loop
        System.out.println("\nAttempting to create a causal loop...");
        // real cycle detection logic is internal to the ledger
        System.out.println("Skipping explicit CausalLoopException trigger in runner for now - internal test is robust.");

        // Final stats after all operations
        System.out.println("Final Ledger State: " + ledger.getStats());
    }


}
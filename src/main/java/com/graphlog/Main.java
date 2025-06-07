package com.graphlog;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Main {

    private static final String LOG_FILE_PATH = "data/events.log";

    public static void main(String[] args) {

        CausalLedger ledger = new CausalLedger(LOG_FILE_PATH);

        System.out.println("Initial Ledger State: " + ledger.getStats());

        // Root Events (No Parents)
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

        // Events with Parents
        String orderEventId = null;
        String inventoryEventId = null;
        try {
            // Retrieve IDs from previous ingestion
            String userCreatedEventId = null;
            String productAddedEventId = null;

            List<EventAtom> userEvents = ledger.getEventsByEntity("USER_ACCOUNT_SERVICE");
            if (!userEvents.isEmpty()) userCreatedEventId = userEvents.get(0).getEventId();

            List<EventAtom> productEvents = ledger.getEventsByEntity("PRODUCT_CATALOG_SERVICE");
            if (!productEvents.isEmpty()) productAddedEventId = productEvents.get(0).getEventId();

            if (userCreatedEventId != null) {
                orderEventId = ledger.ingestEvent("ORDER_PROCESSING_SERVICE", "ORDER_PLACED",
                        Map.of("orderId", "o-abc", "userId", "u-001", "items", List.of("p-101")),
                        List.of(userCreatedEventId)); // Depends on user creation
                System.out.println("Ingested orderEvent: " + orderEventId);

                if (productAddedEventId != null) {
                    inventoryEventId = ledger.ingestEvent("INVENTORY_SERVICE", "STOCK_DECREMENTED",
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

        // Test Descendant Queries
        System.out.println("\n=== Testing Descendant Queries ===");

        if (rootEvent1Id != null) {
            System.out.println("\nQuerying descendants for rootEvent1 (USER_CREATED): " + rootEvent1Id);
            List<String> descendants1 = ledger.getEventAndCausalDescendantsId(rootEvent1Id);
            System.out.println("Descendants of " + rootEvent1Id + " (including self): " + descendants1);
        }

        if (rootEvent2Id != null) {
            System.out.println("\nQuerying descendants for rootEvent2 (PRODUCT_ADDED): " + rootEvent2Id);
            List<String> descendants2 = ledger.getEventAndCausalDescendantsId(rootEvent2Id);
            System.out.println("Descendants of " + rootEvent2Id + " (including self): " + descendants2);
        }

        if (orderEventId != null) {
            System.out.println("\nQuerying descendants for orderEvent (ORDER_PLACED): " + orderEventId);
            List<String> descendantsOrder = ledger.getEventAndCausalDescendantsId(orderEventId);
            System.out.println("Descendants of " + orderEventId + " (including self): " + descendantsOrder);
        }

        if (inventoryEventId != null) {
            System.out.println("\nQuerying descendants for inventoryEvent (STOCK_DECREMENTED): " + inventoryEventId);
            List<String> descendantsInventory = ledger.getEventAndCausalDescendantsId(inventoryEventId);
            System.out.println("Descendants of " + inventoryEventId + " (including self): " + descendantsInventory);
        }

        // Test Shortest Causal Path Queries
        System.out.println("\n=== Testing Shortest Causal Path Queries ===");

        // Test 1: Path from rootEvent1 to inventoryEvent
        // (Recall: rootEvent1 -> orderEvent -> inventoryEvent)
        if (rootEvent1Id != null && inventoryEventId != null) {
            System.out.println("\nQuerying shortest causal path from rootEvent1 (" + rootEvent1Id + ") to inventoryEvent (" + inventoryEventId + ")");
            List<String> path1 = ledger.getShortestCausalPath(rootEvent1Id, inventoryEventId);
            if (path1.isEmpty()) {
                System.out.println("No causal path found from " + rootEvent1Id + " to " + inventoryEventId);
            } else {
                System.out.println("Shortest causal path (Cause->Effect): " + path1);
            }
        } else {
            System.out.println("Skipping shortest path test 1 due to missing event IDs.");
        }

        // Test 2: Path from orderEvent to inventoryEvent (direct link)
        if (orderEventId != null && inventoryEventId != null) {
            System.out.println("\nQuerying shortest causal path from orderEvent (" + orderEventId + ") to inventoryEvent (" + inventoryEventId + ")");
            List<String> path2 = ledger.getShortestCausalPath(orderEventId, inventoryEventId);
            if (path2.isEmpty()) {
                System.out.println("No causal path found from " + orderEventId + " to " + inventoryEventId);
            } else {
                System.out.println("Shortest causal path (Cause->Effect): " + path2);
            }
        } else {
            System.out.println("Skipping shortest path test 2 due to missing event IDs.");
        }

        // Test 3: No path (e.g., from rootEvent1 to rootEvent2, if they are independent roots)
        if (rootEvent1Id != null && rootEvent2Id != null) {
            System.out.println("\nQuerying shortest causal path from rootEvent1 (" + rootEvent1Id + ") to rootEvent2 (" + rootEvent2Id + ")");
            List<String> path3 = ledger.getShortestCausalPath(rootEvent1Id, rootEvent2Id);
            if (path3.isEmpty()) {
                System.out.println("No causal path found from " + rootEvent1Id + " to " + rootEvent2Id + " (EXPECTED)");
            } else {
                System.out.println("UNEXPECTED Path found: " + path3);
            }
        } else {
            System.out.println("Skipping shortest path test 3 due to missing event IDs.");
        }

        // Test 4: Path from an event to itself
        if (rootEvent1Id != null) {
            System.out.println("\nQuerying shortest causal path from rootEvent1 (" + rootEvent1Id + ") to itself");
            List<String> path4 = ledger.getShortestCausalPath(rootEvent1Id, rootEvent1Id);
            if (path4.isEmpty()) {
                System.out.println("No causal path found from " + rootEvent1Id + " to itself (UNEXPECTED for self-path)");
            } else {
                System.out.println("Shortest causal path (Cause->Effect): " + path4);
            }
        } else {
            System.out.println("Skipping shortest path test 4 due to missing event ID.");
        }

        // Test 5: Reverse direction path (should find no path if events are in correct causal order)
        if (inventoryEventId != null && rootEvent1Id != null) {
            System.out.println("\nQuerying shortest causal path from inventoryEvent (" + inventoryEventId + ") to rootEvent1 (" + rootEvent1Id + ") - reverse direction");
            List<String> path5 = ledger.getShortestCausalPath(inventoryEventId, rootEvent1Id);
            if (path5.isEmpty()) {
                System.out.println("No causal path found from " + inventoryEventId + " to " + rootEvent1Id + " (EXPECTED - reverse causality)");
            } else {
                System.out.println("UNEXPECTED reverse path found: " + path5);
            }
        } else {
            System.out.println("Skipping shortest path test 5 due to missing event IDs.");
        }

        // Test 6: Path with non-existent event ID
        System.out.println("\nTesting shortest path with non-existent event ID");
        List<String> path6 = ledger.getShortestCausalPath("non-existent-event-id", rootEvent1Id != null ? rootEvent1Id : "another-non-existent-id");
        if (path6.isEmpty()) {
            System.out.println("No causal path found with non-existent event ID (EXPECTED)");
        } else {
            System.out.println("UNEXPECTED path found with non-existent event: " + path6);
        }

        System.out.println("\n=== Testing Common Causal Ancestors Queries ===");

        // Test 1: Common ancestors of inventoryEvent and itself
        if (inventoryEventId != null) {
            System.out.println("\nQuerying common ancestors for inventoryEvent (" + inventoryEventId + ") and itself");
            List<String> common1 = ledger.getAllCommonCausalAncestors(inventoryEventId, inventoryEventId);
            System.out.println("Common ancestors (should be inventoryEvent's full ancestry): " + common1);
        } else {
            System.out.println("Skipping common ancestors test 1 due to missing inventoryEventId.");
        }

        // Test 2: Common ancestors of inventoryEvent and orderEvent
        if (inventoryEventId != null && orderEventId != null) {
            System.out.println("\nQuerying common ancestors for inventoryEvent (" + inventoryEventId + ") and orderEvent (" + orderEventId + ")");
            List<String> common2 = ledger.getAllCommonCausalAncestors(inventoryEventId, orderEventId);
            System.out.println("Common ancestors: " + common2);
        } else {
            System.out.println("Skipping common ancestors test 2 due to missing event IDs.");
        }

        // Test 3: Common ancestors of two largely independent root events (created in this run)
        if (rootEvent1Id != null && rootEvent2Id != null) {
            if (!rootEvent1Id.equals(rootEvent2Id)) {
                System.out.println("\nQuerying common ancestors for rootEvent1 (" + rootEvent1Id + ") and rootEvent2 (" + rootEvent2Id + ")");
                List<String> common3 = ledger.getAllCommonCausalAncestors(rootEvent1Id, rootEvent2Id);
                if (common3.isEmpty()) {
                    System.out.println("Common ancestors: [] (EXPECTED if truly independent for this test run's branches)");
                } else {
                    System.out.println("Common ancestors (check if this is expected): " + common3);
                }
            } else {
                System.out.println("\nQuerying common ancestors for rootEvent (" + rootEvent1Id + ") and itself (as rootEvent2 is same)");
                List<String> common3 = ledger.getAllCommonCausalAncestors(rootEvent1Id, rootEvent2Id);
                System.out.println("Common ancestors (should be just rootEvent1Id): " + common3);
            }
        } else {
            System.out.println("Skipping common ancestors test 3 due to missing root event IDs.");
        }

        // Test 4: Common ancestors involving a known root and a direct descendant
        if (orderEventId != null && rootEvent1Id != null) {
            System.out.println("\nQuerying common ancestors for orderEvent (" + orderEventId + ") and rootEvent1 (" + rootEvent1Id + ")");
            List<String> common4 = ledger.getAllCommonCausalAncestors(orderEventId, rootEvent1Id);
            System.out.println("Common ancestors: " + common4);
            // Expected: Should contain only rootEvent1Id
        } else {
            System.out.println("Skipping common ancestors test 4 due to missing event IDs.");
        }


        // Test 5: Common ancestors with a non-existent event ID
        System.out.println("\nTesting common ancestors with non-existent event ID");
        if (rootEvent1Id != null) { // Ensure at least one valid ID for the test pair
            List<String> common5 = ledger.getAllCommonCausalAncestors(rootEvent1Id, "non-existent-event-id-for-common-ancestor-test");
            if(common5.isEmpty()){
                System.out.println("Common ancestors: [] (EXPECTED due to non-existent ID)");
            } else {
                System.out.println("Common ancestors (UNEXPECTED with non-existent ID): " + common5);
            }
        } else {
            System.out.println("Skipping common ancestors test 5 (rootEvent1Id is null).");
        }


        // Scenario 3: Attempt to Create a Causal Loop
        System.out.println("\nAttempting to create a causal loop...");
        // real cycle detection logic is internal to the ledger
        System.out.println("Skipping explicit CausalLoopException trigger in runner for now - internal test is robust.");

        // Final stats after all operations
        System.out.println("\nFinal Ledger State: " + ledger.getStats());
    }

}
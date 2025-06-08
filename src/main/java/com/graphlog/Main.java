package com.graphlog;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Main {

    private static final String LOG_FILE_PATH = "data/events.log";

    public static void main(String[] args) {

        CausalLedger ledger = new CausalLedger(LOG_FILE_PATH);
        System.out.println("Initial Ledger State: " + ledger.getStats());

        String userEntityService = "USER_ACCOUNT_SERVICE";
        String productEntityService = "PRODUCT_CATALOG_SERVICE";
        String orderEntityService = "ORDER_PROCESSING_SERVICE";

        String userId1 = "u-001";
        String productId1 = "p-101";
        String orderId1 = "o-abc-123";

        String userCreatedEventId = null;
        String productAddedEventId = null;
        String orderCreatedEventId = null;
        String userRenamedEventId = null;
        String stockIncrementedEventId = null;
        String orderConfirmedEventId = null;
        String userDeactivatedEventId = null;


        try {
            System.out.println("\n--- Phase 1: Initial Event Ingestion ---");
            userCreatedEventId = ledger.ingestEvent(userEntityService, "USER_CREATED",
                    Map.of("userId", userId1, "username", "Archibald", "timestamp", java.time.Instant.now().toString()), // Added timestamp to payload for handlers
                    Collections.emptyList());
            System.out.println("Ingested USER_CREATED: " + userCreatedEventId);

            productAddedEventId = ledger.ingestEvent(productEntityService, "PRODUCT_ADDED",
                    Map.of("productId", productId1, "productName", "Quantum Donut", "price", 199.99, "stock", 10, "timestamp", java.time.Instant.now().toString()),
                    Collections.emptyList());
            System.out.println("Ingested PRODUCT_ADDED: " + productAddedEventId);

            System.out.println("\n--- Phase 2: Dependent and Modifying Events ---");
            if (userCreatedEventId != null) {
                userRenamedEventId = ledger.ingestEvent(userEntityService, "USER_RENAMED",
                        Map.of("userId", userId1, "newUsername", "ArchieBold v2", "timestamp", java.time.Instant.now().toString()),
                        List.of(userCreatedEventId));
                System.out.println("Ingested USER_RENAMED: " + userRenamedEventId);
            }

            if (productAddedEventId != null) {
                stockIncrementedEventId = ledger.ingestEvent(productEntityService, "STOCK_INCREMENTED",
                        Map.of("productId", productId1, "amount", 5, "timestamp", java.time.Instant.now().toString()),
                        List.of(productAddedEventId));
                System.out.println("Ingested STOCK_INCREMENTED: " + stockIncrementedEventId);
            }

            if (userCreatedEventId != null && productAddedEventId !=null) {
                orderCreatedEventId = ledger.ingestEvent(orderEntityService, "ORDER_CREATED",
                        Map.of("orderId", orderId1, "userId", userId1, "items", List.of(Map.of("productId", productId1, "quantity", 2)), "totalAmount", 399.98, "timestamp", java.time.Instant.now().toString()),
                        List.of(userCreatedEventId, productAddedEventId)); // Order depends on user existing and product existing
                System.out.println("Ingested ORDER_CREATED: " + orderCreatedEventId);

                if (orderCreatedEventId != null) {
                    orderConfirmedEventId = ledger.ingestEvent(orderEntityService, "ORDER_CONFIRMED",
                            Map.of("orderId", orderId1, "confirmationMethod", "AUTO", "timestamp", java.time.Instant.now().toString()),
                            List.of(orderCreatedEventId));
                    System.out.println("Ingested ORDER_CONFIRMED: " + orderConfirmedEventId);
                }
            }

            if (userRenamedEventId != null) {
                userDeactivatedEventId = ledger.ingestEvent(userEntityService, "USER_DEACTIVATED",
                        Map.of("userId", userId1, "reason", "Test Deactivation", "timestamp", java.time.Instant.now().toString()),
                        List.of(userRenamedEventId));
                System.out.println("Ingested USER_DEACTIVATED: " + userDeactivatedEventId);
            }

        } catch (CausalLedger.CausalLoopException | CausalLedger.UnknownParentException e) {
            System.err.println("Error during event ingestion phase: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n--- Phase 3: State Projection Tests ---");

        // Project User State
        System.out.println("\n--- Projecting User Account State (" + userEntityService + " for " + userId1 + ") ---");
        Map<String, Object> userState = ledger.getCurrentStateForEntity(userEntityService);
        System.out.println("Final User State in Main: " + userState);

        // Project Product State
        System.out.println("\n--- Projecting Product Catalog State (" + productEntityService + " for " + productId1 + ") ---");
        Map<String, Object> productState = ledger.getCurrentStateForEntity(productEntityService);
        System.out.println("Final Product State in Main: " + productState);

        // Project Order State
        System.out.println("\n--- Projecting Order Processing State (" + orderEntityService + " for " + orderId1 + ") ---");
        Map<String, Object> orderState = ledger.getCurrentStateForEntity(orderEntityService);
        System.out.println("Final Order State in Main: " + orderState);

        // Test getEntityStateUpToEvent
        System.out.println("\n--- Testing Historical State Projection (Time Travel) ---");
        if (userRenamedEventId != null) {
            System.out.println("\nUser state up to USER_RENAMED event (" + userRenamedEventId + "):");
            Map<String, Object> userStateAtRename = ledger.getEntityStateUpToEvent(userEntityService, userRenamedEventId);
            System.out.println("User State at Rename in Main: " + userStateAtRename);
            // Expected: username=ArchieBold v2, isActive=true, version=2
        }
        if (orderCreatedEventId != null) {
            System.out.println("\nOrder state up to ORDER_CREATED event (" + orderCreatedEventId + "):");
            Map<String, Object> orderStateAtCreation = ledger.getEntityStateUpToEvent(orderEntityService, orderCreatedEventId);
            System.out.println("Order State at Creation in Main: " + orderStateAtCreation);
            // Expected: status=CREATED, version=1
        }

        System.out.println("\n--- Running Other Standard Queries ---");
        if (orderConfirmedEventId != null) { // Use a fairly dependent event
            System.out.println("\nAncestry for ORDER_CONFIRMED (" + orderConfirmedEventId + "):");
            List<String> ancestry = ledger.getEventAndCausalAncestryIds(orderConfirmedEventId);
            System.out.println(ancestry);

            System.out.println("\nDescendants of USER_CREATED (" + (userCreatedEventId != null ? userCreatedEventId : "N/A") + ")");
            if (userCreatedEventId != null) System.out.println(ledger.getEventAndCausalDescendantsId(userCreatedEventId));
        }

        System.out.println("\nTopological Order of All Events:");
        List<String> topoOrder = ledger.getEventsInTopologicalOrder();
        System.out.println(topoOrder);

        System.out.println("\nFinal Ledger Stats: " + ledger.getStats());
    }

}
package com.graphDB.tools;

import com.graphDB.core.CausalLedger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class StartupProfiler {

    private static final String TEST_LOG_PATH = "data_large/events.log"; // Same as generator
    private static final String[] INDEX_FILES = {
            "entity_to_event_ids.idx", "children_adjacency.idx",
            "event_to_graph_id.idx", "graph_to_event_id.idx",
            "event_type_to_event_ids.idx", "trace_id_to_event_ids.idx"
    };


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java StartupProfiler [cold|warm]");
            System.exit(1);
        }
        switch (args[0].toLowerCase()) {
            case "cold":
                runColdStartTest();
                break;
            case "warm":
                runWarmStartTest();
                break;
            default:
                System.out.println("Invalid argument. Use 'cold' or 'warm'.");
                System.exit(1);
        }
    }

    private static void runColdStartTest() {
        Path dataDir = Paths.get(TEST_LOG_PATH).getParent();
        deleteIndexes(dataDir);

        System.out.println("--- Profiling Cold Start (Rebuilding Indexes) ---");
        long start = System.nanoTime();
        CausalLedger ledger = new CausalLedger(TEST_LOG_PATH);
        long durationMs = (System.nanoTime() - start) / 1_000_000;

        System.out.println("Cold Start Time: " + durationMs + " ms");
        System.out.println("Cold Start Stats: " + ledger.getStats());

        // Force immediate index persistence
        try {
            var flush = CausalLedger.class.getDeclaredMethod("saveAllIndexes");
            flush.setAccessible(true);
            flush.invoke(ledger);
            System.out.println("Indexes flushed to disk.");
        } catch (Exception e) {
            System.err.println("Error flushing indexes: " + e.getMessage());
        }
    }

    private static void runWarmStartTest() {
        Path dataDir = Paths.get(TEST_LOG_PATH).getParent();

        System.out.println("--- Profiling Warm Start (Loading Persisted Indexes) ---");
        if (!Files.exists(Paths.get(TEST_LOG_PATH))) {
            System.err.println("Event log missing. Generate events before warm test.");
            return;
        }

        boolean allExist = Stream.of(INDEX_FILES)
                .allMatch(f -> Files.exists(dataDir.resolve(f)));
        if (!allExist) {
            System.err.println("One or more index files missing. Run cold start first.");
            return;
        }

        long start = System.nanoTime();
        CausalLedger ledger = new CausalLedger(TEST_LOG_PATH);
        long durationMs = (System.nanoTime() - start) / 1_000_000;

        System.out.println("Warm Start Time: " + durationMs + " ms");
        System.out.println("Warm Start Stats: " + ledger.getStats());

        System.out.println("Attach profiler now to inspect 'ledger' heap usage.");
        System.out.println("Press Enter to exit and save indexes...");
        try { System.in.read(); } catch (IOException ignored) {}
    }

    private static void deleteIndexes(Path dataDir) {
        System.out.println("Deleting existing index files (if any)...");
        for (String filename : INDEX_FILES) {
            try {
                boolean deleted = Files.deleteIfExists(dataDir.resolve(filename));
                if (deleted) {
                    System.out.println("  Deleted: " + filename);
                }
            } catch (IOException e) {
                System.err.println("Could not delete index " + filename + ": " + e.getMessage());
            }
        }
    }
}

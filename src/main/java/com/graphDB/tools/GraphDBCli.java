package com.graphDB.tools;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import picocli.CommandLine;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "graphdb-cli", mixinStandardHelpOptions = true, version = "GraphDB CLI 1.0",
                    description = "Command line Interrogator for GraphDB causal ledger", subcommands = {GraphDBCli.Ingest.class, GraphDBCli.Query.class})
public class GraphDBCli implements Callable<Integer> {

    @CommandLine.Option(names = {"-a", "--api-url"}, description = "Base URL of the GraphDB API.", defaultValue = "http://localhost:8080/api/v1")
    private static String apiBaseUrl;

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EventAtomDTO {
        public String eventId;
        public String nodeId;
        public String serviceName;
        public String traceId;
        public String serviceVersion;
        public String hostname;
        public String eventType;
        public Map<String, Object> payload;
        public List<String> causalParentEventIds;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GraphNodeDTO {
        public String id;
        public String label;
        public String title;
        public String group;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GraphEdgeDTO {
        public String from;
        public String to;
        public String arrows;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GraphDataDTO {
        public List<GraphNodeDTO> nodes;
        public List<GraphEdgeDTO> edges;
    }

    @Override
    public Integer call() {
        System.err.println("No command specified. Use 'ingest' or 'query'. See --help for details.");
        return 1;
    }

    @CommandLine.Command(name = "ingest", mixinStandardHelpOptions = true, description = "Ingest a new event into the ledger.")
    static class Ingest implements Callable<Integer> {
        @CommandLine.ParentCommand
        private GraphDBCli parent;

        @CommandLine.Option(names = {"-t", "--trace-id"}, required = true, description = "Trace ID for this event flow.")
        private String traceId;

        @CommandLine.Option(names = {"-s", "--service-name"}, required = true, description = "Name of the emitting service.")
        private String serviceName;

        @CommandLine.Parameters(index="0", description="The event type (e.g., USER_CREATED).")
        private String eventType;

        @CommandLine.Option(names = {"-p", "--payload"}, description = "Event payload in JSON format.", defaultValue = "{}")
        private String payloadJson;

        @CommandLine.Option(names = {"-v", "--service-version"}, description = "Service version.", defaultValue = "1.0.0")
        private String serviceVersion;

        @CommandLine.Option(names = {"-host", "--hostname"}, description = "Hostname.", defaultValue = "localhost")
        private String hostname;

        @CommandLine.Option(names = {"--parents"}, description = "Comma-separated list of parent event IDs.", defaultValue = "")
        private String parentEventIds;

        @Override
        public Integer call() throws Exception {
            String ingestUrl = parent.apiBaseUrl + "/events";

            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("traceId", traceId);
            bodyMap.put("serviceName", serviceName);
            bodyMap.put("eventType", eventType);
            bodyMap.put("serviceVersion", serviceVersion);
            bodyMap.put("hostname", hostname);

            try {
                bodyMap.put("payload", OBJECT_MAPPER.readValue(payloadJson, new TypeReference<Map<String,Object>>(){}));
            } catch (Exception e) {
                System.err.println("Invalid JSON payload: " + e.getMessage());
                return 1;
            }

            if (!parentEventIds.trim().isEmpty()) {
                List<String> parents = Arrays.stream(parentEventIds.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
                bodyMap.put("causalParentEventIds", parents);
            }

            String requestBody = OBJECT_MAPPER.writeValueAsString(bodyMap);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(ingestUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            System.out.println("Status Code: " + response.statusCode());
            System.out.println("Response:");

            if (response.statusCode() == 200 || response.statusCode() == 201) {
                try {
                    Object jsonResponse = OBJECT_MAPPER.readValue(response.body(), Object.class);
                    String prettyJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonResponse);
                    System.out.println(prettyJson);
                } catch (Exception e) {
                    System.out.println(response.body());
                }
            } else {
                System.out.println(response.body());
            }

            return response.statusCode() == 201 || response.statusCode() == 200 ? 0 : 1;
        }
    }

    @CommandLine.Command(name = "query", description = "Query the causal graph.", mixinStandardHelpOptions = true,
            subcommands = {GraphDBCli.Query.Ancestors.class, GraphDBCli.Query.Descendants.class})
    static class Query implements Callable<Integer> {
        @Override
        public Integer call() {
            System.err.println("No query subcommand specified. Use 'ancestors' or 'descendants'. See --help for details.");
            return 1;
        }

        @CommandLine.Command(name = "ancestors", mixinStandardHelpOptions = true, description = "Get and display the causal ancestry of an event.")
        static class Ancestors implements Callable<Integer> {
            @CommandLine.ParentCommand
            private Query queryParent;

            @CommandLine.Parameters(index = "0", description = "The Event ID whose ancestry to trace.")
            private String targetEventId;

            @CommandLine.Option(names = {"-f", "--format"}, description = "Output format: tree, json, table", defaultValue = "tree")
            private String format;

            @Override
            public Integer call() throws Exception {
                String apiUrl = apiBaseUrl + "/graph/event/" + targetEventId + "/ancestors/json";
                HttpRequest request = HttpRequest.newBuilder().uri(new URI(apiUrl)).GET().build();
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    System.err.println("API Error: " + response.statusCode());
                    System.err.println(response.body());
                    return 1;
                }

                GraphDataDTO graphData = OBJECT_MAPPER.readValue(response.body(), GraphDataDTO.class);

                switch (format.toLowerCase()) {
                    case "json":
                        printJsonFormat(graphData);
                        break;
                    case "table":
                        printTableFormat(graphData);
                        break;
                    case "tree":
                    default:
                        printTreeFormat(graphData, targetEventId);
                        break;
                }

                return 0;
            }

            private GraphDBCli getRootCli() {
                Object current = queryParent;
                while (current != null) {
                    if (current instanceof GraphDBCli) {
                        return (GraphDBCli) current;
                    }

                    try {
                        Field parentField = current.getClass().getDeclaredField("parent");
                        parentField.setAccessible(true);
                        current = parentField.get(current);
                    } catch (Exception e) {
                        break;
                    }
                }

                GraphDBCli cli = new GraphDBCli();
                apiBaseUrl = "http://localhost:8080/api/v1";
                return cli;
            }

            private void printJsonFormat(GraphDataDTO graphData) throws Exception {
                String prettyJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(graphData);
                System.out.println(prettyJson);
            }

            private void printTableFormat(GraphDataDTO graphData) {
                System.out.println("\n=== NODES ===");
                System.out.printf("%-10s %-20s %-15s %-10s%n", "ID", "Label", "Title", "Group");
                System.out.println("-".repeat(60));
                for (GraphNodeDTO node : graphData.nodes) {
                    System.out.printf("%-10s %-20s %-15s %-10s%n",
                            truncate(node.id, 10),
                            truncate(node.label, 20),
                            truncate(node.title, 15),
                            truncate(node.group, 10));
                }

                System.out.println("\n=== EDGES ===");
                System.out.printf("%-10s %-10s%n", "From", "To");
                System.out.println("-".repeat(25));
                for (GraphEdgeDTO edge : graphData.edges) {
                    System.out.printf("%-10s %-10s%n",
                            truncate(edge.from, 10),
                            truncate(edge.to, 10));
                }
            }

            private void printTreeFormat(GraphDataDTO graphData, String targetEventId) {
                Map<String, List<String>> parentToChildren = new HashMap<>();
                Map<String, String> nodeLabels = new HashMap<>();
                Set<String> childNodes = new HashSet<>();

                for (GraphNodeDTO node : graphData.nodes) {
                    parentToChildren.put(node.id, new ArrayList<>());
                    nodeLabels.put(node.id, node.label != null ? node.label : node.id);
                }

                for (GraphEdgeDTO edge : graphData.edges) {
                    parentToChildren.get(edge.from).add(edge.to);
                    childNodes.add(edge.to);
                }

                List<String> roots = new ArrayList<>();
                for (GraphNodeDTO node : graphData.nodes) {
                    if (!childNodes.contains(node.id)) {
                        roots.add(node.id);
                    }
                }

                System.out.println("\n=== Causal Ancestry Tree for Event " + truncate(targetEventId, 12) + "... ===");
                if (roots.isEmpty()) {
                    System.out.println("No root nodes found. Possible circular dependency or data issue.");
                    return;
                }

                for (String rootId : roots) {
                    printAsciiTree(rootId, "", true, parentToChildren, nodeLabels, targetEventId);
                }
            }

            private void printAsciiTree(String nodeId, String prefix, boolean isTail,
                                        Map<String, List<String>> tree, Map<String, String> labels, String targetId) {
                String marker = nodeId.equals(targetId) ? " <<< TARGET" : "";
                String label = labels.getOrDefault(nodeId, nodeId);
                System.out.println(prefix + (isTail ? "└── " : "├── ") + label + " (" + truncate(nodeId, 8) + "...)" + marker);

                List<String> children = tree.getOrDefault(nodeId, new ArrayList<>());
                for (int i = 0; i < children.size(); i++) {
                    printAsciiTree(children.get(i), prefix + (isTail ? "    " : "│   "),
                            i == children.size() - 1, tree, labels, targetId);
                }
            }

            private String truncate(String str, int maxLength) {
                if (str == null) return "";
                return str.length() > maxLength ? str.substring(0, maxLength) : str;
            }
        }

        @CommandLine.Command(name = "descendants", mixinStandardHelpOptions = true, description = "Get and display the causal descendants of an event.")
        static class Descendants implements Callable<Integer> {
            @CommandLine.ParentCommand
            private Query queryParent;

            @CommandLine.Parameters(index = "0", description = "The Event ID whose descendants to trace.")
            private String targetEventId;

            @CommandLine.Option(names = {"-f", "--format"}, description = "Output format: tree, json, table", defaultValue = "tree")
            private String format;

            @Override
            public Integer call() throws Exception {
                String apiUrl = apiBaseUrl + "/graph/event/" + targetEventId + "/descendants/json";
                HttpRequest request = HttpRequest.newBuilder().uri(new URI(apiUrl)).GET().build();
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() != 200) {
                    System.err.println("API Error: " + response.statusCode());
                    System.err.println(response.body());
                    return 1;
                }

                GraphDataDTO graphData = OBJECT_MAPPER.readValue(response.body(), GraphDataDTO.class);

                switch (format.toLowerCase()) {
                    case "json":
                        printJsonFormat(graphData);
                        break;
                    case "table":
                        printTableFormat(graphData);
                        break;
                    case "tree":
                    default:
                        printTreeFormat(graphData, targetEventId);
                        break;
                }

                return 0;
            }

            // Reuse the same printing methods as Ancestors
            private void printJsonFormat(GraphDataDTO graphData) throws Exception {
                String prettyJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(graphData);
                System.out.println(prettyJson);
            }

            private void printTableFormat(GraphDataDTO graphData) {
                System.out.println("\n=== NODES ===");
                System.out.printf("%-10s %-20s %-15s %-10s%n", "ID", "Label", "Title", "Group");
                System.out.println("-".repeat(60));
                for (GraphNodeDTO node : graphData.nodes) {
                    System.out.printf("%-10s %-20s %-15s %-10s%n",
                            truncate(node.id, 10),
                            truncate(node.label, 20),
                            truncate(node.title, 15),
                            truncate(node.group, 10));
                }

                System.out.println("\n=== EDGES ===");
                System.out.printf("%-10s %-10s%n", "From", "To");
                System.out.println("-".repeat(25));
                for (GraphEdgeDTO edge : graphData.edges) {
                    System.out.printf("%-10s %-10s%n",
                            truncate(edge.from, 10),
                            truncate(edge.to, 10));
                }
            }

            private void printTreeFormat(GraphDataDTO graphData, String targetEventId) {
                Map<String, List<String>> parentToChildren = new HashMap<>();
                Map<String, String> nodeLabels = new HashMap<>();
                Set<String> childNodes = new HashSet<>();

                for (GraphNodeDTO node : graphData.nodes) {
                    parentToChildren.put(node.id, new ArrayList<>());
                    nodeLabels.put(node.id, node.label != null ? node.label : node.id);
                }

                for (GraphEdgeDTO edge : graphData.edges) {
                    parentToChildren.get(edge.from).add(edge.to);
                    childNodes.add(edge.to);
                }

                System.out.println("\n=== Causal Descendants Tree for Event " + truncate(targetEventId, 12) + "... ===");
                printAsciiTree(targetEventId, "", true, parentToChildren, nodeLabels, targetEventId);
            }

            private void printAsciiTree(String nodeId, String prefix, boolean isTail,
                                        Map<String, List<String>> tree, Map<String, String> labels, String targetId) {
                String marker = nodeId.equals(targetId) ? " <<< TARGET" : "";
                String label = labels.getOrDefault(nodeId, nodeId);
                System.out.println(prefix + (isTail ? "└── " : "├── ") + label + " (" + truncate(nodeId, 8) + "...)" + marker);

                List<String> children = tree.getOrDefault(nodeId, new ArrayList<>());
                for (int i = 0; i < children.size(); i++) {
                    printAsciiTree(children.get(i), prefix + (isTail ? "    " : "│   "),
                            i == children.size() - 1, tree, labels, targetId);
                }
            }

            private String truncate(String str, int maxLength) {
                if (str == null) return "";
                return str.length() > maxLength ? str.substring(0, maxLength) : str;
            }
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new GraphDBCli()).execute(args);
        System.exit(exitCode);
    }
}

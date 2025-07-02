package com.graphDB.controller;

import com.graphDB.core.CausalLedger;
import com.graphDB.core.CausalRelationship;
import com.graphDB.core.EventAtom;
import com.graphDB.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.*;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class LedgerController {

    private final CausalLedger ledger;

    @PostMapping("/events")
    public ResponseEntity<?> ingestTraceEvent(@RequestBody IngestTraceEventRequest request) {
        try {
            if (request.traceId == null || request.traceId.trim().isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Field 'traceId' is required and cannot be empty.");
            }
            if (request.serviceName == null || request.serviceName.trim().isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Field 'serviceName' is required and cannot be empty.");
            }
            if (request.eventType == null || request.eventType.trim().isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Field 'eventType' is required and cannot be empty.");
            }

            String serviceVersion = request.serviceVersion != null ? request.serviceVersion : "unknown";
            String hostname = request.hostname != null ? request.hostname : "unknown";
            List<String> parents = request.manualParentEventIds != null ? request.manualParentEventIds : Collections.emptyList();
            Map<String, Object> payload = request.payload != null ? request.payload : Collections.emptyMap();

            String newEventId = ledger.ingestEvent(
                    request.traceId,
                    request.serviceName,
                    serviceVersion,
                    hostname,
                    request.eventType,
                    payload,
                    parents
            );

            EventAtom createdEvent = ledger.getEvent(newEventId);
            URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                    .path("/{id}")
                    .buildAndExpand(newEventId)
                    .toUri();

            return ResponseEntity.created(location).body(createdEvent);
        } catch (CausalLedger.UnknownParentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Unknown parent event: " + e.getMessage());
        } catch (CausalLedger.CausalLoopException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Causal loop detected: " + e.getMessage());
        } catch (IllegalArgumentException e){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid argument: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Internal server error: " + e.getMessage());
        }
    }

    @GetMapping("/traces/{traceId}")
    public ResponseEntity<List<EventAtom>> getEventsByTraceId(@PathVariable String traceId) {
        List<EventAtom> events = ledger.getEventsByTraceId(traceId);
        if (events.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Collections.emptyList());
        }
        return ResponseEntity.ok(events);
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<?> getEventById(@PathVariable String eventId) {
        EventAtom event = ledger.getEvent(eventId);
        if (event != null) {
            return ResponseEntity.ok(event);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Event not found: " + eventId);
        }
    }

    @GetMapping("/events/{eventId}/ancestors")
    public ResponseEntity<?> getAncestors(@PathVariable String eventId) {
        if (!ledger.containsEvent(eventId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Event not found: " + eventId);
        }
        List<String> ancestors = ledger.getEventAndCausalAncestryIds(eventId);
        return ResponseEntity.ok(ancestors);
    }

    @GetMapping("/events/{eventId}/descendants")
    public ResponseEntity<?> getDescendants(@PathVariable String eventId) {
        if (!ledger.containsEvent(eventId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Event not found: " + eventId);
        }
        List<String> descendants = ledger.getEventAndCausalDescendantsId(eventId);
        return ResponseEntity.ok(descendants);
    }

    @GetMapping("/events/topological")
    public ResponseEntity<List<String>> getTopologicalOrder() {
        List<String> topoOrder = ledger.getEventsInTopologicalOrder();
        return ResponseEntity.ok(topoOrder);
    }

    @GetMapping("/entities/{entityId}/state")
    public ResponseEntity<?> getEntityState(
            @PathVariable String entityId,
            @RequestParam(required = false) String upToEventId) {

        try {
            Map<String, Object> state;

            if (upToEventId != null && !upToEventId.isEmpty()) {
                if (!ledger.containsEvent(upToEventId)) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("upToEventId '" + upToEventId + "' not found.");
                }
                state = ledger.getEntityStateUpToEvent(entityId, upToEventId);
            } else {
                state = ledger.getCurrentStateForEntity(entityId);
            }

            if (state.isEmpty() && ledger.getEventsByEntity(entityId).isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No events or state found for entity: " + entityId);
            }

            return ResponseEntity.ok(state);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error projecting state: " + e.getMessage());
        }
    }

    @GetMapping("/ledger/stats")
    public ResponseEntity<String> getLedgerStats() {
        return ResponseEntity.ok(ledger.getStats());
    }

    @GetMapping("/graph/event/{eventId}/ancestors/json")
    public ResponseEntity<?> getAncestorGraphJson(@PathVariable String eventId) {
        try {
            if (!ledger.containsEvent(eventId)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event not found: " + eventId);
            }

            // 1. Get all ancestor event IDs (includes self)
            List<String> ancestorEventIds = ledger.getEventAndCausalAncestryIds(eventId);
            if (ancestorEventIds.isEmpty() && ledger.containsEvent(eventId)) {
                ancestorEventIds = List.of(eventId); // Handle root event case
            } else if (ancestorEventIds.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event or its ancestry not found for JSON graph: " + eventId);
            }

            Set<String> relevantEventIdsSet = new HashSet<>(ancestorEventIds);
            List<GraphNode> nodes = new ArrayList<>();
            List<GraphEdge> edges = new ArrayList<>();

            // 2. Populate Nodes
            for (String currentAncestorId : ancestorEventIds) {
                EventAtom event = ledger.getEvent(currentAncestorId);
                if (event != null) {
                    String shortId = event.getEventId().substring(0, Math.min(event.getEventId().length(), 8));
                    String label = event.getEventType(); // Or more complex label
                    String title = String.format("Type: %s\nEntity: %s\nID: %s", event.getEventType(), event.getServiceName(), event.getEventId());
                    String group = event.getEventType(); // Group by event type for Vis.js styling

                    nodes.add(new GraphNode(event.getEventId(), label, title, group));
                }
            }

            // 3. Populate Edges (CAUSE -> EFFECT representation)
            // For an ancestor graph, iterate through each ancestor. If its child is also an ancestor, draw edge.
            for (String currentCauseId : ancestorEventIds) { // currentCauseId is an ancestor (a CAUSE)
                EventAtom causeAtom = ledger.getEvent(currentCauseId);
                if (causeAtom == null) continue;

                // Who did this 'currentCauseId' directly cause THAT IS ALSO in the set of ancestors?
                // (This means currentCauseId caused an effect, which itself is an ancestor of the original query event)
                Integer causeGraphId = ledger.getGraphIdForEventId(currentCauseId);
                if (causeGraphId == null) continue;

                List<Integer> childrenGraphIds = ledger.getChildrenGraphIds(causeGraphId); // Get direct effects of currentCauseId

                for (Integer childGraphId : childrenGraphIds) {
                    String childEffectId = ledger.getEventIdForGraphId(childGraphId);
                    // Only add edge if the child (effect) is also in our ancestor subgraph context
                    if (childEffectId != null && relevantEventIdsSet.contains(childEffectId)) {
                        edges.add(new GraphEdge(currentCauseId, childEffectId)); // CAUSE -> EFFECT
                    }
                }
            }

            GraphData graphData = new GraphData(nodes, edges);
            return ResponseEntity.ok(graphData);

        } catch (Exception e) {
            e.printStackTrace(); // Log server-side for debugging
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error generating JSON graph: " + e.getMessage());
        }
    }

    @GetMapping("/graph/event/{eventId}/descendants/json")
    public ResponseEntity<?> getDescendantGraphJson(@PathVariable String eventId) {
        try {
            if (!ledger.containsEvent(eventId)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event not found: " + eventId);
            }

            // 1. Get all descendant event IDs (includes self)
            List<String> descendantEventIds = ledger.getEventAndCausalDescendantsId(eventId); // Assuming plural method name from earlier correction
            if (descendantEventIds.isEmpty() && ledger.containsEvent(eventId)) {
                descendantEventIds = List.of(eventId); // Handle leaf event case
            } else if (descendantEventIds.isEmpty()){
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event or its descendants not found for JSON graph: " + eventId);
            }

            Set<String> relevantEventIdsSet = new HashSet<>(descendantEventIds);
            List<GraphNode> nodes = new ArrayList<>();
            List<GraphEdge> edges = new ArrayList<>();

            // 2. Populate Nodes
            for (String currentDescendantId : descendantEventIds) {
                EventAtom event = ledger.getEvent(currentDescendantId);
                if (event != null) {
                    String shortId = event.getEventId().substring(0, Math.min(event.getEventId().length(), 8));
                    String label = event.getEventType();
                    String title = String.format("Type: %s\nEntity: %s\nID: %s", event.getEventType(), event.getServiceName(), event.getEventId());
                    String group = event.getEventType();

                    nodes.add(new GraphNode(event.getEventId(), label, title, group));
                }
            }

            // 3. Populate Edges (CAUSE -> EFFECT representation)
            // For a descendant graph, iterate through each descendant (which can be a cause).
            // Find its direct children that are also in the descendant set.
            for (String currentCauseId : descendantEventIds) { // currentCauseId is one of the events in the descendant subgraph
                EventAtom causeAtom = ledger.getEvent(currentCauseId);
                if (causeAtom == null) continue;

                Integer causeGraphId = ledger.getGraphIdForEventId(currentCauseId);
                if (causeGraphId == null) continue;

                List<Integer> childrenGraphIds = ledger.getChildrenGraphIds(causeGraphId); // Get direct effects of currentCauseId

                for (Integer childGraphId : childrenGraphIds) {
                    String childEffectId = ledger.getEventIdForGraphId(childGraphId);
                    // Only add edge if this child (effect) is also part of the overall descendant set requested
                    if (childEffectId != null && relevantEventIdsSet.contains(childEffectId)) {
                        edges.add(new GraphEdge(currentCauseId, childEffectId)); // CAUSE -> EFFECT
                    }
                }
            }

            GraphData graphData = new GraphData(nodes, edges);
            return ResponseEntity.ok(graphData);

        } catch (Exception e) {
            e.printStackTrace(); // Log server-side for debugging
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error generating JSON graph: " + e.getMessage());
        }
    }

    @GetMapping("/visualize/event/{eventId}/ancestors/dot")
    public ResponseEntity<String> getAncestorGraphDot(@PathVariable("eventId") String eventId) {
        try {
            if (!ledger.containsEvent(eventId)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event not found: " + eventId);
            }

            // Get the event itself and all its ancestors
            List<String> ancestorEventIds = ledger.getEventAndCausalAncestryIds(eventId);
            if (ancestorEventIds.isEmpty() && ledger.containsEvent(eventId)) {
                ancestorEventIds = List.of(eventId); // Include the root event itself
            } else if (ancestorEventIds.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event or its ancestry not found: " + eventId);
            }

            Set<String> relevantEventIds = new HashSet<>(ancestorEventIds);

            StringBuilder dotBuilder = new StringBuilder();
            dotBuilder.append("digraph CausalAncestry {\n");
            dotBuilder.append("    rankdir=\"BT\"; // Bottom-to-Top layout: Causes below Effects\n");
            dotBuilder.append("    node [shape=box, style=\"filled,rounded\", fillcolor=lightyellow];\n");
            dotBuilder.append("    edge [arrowhead=vee];\n\n");

            // Define nodes in DOT format
            Map<String, EventAtom> eventMap = new HashMap<>();
            for (String currentEventId : relevantEventIds) {
                EventAtom event = ledger.getEvent(currentEventId);
                if (event != null) {
                    eventMap.put(currentEventId, event);
                    String label = String.format("%s\\n(%s)\\nID:%.8s",
                            event.getEventType().replace("\"", "\\\""),
                            event.getServiceName().replace("\"", "\\\""),
                            event.getEventId());
                    dotBuilder.append(String.format("    \"%s\" [label=\"%s\"];\n", event.getEventId(), label));
                }
            }
            dotBuilder.append("\n");

            // Define edges (Effect -> Cause links)
            for (String currentEventId : relevantEventIds) {
                EventAtom currentEventAtom = eventMap.get(currentEventId);
                if (currentEventAtom != null) {
                    for (String parentId : currentEventAtom.getCausalParentEventIds()) {
                        if (relevantEventIds.contains(parentId)) {
                            dotBuilder.append(String.format("    \"%s\" -> \"%s\";\n", currentEventAtom.getEventId(), parentId));
                        }
                    }
                }
            }

            dotBuilder.append("}\n");

            return ResponseEntity.ok()
                    .header("Content-Type", "text/vnd.graphviz")
                    .body(dotBuilder.toString());

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error generating DOT graph: " + e.getMessage());
        }
    }

    @GetMapping("/visualize/event/{eventId}/descendants/dot")
    public ResponseEntity<String> getDescendantGraphDot(@PathVariable("eventId") String eventId) {
        try {
            if (!ledger.containsEvent(eventId)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event not found: " + eventId);
            }

            // Get the event itself and all its descendants
            List<String> descendantEventIds = ledger.getEventAndCausalDescendantsId(eventId);
            if (descendantEventIds.isEmpty() && ledger.containsEvent(eventId)) {
                descendantEventIds = List.of(eventId);
            } else if (descendantEventIds.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error: Event or its descendants not found: " + eventId);
            }

            Set<String> relevantEventIds = new HashSet<>(descendantEventIds);

            StringBuilder dotBuilder = new StringBuilder();
            dotBuilder.append("digraph CausalDescendants {\n");
            dotBuilder.append("    rankdir=\"LR\"; // Left-to-Right layout: Cause -> Effect\n");
            dotBuilder.append("    node [shape=box, style=\"filled,rounded\", fillcolor=lightcyan];\n");
            dotBuilder.append("    edge [arrowhead=vee];\n\n");

            // Define nodes in DOT format
            Map<String, EventAtom> eventMap = new HashMap<>();
            for (String currentEventId : relevantEventIds) {
                EventAtom event = ledger.getEvent(currentEventId);
                if (event != null) {
                    eventMap.put(currentEventId, event);
                    String label = String.format("%s\\n(%s)\\nID:%.8s",
                            event.getEventType().replace("\"", "\\\""),
                            event.getServiceName().replace("\"", "\\\""),
                            event.getEventId());
                    dotBuilder.append(String.format("    \"%s\" [label=\"%s\"];\n", event.getEventId(), label));
                }
            }
            dotBuilder.append("\n");

            // Define edges (Cause -> Effect links using childrenAdjacencyList)
            for (String causeEventId : relevantEventIds) {
                EventAtom causeAtom = eventMap.get(causeEventId);
                if (causeAtom == null) continue;

                Integer causeGraphId = ledger.getGraphIdForEventId(causeEventId);
                if (causeGraphId == null) continue;

                List<Integer> childrenGraphIds = ledger.getChildrenGraphIds(causeGraphId);

                for (Integer childGraphId : childrenGraphIds) {
                    String effectEventId = ledger.getEventIdForGraphId(childGraphId);
                    if (effectEventId != null && relevantEventIds.contains(effectEventId)) {
                        dotBuilder.append(String.format("    \"%s\" -> \"%s\";\n", causeEventId, effectEventId));
                    }
                }
            }

            dotBuilder.append("}\n");

            return ResponseEntity.ok()
                    .header("Content-Type", "text/vnd.graphviz")
                    .body(dotBuilder.toString());

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error generating DOT graph: " + e.getMessage());
        }
    }

    @GetMapping("/graph/events/latest/json")
    public ResponseEntity<?> getLatestEventsGraphJson(
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "1") int historyDepth) {
        try {
            if (limit <= 0 || limit > 100) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Limit must be between 1 and 100");
            }
            if (historyDepth <= 0 || historyDepth > 5) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("History depth must be between 1 and 5");
            }

            List<String> allTopoEvents = ledger.getEventsInTopologicalOrder();
            if (allTopoEvents.isEmpty()) {
                return ResponseEntity.ok(new GraphData(Collections.emptyList(), Collections.emptyList()));
            }

            List<String> recentEventIds = allTopoEvents.subList(
                    Math.max(0, allTopoEvents.size() - limit),
                    allTopoEvents.size()
            );

            Set<String> relevantEventIdsSet = new HashSet<>(recentEventIds);
            Queue<String> frontier = new LinkedList<>(recentEventIds); // for BFS like traversal

            Map<String, Integer> eventDepth = new HashMap<>();
            for (String recentId : recentEventIds) {
                eventDepth.put(recentId, 0); // initial events are depth 0 from themselves
            }

            while (!frontier.isEmpty()) {
                String currentEventId = frontier.poll();
                int currentDepth = eventDepth.getOrDefault(currentEventId, 0);

                if (currentDepth >= historyDepth) {
                    continue;
                }

                EventAtom currentEventAtom = ledger.getEvent(currentEventId);
                if (currentEventAtom != null) {
                    for (String parentId : currentEventAtom.getCausalParentEventIds()) {
                        if (relevantEventIdsSet.add(parentId)) {
                            frontier.offer(parentId);
                            eventDepth.put(parentId, currentDepth + 1);
                        }
                    }
                }
            }

            List<GraphNode> nodes = new ArrayList<>();
            for (String eventIdFromSet : relevantEventIdsSet) {
                EventAtom event = ledger.getEvent(eventIdFromSet);
                if (event != null) {
                    String label = event.getEventType();
                    String title = String.format("Type: %s\nEntity: %s\nID: %s", event.getEventType(), event.getServiceName(), event.getEventId());
                    String group = event.getEventType();
                    nodes.add(new GraphNode(event.getEventId(), label, title, group));
                }
            }

            // cause -> effect
            List<GraphEdge> edges = new ArrayList<>();
            for (String causeId : relevantEventIdsSet) { // Each node in our subgraph can be a cause
                Integer causeGraphId = ledger.getGraphIdForEventId(causeId);
                if (causeGraphId == null) continue;

                List<Integer> childrenGraphIds = ledger.getChildrenGraphIds(causeGraphId);
                for (Integer childGraphId : childrenGraphIds) {
                    String effectId = ledger.getEventIdForGraphId(childGraphId);
                    // Add edge only if BOTH cause and effect are in our 'relevantEventIdsSet'
                    if (effectId != null && relevantEventIdsSet.contains(effectId)) {
                        edges.add(new GraphEdge(causeId, effectId));
                    }
                }
            }

            GraphData graphData = new GraphData(nodes, edges);
            return ResponseEntity.ok(graphData);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error generating latest events graph: " + e.getMessage());
        }
    }

    @PostMapping("/events/compare-causality")
    public ResponseEntity<?> compareCausality(@RequestBody CompareCausalityRequest req) {
        String e1 = req.getEventId1();
        String e2 = req.getEventId2();

        if (!ledger.containsEvent(e1) || !ledger.containsEvent(e2)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("One or both event IDs not found: " + e1 + ", " + e2);
        }

        EventAtom atom1 = ledger.getEvent(e1);
        EventAtom atom2 = ledger.getEvent(e2);

        // VC-based relationship
        CausalRelationship vcRel = atom1.getCausalRelationshipWith(atom2);
        boolean vcCauses = (vcRel == CausalRelationship.CAUSES);

        // Explicit graph computations
        List<String> path1To2 = ledger.getShortestCausalPath(e1, e2);
        List<String> path2To1 = ledger.getShortestCausalPath(e2, e1);
        List<String> allCommon = ledger.getAllCommonCausalAncestors(e1, e2);
        List<String> nearestCommon = ledger.getNearestCommonCausalAncestors(e1, e2);

        CompareCausalityResponse resp = CompareCausalityResponse.builder()
                .eventId1(e1)
                .eventId2(e2)
                .relationship(vcRel)
                .event1HappensBeforeEvent2_VC(vcCauses)
                .shortestPath1To2_ExplicitGraph(path1To2)
                .shortestPath2To1_ExplicitGraph(path2To1)
                .allCommonAncestors_ExplicitGraph(allCommon)
                .nearestCommonAncestors_ExplicitGraph(nearestCommon)
                .build();

        return ResponseEntity.ok(resp);
    }
}

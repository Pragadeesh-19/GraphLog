package com.graphlog.controller;

import com.graphlog.core.CausalLedger;
import com.graphlog.core.EventAtom;
import com.graphlog.dto.GraphData;
import com.graphlog.dto.GraphEdge;
import com.graphlog.dto.GraphNode;
import com.graphlog.dto.IngestEventRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class LedgerController {

    private final CausalLedger ledger;

    @PostMapping("/events")
    public ResponseEntity<?> ingestNewEvent(@RequestBody IngestEventRequest request) {
        try {
            String eventId = ledger.ingestEvent(
                    request.entityId,
                    request.eventType,
                    request.payload,
                    request.causalParentEventIds
            );
            EventAtom createdEvent = ledger.getEvent(eventId);
            if (createdEvent != null) {
                return ResponseEntity.status(HttpStatus.CREATED).body(createdEvent);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error retrieving created event");
            }
        } catch (CausalLedger.UnknownParentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Error: " + e.getMessage());
        } catch (CausalLedger.CausalLoopException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Error: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred: " + e.getMessage());
        }
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
                    String title = String.format("Type: %s\nEntity: %s\nID: %s", event.getEventType(), event.getEntityId(), event.getEventId());
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
                    String title = String.format("Type: %s\nEntity: %s\nID: %s", event.getEventType(), event.getEntityId(), event.getEventId());
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
                            event.getEntityId().replace("\"", "\\\""),
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
                            event.getEntityId().replace("\"", "\\\""),
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

}

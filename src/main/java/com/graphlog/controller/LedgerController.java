package com.graphlog.controller;

import com.graphlog.core.CausalLedger;
import com.graphlog.core.EventAtom;
import com.graphlog.dto.IngestEventRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

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
}

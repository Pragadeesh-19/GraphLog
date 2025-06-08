package com.graphlog.interfaces;

import java.util.Map;

@FunctionalInterface
public interface StateUpdater<S>{

    /**
     * Applies the given event to the current state and returns the new state.
     * This method should be pure - it should not modify the currentState directly
     * if it's mutable, but rather return a new state or a modified copy.
     *
     * @param currentState The current state of the entity before the event
     * @param eventPayload The payload of the event being applied
     * @param eventType    The type of the event being applied
     * @return The new state of the entity after the event is applied
     */
    S apply(S currentState, Map<String, Object> eventPayload, String eventType);
}

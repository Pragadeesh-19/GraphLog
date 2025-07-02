package com.graphDB.core;

/**
 * Enum representing the causal relationship between two events in a distributed system.
 *
 * This follows the Vector Clock causality model where events can be:
 * - Causally ordered (one happens before the other)
 * - Concurrent (neither causally affects the other)
 */

public enum CausalRelationship {

    /** Event A strictly causes Event B (A happens-before B). */
    CAUSES,
    /** Event A is strictly caused by Event B (A happens-after B). */
    CAUSED_BY,
    /** The two events are concurrent (neither causally affects the other). */
    CONCURRENT,
    /** The two events are identical in vector time. */
    IDENTICAL,
    /** Causality is undefined (e.g., missing or incomparable clocks). */
    UNDEFINED;

    public CausalRelationship inverse() {
        switch (this) {
            case CAUSES:     return CAUSED_BY;
            case CAUSED_BY:  return CAUSES;
            default:         return this;
        }
    }

    /**
     * Checks if there is a true causal dependency (not concurrent/undefined).
     */
    public boolean isCausallyDependent() {
        return this == CAUSES || this == CAUSED_BY;
    }


}

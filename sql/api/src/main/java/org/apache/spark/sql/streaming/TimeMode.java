package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.plans.logical.EventTime$;
import org.apache.spark.sql.catalyst.plans.logical.NoTime$;
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTime$;

/**
 * Represents the time modes (used for specifying timers and ttl) possible for
 * the Dataset operations {@code transformWithState}.
 */
@Experimental
@Evolving
public class TimeMode {

    /**
     * Stateful processor that does not register timers, or use ttl eviction.
     */
    public static final TimeMode None() { return NoTime$.MODULE$; }

    /**
     * Stateful processor that uses query processing time to register timers or
     * calculate ttl expiration.
     */
    public static final TimeMode ProcessingTime() { return ProcessingTime$.MODULE$; }

    /**
     * Stateful processor that uses event time to register timers. Note that ttl is not
     * supported in this TimeMode.
     */
    public static final TimeMode EventTime() { return EventTime$.MODULE$; }
}

package org.apache.spark.sql.execution.streaming;

/**
 * The shared interface between V1 and V2 streaming sinks.
 *
 * This is a temporary interface for compatibility during migration. It should not be implemented
 * directly, and will be removed in future versions.
 */
public interface BaseStreamingSink {
}

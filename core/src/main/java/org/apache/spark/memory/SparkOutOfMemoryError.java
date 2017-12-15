package org.apache.spark.memory;

/**
 * This exception is thrown when a task can not acquire memory from the Memory manager.
 * Instead of throwing {@link OutOfMemoryError}, which kills the executor,
 * we should use throw this exception, which will just kill the current task.
 */
public final class SparkOutOfMemoryError extends OutOfMemoryError {

    public SparkOutOfMemoryError(String s) {
        super(s);
    }

    public SparkOutOfMemoryError(OutOfMemoryError e) {
        super(e.getMessage());
    }
}

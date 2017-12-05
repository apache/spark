package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.sources.v2.reader.PartitionOffset;

import java.io.IOException;

/**
 * A variation on {@link DataReader} for use with streaming in continuous processing mode.
 */
public interface ContinuousDataReader<T> extends DataReader<T> {
    /**
     * Proceed to next record, returning false only if the read is interrupted.
     *
     * @throws IOException if failure happens during disk/network IO like reading files.
     */
    boolean next() throws IOException;

    /**
     * Return the current record. This method should return same value until `next` is called.
     */
    T get();

    /**
     * Get the offset of the current record.
     *
     * The execution engine will use this offset as a restart checkpoint.
     */
    PartitionOffset getOffset();
}

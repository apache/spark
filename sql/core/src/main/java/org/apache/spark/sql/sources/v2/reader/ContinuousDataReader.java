package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.sources.v2.reader.PartitionOffset;

import java.io.IOException;

/**
 * A variation on {@link DataReader} for use with streaming in continuous processing mode.
 */
public interface ContinuousDataReader<T> extends DataReader<T> {
    /**
     * Get the offset of the current record.
     *
     * The execution engine will call this method along with get() to keep track of the current
     * offset. When an epoch ends, the offset of the previous record in each partition will be saved
     * as a restart checkpoint.
     */
    PartitionOffset getOffset();
}

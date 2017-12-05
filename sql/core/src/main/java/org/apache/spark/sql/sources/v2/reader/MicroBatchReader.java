package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.sources.v2.BaseStreamingSource;

import java.util.List;
import java.util.Optional;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to indicate they allow micro-batch streaming reads.
 */
public interface MicroBatchReader extends DataSourceV2Reader, BaseStreamingSource {
    /**
     * Set the desired offset range for read tasks created from this reader.
     *
     * @param start The initial offset to scan from. If absent(), scan from the earliest available
     *              offset.
     * @param end The last offset to include in the scan. If absent(), scan up to an
     *            implementation-defined inferred endpoint, such as the last available offset
     *            or the start offset plus a target batch size.
     */
    void setOffsetRange(Optional<Offset> start, Optional<Offset> end);

    /**
     * Returns the current start offset for this reader.
     */
    Offset getStart();

    /**
     * Return the current end offset for this reader.
     */
    Offset getEnd();
}

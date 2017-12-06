package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.sources.v2.reader.Offset;
import org.apache.spark.sql.execution.streaming.BaseStreamingSource;

import java.util.Optional;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to indicate they allow micro-batch streaming reads.
 */
public interface MicroBatchReader extends DataSourceV2Reader, BaseStreamingSource {
    /**
     * Set the desired offset range for read tasks created from this reader. Read tasks will
     * generate only data within (`start`, `end`]; that is, from the first record after `start` to
     * the record with offset `end`.
     *
     * @param start The initial offset to scan from. If not specified, scan from an
     *              implementation-specified start point, such as the earliest available record.
     * @param end The last offset to include in the scan. If not specified, scan up to an
     *            implementation-defined endpoint, such as the last available offset
     *            or the start offset plus a target batch size.
     */
    void setOffsetRange(Optional<Offset> start, Optional<Offset> end);

    /**
     * Returns the specified or inferred start offset for this reader.
     *
     * @throws IllegalStateException if setOffsetRange has not been called
     */
    Offset getStartOffset();

    /**
     * Return the specified or inferred end offset for this reader.
     *
     * @throws IllegalStateException if setOffsetRange has not been called
     */
    Offset getEndOffset();

    /**
     * Deserialize a JSON string into an Offset of the implementation-defined offset type.
     * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
     */
    Offset deserialize(String json);
}

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.PartitionOffset;
import org.apache.spark.sql.sources.v2.BaseStreamingSource;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Optional;

/**
 * A mix-in interface for {@link DataSourceV2Reader}. Data source readers can implement this
 * interface to allow reading in a continuous processing mode stream.
 *
 * Implementations must ensure each read task output is a {@link ContinuousDataReader}.
 */
public interface ContinuousReader extends BaseStreamingSource, DataSourceV2Reader {
    /**
     * Merge offsets coming from {@link ContinuousDataReader} instances in each partition to
     * a single global offset.
     */
    Offset mergeOffsets(PartitionOffset[] offsets);

    /**
     * Set the desired start offset for read tasks created from this reader.
     *
     * @param start The initial offset to scan from. May be None, in which case scan will start from
     *              the beginning of the stream.
     */
    void setOffset(Optional<Offset> start);

    /**
     * The execution engine will call this method in every epoch to determine if new read tasks need
     * to be generated, which may be required if for example the underlying source system has had
     * partitions added or removed.
     *
     * If true, the query will be shut down and restarted with a new reader.
     */
    default boolean needsReconfiguration() {
        return false;
    }
}

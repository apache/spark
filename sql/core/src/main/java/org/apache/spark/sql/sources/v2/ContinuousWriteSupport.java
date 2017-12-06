package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.BaseStreamingSink;
import org.apache.spark.sql.sources.v2.writer.ContinuousWriter;
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data writing ability for continuous stream processing.
 */
@InterfaceStability.Evolving
public interface ContinuousWriteSupport extends BaseStreamingSink {

    /**
     * Creates an optional {@link ContinuousWriter} to save the data to this data source. Data
     * sources can return None if there is no writing needed to be done.
     *
     * @param queryId A unique string for the writing query. It's possible that there are many writing
     *                queries running at the same time, and the returned {@link DataSourceV2Writer}
     *                can use this id to distinguish itself from others.
     * @param schema the schema of the data to be written.
     * @param mode the output mode which determines what successive batch output means to this
     *             source, please refer to {@link OutputMode} for more details.
     * @param options the options for the returned data source writer, which is an immutable
     *                case-insensitive string-to-string map.
     */
    Optional<ContinuousWriter> createContinuousWriter(
        String queryId,
        StructType schema,
        OutputMode mode,
        DataSourceV2Options options);
}

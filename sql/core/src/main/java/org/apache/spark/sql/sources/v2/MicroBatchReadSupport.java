package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.reader.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide streaming micro-batch data reading ability.
 */
@InterfaceStability.Evolving
public interface MicroBatchReadSupport extends DataSourceV2 {
  /**
   * Creates a {@link MicroBatchReader} to read batches of data from this data source in a
   * streaming query.
   *
   * The execution engine will create a micro-batch reader at the start of a streaming query,
   * alternate calls to setOffsetRange and createReadTasks for each batch to process, and then
   * call stop() when the execution is complete. Note that a single query may have multiple
   * executions due to restart or failure recovery.
   *
   * @param schema the user provided schema, or empty() if none was provided
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Readers for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  MicroBatchReader createMicroBatchReader(
      Optional<StructType> schema,
      String checkpointLocation,
      DataSourceV2Options options);
}

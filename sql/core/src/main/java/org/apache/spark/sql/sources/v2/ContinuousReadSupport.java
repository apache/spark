package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.sql.sources.v2.reader.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.DataSourceV2Reader;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide data reading ability for continuous stream processing.
 */
public interface ContinuousReadSupport extends DataSourceV2 {
  /**
   * Creates a {@link ContinuousReader} to scan the data from this data source.
   *
   * @param schema the user provided schema, or empty() if none was provided
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Readers for the same logical source in the same query
   *                           will be given the same checkpointLocation.
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  ContinuousReader createContinuousReader(Optional<StructType> schema, String checkpointLocation, DataSourceV2Options options);
}

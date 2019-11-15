package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

/**
 * :: Experimental ::
 * This interface contains physical (i.e. RDD) write information that data sources can use when
 * generating a {@link DataWriterFactory} or a {@link StreamingDataWriterFactory}.
 */
@Experimental
public interface PhysicalWriteInfo {
  /**
   * @return  The number of partitions of the RDD that is going to be written.
   */
  int numPartitions();
}

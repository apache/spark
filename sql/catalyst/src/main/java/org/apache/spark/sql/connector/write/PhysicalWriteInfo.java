package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;

/**
 *
 */
@Experimental
public interface PhysicalWriteInfo {
  /**
   * @return  The number of partitions of the RDD that is going to be written.
   */
  int numPartitions();
}

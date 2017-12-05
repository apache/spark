package org.apache.spark.sql.sources.v2;

import org.apache.spark.sql.execution.streaming.Offset;

/**
 * The shared interface between V1 streaming sources and V2 streaming readers.
 */
public interface BaseStreamingSource {
  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  void commit(Offset end);

  /** Stop this source and free any resources it has allocated. */
  void stop();
}

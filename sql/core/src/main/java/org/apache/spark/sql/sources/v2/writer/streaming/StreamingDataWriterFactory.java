package org.apache.spark.sql.sources.v2.writer.streaming;

import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

public interface StreamingDataWriterFactory<T> extends DataWriterFactory<T> {
  /**
   * Returns a data writer to do the actual writing work.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param attemptNumber Spark may launch multiple tasks with the same task id. For example, a task
   *                      failed, Spark launches a new task wth the same task id but different
   *                      attempt number. Or a task is too slow, Spark launches new tasks wth the
   *                      same task id but different attempt number, which means there are multiple
   *                      tasks with the same task id running at the same time. Implementations can
   *                      use this attempt number to distinguish writers of different task attempts.
   * @param epochId A monotonically increasing id for streaming queries that are split in to
   *                discrete periods of execution. For non-streaming queries,
   *                this ID will always be 0.
   */
  DataWriter<T> createDataWriter(int partitionId, int attemptNumber, long epochId);

  @Override default DataWriter<T> createDataWriter(int partitionId, int attemptNumber) {
    throw new IllegalStateException("Streaming data writer factory cannot create data writers without epoch.");
  }
}

package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;

/**
 * A {@link DataSourceV2Writer} for use with continuous stream processing.
 */
@InterfaceStability.Evolving
public interface ContinuousWriter extends DataSourceV2Writer {
  /**
   * Commits this writing job for the specified epoch with a list of commit messages. The commit
   * messages are collected from successful data writers and are produced by
   * {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to have been
   * failed, and the execution engine will attempt to call {@link #abort(WriterCommitMessage[])}.
   */
  void commit(long epochId, WriterCommitMessage[] messages);

  default void commit(WriterCommitMessage[] messages) {
    throw new UnsupportedOperationException(
       "Commit without epoch should not be called with ContinuousWriter");
  }
}

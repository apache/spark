/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Evolving;

/**
 * An interface that defines how to write the data to data source for batch processing.
 * <p>
 * The writing procedure is:
 * <ol>
 *   <li>Create a writer factory by {@link #createBatchWriterFactory(PhysicalWriteInfo)}, serialize
 *     and send it to all the partitions of the input data(RDD).</li>
 *   <li>For each partition, create the data writer, and write the data of the partition with this
 *     writer. If all the data are written successfully, call {@link DataWriter#commit()}. If
 *     exception happens during the writing, call {@link DataWriter#abort()}.</li>
 *   <li>If all writers are successfully committed, call {@link #commit(WriterCommitMessage[])}. If
 *     some writers are aborted, or the job failed with an unknown reason, call
 *     {@link #abort(WriterCommitMessage[])}.</li>
 * </ol>
 * <p>
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 * <p>
 * Please refer to the documentation of commit/abort methods for detailed specifications.
 *
 * @since 3.0.0
 */
@Evolving
public interface BatchWrite {

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   * <p>
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param info Physical information about the input data that will be written to this table.
   */
  DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info);

  /**
   * Returns whether Spark should use the commit coordinator to ensure that at most one task for
   * each partition commits.
   *
   * @return true if commit coordinator should be used, false otherwise.
   */
  default boolean useCommitCoordinator() {
    return true;
  }

  /**
   * Handles a commit message on receiving from a successful data writer.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called.
   */
  default void onDataWriterCommit(WriterCommitMessage message) {}

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected from
   * successful data writers and are produced by {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called. The state of the destination
   * is undefined and @{@link #abort(WriterCommitMessage[])} may not be able to deal with it.
   *
   * Note that speculative execution may cause multiple tasks to run for a partition. By default,
   * Spark uses the commit coordinator to allow at most one task to commit. Implementations can
   * disable this behavior by overriding {@link #useCommitCoordinator()}. If disabled, multiple
   * tasks may have committed successfully and one successful commit message per task will be
   * passed to this commit method. The remaining commit messages are ignored by Spark.
   */
  void commit(WriterCommitMessage[] messages);

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry,
   * or the Spark job fails with some unknown reasons,
   * or {@link #onDataWriterCommit(WriterCommitMessage)} fails,
   * or {@link #commit(WriterCommitMessage[])} fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   *
   * Unless the abort is triggered by the failure of commit, the given messages should have some
   * null slots as there maybe only a few data writers that are committed before the abort
   * happens, or some data writers were committed but their commit messages haven't reached the
   * driver when the abort is triggered. So this is just a "best effort" for data sources to
   * clean up the data left by data writers.
   */
  void abort(WriterCommitMessage[] messages);
}

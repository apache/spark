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

package org.apache.spark.sql.sources.v2.writer;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

/**
 * An interface that defines how to write the data to data source for batch processing.
 *
 * The writing procedure is:
 *   1. Create a writer factory by {@link #createBatchWriterFactory(WriteConfig)}, serialize and
 *      send it to all the partitions of the input data(RDD).
 *   2. For each partition, create the data writer, and write the data of the partition with this
 *      writer. If all the data are written successfully, call {@link DataWriter#commit()}. If
 *      exception happens during the writing, call {@link DataWriter#abort()}.
 *   3. If all writers are successfully committed, call
 *      {@link #commit(WriteConfig,WriterCommitMessage[])}. If some writers are aborted, or the job
 *      failed with an unknown reason, call {@link #abort(WriteConfig, WriterCommitMessage[])}.
 *
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 *
 * Please refer to the documentation of commit/abort methods for detailed specifications.
 */
@InterfaceStability.Evolving
public interface BatchWriteSupport {

  /**
   * Creates a {@link WriteConfig} for a batch write operation.
   *
   * @param schema schema of the data that will be written
   * @param options options to configure the write operation
   * @return a new WriteConfig
   */
  // TODO: replace DataSourceOptions with CaseInsensitiveStringMap
  WriteConfig createWriteConfig(StructType schema, DataSourceOptions options);

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  DataWriterFactory createBatchWriterFactory(WriteConfig config);

  /**
   * Returns whether Spark should use the commit coordinator to ensure that at most one task for
   * each partition commits.
   *
   * @return true if commit coordinator should be used, false otherwise.
   */
  default boolean useCommitCoordinator(WriteConfig config) {
    return true;
  }

  /**
   * Handles a commit message on receiving from a successful data writer.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriteConfig, WriterCommitMessage[])} would be called.
   */
  default void onDataWriterCommit(WriteConfig config, WriterCommitMessage message) {}

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected from
   * successful data writers and are produced by {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriteConfig, WriterCommitMessage[])} would be called. The state of
   * the destination is undefined and @{@link #abort(WriteConfig, WriterCommitMessage[])} may not be
   * able to deal with it.
   *
   * Note that speculative execution may cause multiple tasks to run for a partition. By default,
   * Spark uses the commit coordinator to allow at most one task to commit. Implementations can
   * disable this behavior by overriding {@link #useCommitCoordinator(WriteConfig)}. If disabled,
   * multiple tasks may have committed successfully and one successful commit message per task will
   * be passed to this commit method. The remaining commit messages are ignored by Spark.
   */
  void commit(WriteConfig config, WriterCommitMessage[] messages);

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry,
   * or the Spark job fails with some unknown reasons,
   * or {@link #onDataWriterCommit(WriteConfig,WriterCommitMessage)} fails,
   * or {@link #commit(WriteConfig,WriterCommitMessage[])} fails.
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
  void abort(WriteConfig config, WriterCommitMessage[] messages);
}

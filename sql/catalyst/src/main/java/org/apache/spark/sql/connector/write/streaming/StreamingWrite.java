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

package org.apache.spark.sql.connector.write.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * An interface that defines how to write the data to data source in streaming queries.
 *
 * The writing procedure is:
 *   1. Create a writer factory by {@link #createStreamingWriterFactory(PhysicalWriteInfo)},
 *      serialize and send it to all the partitions of the input data(RDD).
 *   2. For each epoch in each partition, create the data writer, and write the data of the epoch in
 *      the partition with this writer. If all the data are written successfully, call
 *      {@link DataWriter#commit()}. If exception happens during the writing, call
 *      {@link DataWriter#abort()}.
 *   3. If writers in all partitions of one epoch are successfully committed, call
 *      {@link #commit(long, WriterCommitMessage[])}. If some writers are aborted, or the job failed
 *      with an unknown reason, call {@link #abort(long, WriterCommitMessage[])}.
 *
 * While Spark will retry failed writing tasks, Spark won't retry failed writing jobs. Users should
 * do it manually in their Spark applications if they want to retry.
 *
 * Please refer to the documentation of commit/abort methods for detailed specifications.
 *
 * @since 3.0.0
 */
@Evolving
public interface StreamingWrite {

  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   * @param info Information about the RDD that will be written to this data writer
   */
  StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info);

  /**
   * Commits this writing job for the specified epoch with a list of commit messages. The commit
   * messages are collected from successful data writers and are produced by
   * {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to have been
   * failed, and the execution engine will attempt to call
   * {@link #abort(long, WriterCommitMessage[])}.
   *
   * The execution engine may call `commit` multiple times for the same epoch in some circumstances.
   * To support exactly-once data semantics, implementations must ensure that multiple commits for
   * the same epoch are idempotent.
   */
  void commit(long epochId, WriterCommitMessage[] messages);

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retried, or
   * the Spark job fails with some unknown reasons, or {@link #commit(long, WriterCommitMessage[])}
   * fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   *
   * Unless the abort is triggered by the failure of commit, the given messages will have some
   * null slots, as there may be only a few data writers that were committed before the abort
   * happens, or some data writers were committed but their commit messages haven't reached the
   * driver when the abort is triggered. So this is just a "best effort" for data sources to
   * clean up the data left by data writers.
   */
  void abort(long epochId, WriterCommitMessage[] messages);
}

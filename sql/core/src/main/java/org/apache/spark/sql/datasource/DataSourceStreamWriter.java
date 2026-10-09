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

package org.apache.spark.sql.datasource;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Writes data to a {@link DataSource} for a streaming write.
 * <p>
 * Spark creates the writer on the driver with {@link DataSource#streamWriter} and keeps it for
 * the lifetime of the streaming query. For each micro-batch (epoch), it serializes the writer and
 * sends it to the executors, where {@link #createWriter} is called once for each task. When all
 * the tasks of the micro-batch succeed, Spark calls {@link #commit} on the driver with the
 * messages returned by the task writers. Otherwise, it calls {@link #abort}.
 * <p>
 * As with {@link DataSourceWriter}, the batches passed to {@link DataWriter#write} are backed by
 * Apache Arrow and are only valid during the call.
 *
 * @since 4.4.0
 */
@Evolving
public interface DataSourceStreamWriter extends Serializable {

  /**
   * Creates the writer for one task of a micro-batch. Called on an executor.
   *
   * @param partitionId the partition the task writes
   * @param taskId the ID of the task attempt
   * @param epochId the ID of the micro-batch
   */
  DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId, long epochId);

  /**
   * Commits a micro-batch. Called on the driver when all the tasks of the micro-batch succeeded.
   *
   * @param epochId the ID of the micro-batch
   * @param messages the messages returned by {@link DataWriter#commit()} of each task
   */
  void commit(long epochId, WriterCommitMessage[] messages);

  /**
   * Aborts a micro-batch. Called on the driver when the micro-batch failed.
   *
   * @param epochId the ID of the micro-batch
   * @param messages the messages returned by the tasks that committed. An element is null if its
   *                 task did not commit.
   */
  void abort(long epochId, WriterCommitMessage[] messages);
}

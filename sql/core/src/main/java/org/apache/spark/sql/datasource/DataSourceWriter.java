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
 * Writes data to a {@link DataSource} for a batch write.
 * <p>
 * Spark creates the writer on the driver with {@link DataSource#writer}, then serializes it and
 * sends it to the executors, where {@link #createWriter} is called once for each task. When all
 * the tasks succeed, Spark calls {@link #commit} on the driver with the messages returned by the
 * task writers. Otherwise, it calls {@link #abort}.
 * <p>
 * The batches passed to {@link DataWriter#write} are backed by Apache Arrow: every column is an
 * {@link org.apache.spark.sql.vectorized.ArrowColumnVector}. A batch is only valid during the
 * call, so a writer that keeps data after the call returns must copy it.
 *
 * @since 4.4.0
 */
@Evolving
public interface DataSourceWriter extends Serializable {

  /**
   * Creates the writer for one task. Called on an executor.
   *
   * @param partitionId the partition the task writes
   * @param taskId the ID of the task attempt
   */
  DataWriter<ColumnarBatch> createWriter(int partitionId, long taskId);

  /**
   * Commits the write. Called on the driver when all the tasks succeeded.
   *
   * @param messages the messages returned by {@link DataWriter#commit()} of each task
   */
  void commit(WriterCommitMessage[] messages);

  /**
   * Aborts the write. Called on the driver when the write failed.
   *
   * @param messages the messages returned by the tasks that committed. An element is null if its
   *                 task did not commit.
   */
  void abort(WriterCommitMessage[] messages);
}

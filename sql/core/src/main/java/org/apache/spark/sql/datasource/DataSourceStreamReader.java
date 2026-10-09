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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Reads the data of a {@link DataSource} for a micro-batch streaming scan.
 * <p>
 * Spark creates the reader on the driver with {@link DataSource#streamReader} and keeps it for
 * the lifetime of the streaming query. Offsets are JSON strings defined by the data source; Spark
 * stores them in the checkpoint and passes them back unchanged. For each micro-batch, Spark plans
 * the partitions between two offsets with {@link #partitions}, then serializes the reader and
 * sends it to the executors, where {@link #read} is called once for each partition.
 *
 * @since 4.4.0
 */
@Evolving
public interface DataSourceStreamReader extends Serializable {

  /**
   * Returns the offset to start from when the query has no checkpointed offset.
   */
  String initialOffset();

  /**
   * Returns the most recent offset available.
   */
  String latestOffset();

  /**
   * Plans the partitions that read the data after {@code start} up to and including {@code end}.
   */
  InputPartition[] partitions(String start, String end);

  /**
   * Reads a partition. Called on an executor. The columns of the returned batches must match the
   * schema passed to {@link DataSource#streamReader}. A batch only needs to stay valid until the
   * next call to {@link PartitionReader#next()}.
   */
  PartitionReader<ColumnarBatch> read(InputPartition partition);

  /**
   * Tells the data source that Spark has processed all the data up to and including {@code end}
   * and will not ask for it again.
   */
  default void commit(String end) {}

  /**
   * Stops the reader when the streaming query terminates.
   */
  default void stop() {}
}

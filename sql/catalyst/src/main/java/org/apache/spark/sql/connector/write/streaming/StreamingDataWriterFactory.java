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

import java.io.Serializable;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;

/**
 * A factory of {@link DataWriter} returned by
 * {@link StreamingWrite#createStreamingWriterFactory(PhysicalWriteInfo)}, which is responsible for
 * creating and initializing the actual data writer at executor side.
 * <p>
 * Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So this interface must be
 * serializable and {@link DataWriter} doesn't need to be.
 *
 * @since 3.0.0
 */
@Evolving
public interface StreamingDataWriterFactory extends Serializable {

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers
   * are responsible for defensive copies if necessary, e.g. copy the data before buffer it in a
   * list.
   * <p>
   * If this method fails (by throwing an exception), the corresponding Spark write task would fail
   * and get retried until hitting the maximum retry times.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param taskId The task id returned by {@link TaskContext#taskAttemptId()}. Spark may run
   *               multiple tasks for the same partition (due to speculation or task failures,
   *               for example).
   * @param epochId A monotonically increasing id for streaming queries that are split in to
   *                discrete periods of execution.
   */
  DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId);
}

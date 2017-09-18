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

import java.io.Serializable;

import org.apache.spark.annotation.InterfaceStability;

/**
 * A factory of {@link DataWriter} returned by {@link DataSourceV2Writer#createWriterFactory()},
 * which is responsible for creating and initializing the actual data writer at executor side.
 *
 * Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So {@link DataWriterFactory} must be
 * serializable and {@link DataWriter} doesn't need to be.
 */
@InterfaceStability.Evolving
public interface DataWriterFactory<T> extends Serializable {

  /**
   * Returns a data writer to do the actual writing work.
   *
   * @param stageId The id of the Spark stage that runs the returned writer.
   * @param partitionId The id of the RDD partition that the returned writer will process.
   * @param attemptNumber The attempt number of the Spark task that runs the returned writer, which
   *                      is usually 0 if the task is not a retried task or a speculative task.
   */
  DataWriter<T> createWriter(int stageId, int partitionId, int attemptNumber);
}

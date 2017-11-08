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
   */
  DataWriter<T> createDataWriter(int partitionId, int attemptNumber);
}

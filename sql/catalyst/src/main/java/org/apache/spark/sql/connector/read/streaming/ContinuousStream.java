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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;

/**
 * A {@link SparkDataStream} for streaming queries with continuous mode.
 *
 * @since 3.0.0
 */
@Evolving
public interface ContinuousStream extends SparkDataStream {

  /**
   * Returns a list of {@link InputPartition input partitions} given the start offset. Each
   * {@link InputPartition} represents a data split that can be processed by one Spark task. The
   * number of input partitions returned here is the same as the number of RDD partitions this scan
   * outputs.
   * <p>
   * If the {@link Scan} supports filter pushdown, this stream is likely configured with a filter
   * and is responsible for creating splits for that filter, which is not a full scan.
   * </p>
   * <p>
   * This method will be called to launch one Spark job for reading the data stream. It will be
   * called more than once, if {@link #needsReconfiguration()} returns true and Spark needs to
   * launch a new job.
   * </p>
   */
  InputPartition[] planInputPartitions(Offset start);

  /**
   * Returns a factory to create a {@link ContinuousPartitionReader} for each
   * {@link InputPartition}.
   */
  ContinuousPartitionReaderFactory createContinuousReaderFactory();

  /**
   * Merge partitioned offsets coming from {@link ContinuousPartitionReader} instances
   * for each partition to a single global offset.
   */
  Offset mergeOffsets(PartitionOffset[] offsets);

  /**
   * The execution engine will call this method in every epoch to determine if new input
   * partitions need to be generated, which may be required if for example the underlying
   * source system has had partitions added or removed.
   * <p>
   * If true, the Spark job to scan this continuous data stream will be interrupted and Spark will
   * launch it again with a new list of {@link InputPartition input partitions}.
   */
  default boolean needsReconfiguration() {
    return false;
  }
}

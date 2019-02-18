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

package org.apache.spark.sql.sources.v2.reader.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.PartitionReader;
import org.apache.spark.sql.sources.v2.reader.PartitionReaderFactory;
import org.apache.spark.sql.sources.v2.reader.Scan;

/**
 * A {@link SparkDataStream} for streaming queries with micro-batch mode.
 */
@Evolving
public interface MicroBatchStream extends SparkDataStream {

  /**
   * Returns the most recent offset available.
   */
  Offset latestOffset();

  /**
   * Returns a list of {@link InputPartition input partitions} given the start and end offsets. Each
   * {@link InputPartition} represents a data split that can be processed by one Spark task. The
   * number of input partitions returned here is the same as the number of RDD partitions this scan
   * outputs.
   * <p>
   * If the {@link Scan} supports filter pushdown, this stream is likely configured with a filter
   * and is responsible for creating splits for that filter, which is not a full scan.
   * </p>
   * <p>
   * This method will be called multiple times, to launch one Spark job for each micro-batch in this
   * data stream.
   * </p>
   */
  InputPartition[] planInputPartitions(Offset start, Offset end);

  /**
   * Returns a factory to create a {@link PartitionReader} for each {@link InputPartition}.
   */
  PartitionReaderFactory createReaderFactory();
}

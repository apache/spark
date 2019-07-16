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

/**
 * An interface that defines how to scan the data from data source for micro-batch streaming
 * processing.
 *
 * The scanning procedure is:
 *   1. Determine the start offset of the next micro-batch by {@link #initialOffset()} or recover
 *      from the checkpoint if it exists. Determine the end offset of the next micro-batch by
 *      {@link #latestOffset()}.
 *   2. Create the input partitions to scan the next micro-batch by
 *      {@link #planInputPartitions(Offset, Offset)} with the start and end offset. Launch a Spark
 *      job and submit one task for each input partition to produce data.
 *   3. Create a partition reader factory by {@link #createReaderFactory()}, serialize and send it
 *      to each input partition.
 *   4. For each partition, create the data reader from the reader factory, and scan the data from
 *      the input partition with this reader.
 *   5. After finish scanning this micro-batch, Spark saves the offset to the checkpoint, go back to
 *      step 1 to scan the next micro-batch.
 */
@Evolving
public interface MicroBatchScan extends StreamingScan {

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
   * If the data source supports filter pushdown, this scan is likely configured with a filter
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

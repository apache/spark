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

/**
 * A {@link MicroBatchStream} for streaming queries with real time mode.
 *
 */
@Evolving
public interface SupportsRealTimeMode {
    /**
     * Returns a list of {@link InputPartition input partitions} given the start offset. Each
     * {@link InputPartition} represents a data split that can be processed by one Spark task. The
     * number of input partitions returned here is the same as the number of RDD partitions
     * this scan outputs.
     */
    InputPartition[] planInputPartitions(Offset start);

    /**
     * Merge partitioned offsets coming from {@link SupportsRealTimeMode} instances
     * for each partition to a single global offset.
     */
    Offset mergeOffsets(PartitionOffset[] offsets);

    /**
     * Called during logical planning to inform the source if it's in real time mode
     */
    default void prepareForRealTimeMode() {}
}

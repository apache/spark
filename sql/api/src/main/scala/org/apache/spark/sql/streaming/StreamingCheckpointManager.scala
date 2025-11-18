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

package org.apache.spark.sql.streaming

/**
 * A class to manage operations on streaming query checkpoints.
 */
private[spark] abstract class StreamingCheckpointManager {

  /**
   * Repartition the stateful streaming operators state in the streaming checkpoint to have
   * `numPartitions` partitions. The streaming query MUST not be running. If `numPartitions` is
   * the same as the current number of partitions, this is a no-op, and an exception will be
   * thrown.
   *
   * This produces a new microbatch in the checkpoint that contains the repartitioned state i.e.
   * if the last streaming batch was batch `N`, this will create batch `N+1` with the
   * repartitioned state. Note that this new batch doesn't read input data from sources, it only
   * represents the repartition operation. The next time the streaming query is started, it will
   * pick up from this new batch.
   *
   * This will return only when the repartitioning is complete or fails.
   *
   * @note
   *   This operation should only be performed after the streaming query has been stopped. If not,
   *   can lead to undefined behavior or checkpoint corruption.
   * @param checkpointLocation
   *   The checkpoint location of the streaming query, should be the `checkpointLocation` option
   *   on the DataStreamWriter.
   * @param numPartitions
   *   the target number of state partitions.
   * @param enforceExactlyOnceSink
   *   if we shouldn't allow skipping failed batches, to avoid duplicates in exactly once sinks.
   */
  private[spark] def repartition(
      checkpointLocation: String,
      numPartitions: Int,
      enforceExactlyOnceSink: Boolean = true): Unit
}

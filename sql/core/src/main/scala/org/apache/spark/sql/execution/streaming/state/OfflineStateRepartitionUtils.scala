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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqLog, OffsetSeqMetadataBase}
import org.apache.spark.sql.internal.SQLConf

/**
 * Utility methods for offline state repartitioning.
 * They can be called by repartition runner or other streaming components.
 */
object OfflineStateRepartitionUtils {
  /**
   * Determines if a given batch is a repartition batch.
   *
   * @param batchId The ID of the batch to check
   * @param offsetLog The offset log for the query
   */
  def isRepartitionBatch(
      batchId: Long,
      offsetLog: OffsetSeqLog): Boolean = {
    require(batchId >= 0, "Batch ID must be non-negative")
    batchId match {
      // first batch can never be a repartition batch since we require at least one committed batch
      case 0 => false
      case _ =>
        // A repartition batch is a batch where the number of shuffle partitions changed
        // compared to the previous batch.
        val batchOpt = offsetLog.get(batchId)
        require(batchOpt.isDefined, s"Batch $batchId not found in offset log")
        val batch = batchOpt.get

        val prevBatchId = batchId - 1
        val prevBatchOpt = offsetLog.get(prevBatchId)
        require(prevBatchOpt.isDefined, s"Previous batch $prevBatchId not found in offset log")
        val previousBatch = prevBatchOpt.get

        require(batch.metadataOpt.isDefined, s"Batch $batchId metadata not found")
        val shufflePartitions = getShufflePartitions(batch.metadataOpt.get).get

        require(previousBatch.metadataOpt.isDefined,
          s"Previous batch $prevBatchId metadata not found")
        val previousShufflePartitions = getShufflePartitions(previousBatch.metadataOpt.get).get

        previousShufflePartitions != shufflePartitions
    }
  }

  def getShufflePartitions(metadata: OffsetSeqMetadataBase): Option[Int] = {
    metadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).map(_.toInt)
  }
}

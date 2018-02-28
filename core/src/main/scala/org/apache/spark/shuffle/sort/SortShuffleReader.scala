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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.{ExternalMerger, MemoryShuffleBlock}


/**
 * SortShuffleReader merges and aggregates shuffle data that has already been sorted within each
 * map output block.
 */
private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")


  override def read(): Iterator[Product2[K, C]] = {
    val blockFetcherItr = new ShuffleBlockFetcherIterator[ManagedBuffer](
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      (_, in) => in,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true),
      rawFetcher = true)

    val merger = new ExternalMerger(
      context,
      blockManager,
      SparkEnv.get.serializerManager,
      handle)

    blockFetcherItr.foreach { case (blockId, buf) =>
      merger.insertBlock(MemoryShuffleBlock(blockId, buf))
    }

    val completionItr = merger.merge()

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      completionItr.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // Update the spill metrics
    context.taskMetrics().incMemoryBytesSpilled(merger.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(merger.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(merger.peakMemoryUsedBytes)

    new InterruptibleIterator(context, metricIter.map(p => (p._1, p._2)))
  }
}

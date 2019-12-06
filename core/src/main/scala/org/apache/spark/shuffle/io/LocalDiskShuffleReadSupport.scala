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

package org.apache.spark.shuffle.io

import scala.collection.JavaConverters._

import org.apache.spark.{MapOutputTracker, SparkConf, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.api.{ShuffleBlockInfo, ShuffleBlockInputStream}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockFetcherIterator, ShuffleBlockId}

class LocalDiskShuffleReadSupport(
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    serializerManager: SerializerManager,
    conf: SparkConf) {

  private val maxBytesInFlight = conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024
  private val maxReqsInFlight = conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT)
  private val maxBlocksInFlightPerAddress =
    conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS)
  private val maxReqSizeShuffleToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)
  private val detectCorrupt = conf.get(config.SHUFFLE_DETECT_CORRUPT)
  private val detectCorruptMemory = conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY)

  def getPartitionReaders(
      blockMetadata: java.lang.Iterable[ShuffleBlockInfo],
      doBatchFetch: Boolean): java.lang.Iterable[ShuffleBlockInputStream] = {
    val iterableToReturn = if (blockMetadata.asScala.isEmpty) {
      Iterable.empty
    } else {
      new ShuffleBlockFetcherIterable(
        TaskContext.get(),
        blockManager,
        serializerManager,
        maxBytesInFlight,
        maxReqsInFlight,
        maxBlocksInFlightPerAddress,
        maxReqSizeShuffleToMem,
        detectCorrupt,
        detectCorruptMemory,
        shuffleMetrics = TaskContext.get().taskMetrics().createTempShuffleReadMetrics(),
        blockMetadata,
        mapOutputTracker,
        doBatchFetch
      )
    }
    iterableToReturn.asJava
  }
}

private class ShuffleBlockFetcherIterable(
    context: TaskContext,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorruption: Boolean,
    detectCorruptMemory: Boolean,
    shuffleMetrics: ShuffleReadMetricsReporter,
    blockMetadata: java.lang.Iterable[ShuffleBlockInfo],
    mapOutputTracker: MapOutputTracker,
    doBatchFetch: Boolean) extends Iterable[ShuffleBlockInputStream] {

  override def iterator: Iterator[ShuffleBlockInputStream] = {
    new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      blockMetadata.asScala
        .groupBy(shuffleBlockInfo => shuffleBlockInfo.getShuffleLocation.get())
        .map{ case (blockManagerId, shuffleBlockInfos) =>
          (blockManagerId,
            shuffleBlockInfos.map(info =>
              (ShuffleBlockId(
                info.getShuffleId,
                info.getMapId,
                info.getReduceId).asInstanceOf[BlockId],
                info.getLength, info.getMapId)).toSeq)
        }
        .iterator,
      serializerManager.wrapStream,
      maxBytesInFlight,
      maxReqsInFlight,
      maxBlocksInFlightPerAddress,
      maxReqSizeShuffleToMem,
      detectCorruption,
      detectCorruptMemory,
      shuffleMetrics,
      doBatchFetch).toCompletionIterator
      .map{ case(blockId, inputStream) => new ShuffleBlockInputStream(blockId, inputStream)}
  }
}

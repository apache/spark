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

package org.apache.spark.shuffle

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.api.java.Optional
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.api.{ShuffleBlockInfo, ShuffleExecutorComponents}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the blocks from a shuffle by requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    shuffleExecutorComponents: ShuffleExecutorComponents,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    sparkConf: SparkConf = SparkEnv.get.conf,
    mapIndex: Option[Int] = None,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  private val compressionCodec = CompressionCodec.createCodec(sparkConf)
  private val compressShuffle = sparkConf.get(config.SHUFFLE_COMPRESS)

  private def fetchContinuousBlocksInBatch: Boolean = {
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = sparkConf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(
        CompressionCodec.createCodec(sparkConf))
    } else {
      true
    }
    val useOldFetchProtocol = sparkConf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol.")
    }
    doBatchFetch
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val streamsIterator =
      shuffleExecutorComponents.getPartitionReaders(new Iterable[ShuffleBlockInfo] {
        override def iterator: Iterator[ShuffleBlockInfo] = {
          val blockByAddress = mapIndex match {
            case (Some(mapId)) =>
              mapOutputTracker
                .getMapSizesByMapIndex(
                  handle.shuffleId,
                  mapId,
                  startPartition,
                  endPartition)
            case None =>
              mapOutputTracker
                .getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)
            case (_) => throw new IllegalArgumentException(
              "mapId should be both set or unset")
          }
          blockByAddress
            .flatMap { shuffleLocationInfo =>
              shuffleLocationInfo._2.map { blockInfo =>
                val block = blockInfo._1.asInstanceOf[ShuffleBlockId]
                new ShuffleBlockInfo(
                  block.shuffleId,
                  block.mapId.toInt,
                  block.reduceId,
                  blockInfo._2,
                  Optional.ofNullable(shuffleLocationInfo._1))
              }
            }
        }
      }.asJava, fetchContinuousBlocksInBatch).iterator()

    val retryingWrapedStreams = streamsIterator.asScala.map { rawReaderStream =>
      if (shuffleExecutorComponents.shouldWrapPartitionReaderStream()) {
        if (compressShuffle) {
          compressionCodec.compressedContinuousInputStream(
            serializerManager.wrapForEncryption(rawReaderStream))
        } else {
          serializerManager.wrapForEncryption(rawReaderStream)
        }
      } else {
        // The default implementation checks for corrupt streams, so it will already have
        // decompress/decrypted the bytes, thusly the rawReaderStream can be returned
        rawReaderStream
      }
    }

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = retryingWrapedStreams.flatMap {wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}

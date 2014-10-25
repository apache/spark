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

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.Comparator

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{Logging, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.network.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleReader, BaseShuffleHandle}
import org.apache.spark.shuffle.hash.BlockStoreShuffleFetcher
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.{MergeUtil, TieredDiskMerger}

/**
 * SortShuffleReader merges and aggregates shuffle data that has already been sorted within each
 * map output block.
 *
 * As blocks are fetched, we store them in memory until we fail to acquire space frm the
 * ShuffleMemoryManager. When this occurs, we merge the in-memory blocks to disk and go back to
 * fetching.
 *
 * TieredDiskMerger is responsible for managing the merged on-disk blocks and for supplying an
 * iterator with their merged contents. The final iterator that is passed to user code merges this
 * on-disk iterator with the in-memory blocks that have not yet been spilled.
 */
private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  case class MemoryBlock(blockId: BlockId, blockData: ManagedBuffer)

  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = _

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val memoryBlocks = new ArrayBuffer[MemoryBlock]()

  private val tieredMerger = new TieredDiskMerger(conf, dep, context)

  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      h1 - h2
    }
  })

  override def read(): Iterator[Product2[K, C]] = {
    if (!dep.mapSideCombine && dep.aggregator.isDefined) {
      val iter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser)
      new InterruptibleIterator(context,
        dep.aggregator.get.combineValuesByKey(iter, context))
    } else {
      sortShuffleRead()
    }
  }

  private def sortShuffleRead(): Iterator[Product2[K, C]] = {
    tieredMerger.start()

    for ((blockId, blockData) <- fetchRawBlocks()) {
      if (blockData.isEmpty) {
        throw new IllegalStateException(s"block $blockId is empty for unknown reason")
      }

      memoryBlocks += MemoryBlock(blockId, blockData.get)

      // Try to fit block in memory. If this fails, merge in-memory blocks to disk.
      val blockSize = blockData.get.size
      val granted = shuffleMemoryManager.tryToAcquire(blockData.get.size)
      if (granted < blockSize) {
        shuffleMemoryManager.release(granted)

        val itrGroup = memoryBlocksToIterators()
        val partialMergedIter =
          MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

        // Write merged blocks to disk
        val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
        val fos = new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)
        blockManager.dataSerializeStream(tmpBlockId, fos, partialMergedIter, ser)
        tieredMerger.registerOnDiskBlock(tmpBlockId, file)

        for (block <- memoryBlocks) {
          shuffleMemoryManager.release(block.blockData.size)
        }
        memoryBlocks.clear()
      }

      shuffleRawBlockFetcherItr.currentResult = null
    }
    tieredMerger.doneRegisteringOnDiskBlocks()

    // Merge on-disk blocks with in-memory blocks to directly feed to the reducer.
    val finalItrGroup = memoryBlocksToIterators() ++ Seq(tieredMerger.readMerged())
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    // Release the in-memory block and on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, () => {
        memoryBlocks.foreach(block => shuffleMemoryManager.release(block.blockData.size))
        memoryBlocks.clear()
      })

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
  }

  def memoryBlocksToIterators(): Seq[Iterator[Product2[K, C]]] = {
    memoryBlocks.map{ case MemoryBlock(id, buf) =>
      blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        .asInstanceOf[Iterator[Product2[K, C]]]
    }
  }

  override def stop(): Unit = ???

  private def fetchRawBlocks(): Iterator[(BlockId, Option[ManagedBuffer])] = {
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(handle.shuffleId, startPartition)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]()
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }
    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => (ShuffleBlockId(handle.shuffleId, s._1, startPartition), s._2)))
    }
    var numMapBlocks = 0
    blocksByAddress.foreach { case (_, blocks) =>
      blocks.foreach { case (_, len) => if (len > 0) numMapBlocks += 1 }
    }
    val threadId = Thread.currentThread.getId
    logInfo(s"Fetching $numMapBlocks blocks for $threadId")

    shuffleRawBlockFetcherItr = new ShuffleRawBlockFetcherIterator(
      context,
      SparkEnv.get.blockTransferService,
      blockManager,
      blocksByAddress,
      SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)

    val completionItr = CompletionIterator[
      (BlockId, Option[ManagedBuffer]),
      Iterator[(BlockId, Option[ManagedBuffer])]](shuffleRawBlockFetcherItr,
      () => context.taskMetrics.updateShuffleReadMetrics())

    new InterruptibleIterator[(BlockId, Option[ManagedBuffer])](context, completionItr)
  }
}

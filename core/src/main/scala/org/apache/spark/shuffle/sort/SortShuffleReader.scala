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

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException, ShuffleReader}
import org.apache.spark.storage._
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.util.collection.{MergeUtil, TieredDiskMerger}

/**
 * SortShuffleReader merges and aggregates shuffle data that has already been sorted within each
 * map output block.
 *
 * As blocks are fetched, we store them in memory until we fail to acquire space from the
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

  /** Manage the fetched in-memory shuffle block and related buffer*/
  case class MemoryShuffleBlock(blockId: BlockId, blockData: ManagedBuffer)

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  /** Shuffle block fetcher iterator */
  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = _

  /** Number of bytes left to fetch */
  private var unfetchedBytes: Long = _

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  /** Number of bytes spilled in memory and on disk */
  private var _diskBytesSpilled = 0L
  private var _memoryBytesSpilled = 0L

  /** ArrayBuffer to store in-memory shuffle blocks */
  private val inMemoryBlocks = new Queue[MemoryShuffleBlock]()

  /** Manage the BlockManagerId and related shuffle blocks */
  private var  statuses: Array[(BlockManagerId, Long)] = _

  /** keyComparator for mergeSort, id keyOrdering is not available,
    * using hashcode of key to compare */
  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  /** A merge thread to merge on-disk blocks */
  private val tieredMerger = new TieredDiskMerger(conf, dep, keyComparator, context)

  override def read(): Iterator[Product2[K, C]] = {
    tieredMerger.start()

    for ((blockId, blockOption) <- fetchRawBlocks()) {
      val blockData = blockOption match {
        case Success(block) => block
        case Failure(e) =>
          blockId match {
            case ShuffleBlockId (shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException (address, shufId.toInt, mapId.toInt, startPartition,
                Utils.exceptionString (e))
            case _ =>
              throw new SparkException (
                s"Failed to get block $blockId, which is not a shuffle block", e)
          }
      }

      inMemoryBlocks += MemoryShuffleBlock(blockId, blockData)
      shuffleRawBlockFetcherItr.currentResult = null

      // Try to fit block in memory. If this fails, merge in-memory blocks to disk.
      val blockSize = blockData.size
      val granted = shuffleMemoryManager.tryToAcquire(blockSize)
      val block = MemoryShuffleBlock(blockId, blockData)
      if (granted < blockSize) {
        logInfo(s"Granted $granted memory is not enough to store shuffle block id $blockId, " +
          s"block size $blockSize, try to consolidate in-memory blocks to release the memory")

        shuffleMemoryManager.release(granted)

        // Write merged blocks to disk
        val (tmpBlockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
        val fos = new FileOutputStream(file)
        val bos = new BufferedOutputStream(fos, fileBufferSize)

        if (inMemoryBlocks.size > 1) {
          val itrGroup = inMemoryBlocksToIterators()
          val partialMergedItr =
            MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
          // TODO. change this into objectWriter
          // TODO. Track the memory and disk spill
          blockManager.dataSerializeStream(tmpBlockId, bos, partialMergedItr, ser)
        } else {
          val buffer = inMemoryBlocks.map(_.blockData.nioByteBuffer()).head
          val channel = fos.getChannel
          while (buffer.hasRemaining) {
            channel.write(buffer)
          }
          channel.close()
        }

        tieredMerger.registerOnDiskBlock(tmpBlockId, file)

        logInfo(s"Merge ${inMemoryBlocks.size} in-memory blocks into file ${file.getName}")

        for (block <- inMemoryBlocks) {
          block.blockData.release()
          if (block.blockId != blockId) {
            shuffleMemoryManager.release(block.blockData.size)
          }
        }
        inMemoryBlocks.clear()
      }
    }
    assert(unfetchedBytes == 0)

    tieredMerger.doneRegisteringOnDiskBlocks()

    // Merge on-disk blocks with in-memory blocks to directly feed to the reducer.
    val finalItrGroup = inMemoryBlocksToIterators(inMemoryBlocks) ++ Seq(tieredMerger.readMerged())
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    // Release the in-memory block when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, {
        inMemoryBlocks.foreach { block =>
          block.blockData.release()
          shuffleMemoryManager.release(block.blockData.size)
        }
        inMemoryBlocks.clear()
      })

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
  }

  def spillInMemoryBlocks(tippingBlock: MemoryShuffleBlock): Unit = {
    // Write merged blocks to disk
    val (tmpBlockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
    val fos = new FileOutputStream(file)
    val bos = new BufferedOutputStream(fos, fileBufferSize)

    // If the remaining unfetched data would fit inside our current allocation, we don't want to
    // waste time spilling blocks beyond the space needed for it.
    var bytesToSpill = unfetchedBytes
    val blocksToSpill = new ArrayBuffer[MemoryShuffleBlock]()
    blocksToSpill += tippingBlock
    bytesToSpill -= tippingBlock.blockData.size
    while (bytesToSpill > 0 && !inMemoryBlocks.isEmpty) {
      val block = inMemoryBlocks.dequeue()
      blocksToSpill += block
      bytesToSpill -= block.blockData.size
    }

    if (blocksToSpill.size > 1) {
      val itrGroup = inMemoryBlocksToIterators(blocksToSpill)
      val partialMergedItr =
        MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
      blockManager.dataSerializeStream(tmpBlockId, bos, partialMergedItr, ser)
    } else {
      val buffer = blocksToSpill.map(_.blockData.nioByteBuffer()).head
      val channel = fos.getChannel
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      channel.close()
    }

    tieredMerger.registerOnDiskBlock(tmpBlockId, file)

    logInfo(s"Merged ${blocksToSpill.size} in-memory blocks into file ${file.getName}")

    for (block <- blocksToSpill) {
      block.blockData.release()
      if (block != tippingBlock) {
        shuffleMemoryManager.release(block.blockData.size)
      }
    }
  }

  private def inMemoryBlocksToIterators(blocks: Seq[MemoryShuffleBlock])
    : Seq[Iterator[Product2[K, C]]] = {
    blocks.map{ case MemoryShuffleBlock(id, buf) =>
      blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        .asInstanceOf[Iterator[Product2[K, C]]]
    }
  }

  private def fetchRawBlocks(): Iterator[(BlockId, Try[ManagedBuffer])] = {
    statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(handle.shuffleId, startPartition)

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]()
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress = splitsByAddress.toSeq.map { case (address, splits) =>
      val blocks = splits.map { s =>
        (ShuffleBlockId(handle.shuffleId, s._1, startPartition), s._2)
      }
      (address, blocks.toSeq)
    }
    unfetchedBytes = blocksByAddress.flatMap(a => a._2.map(b => b._2)).sum

    shuffleRawBlockFetcherItr = new ShuffleRawBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)

    val completionItr = CompletionIterator[
      (BlockId, Try[ManagedBuffer]),
      Iterator[(BlockId, Try[ManagedBuffer])]](shuffleRawBlockFetcherItr,
        context.taskMetrics.updateShuffleReadMetrics())

    new InterruptibleIterator[(BlockId, Try[ManagedBuffer])](context, completionItr)
  }
}

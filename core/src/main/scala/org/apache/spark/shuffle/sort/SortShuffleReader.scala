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

import java.io.{BufferedOutputStream, FileOutputStream, File}
import java.util.Comparator
import java.util.concurrent.{CountDownLatch, TimeUnit, LinkedBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{Logging, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.network.ManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleReader, BaseShuffleHandle}
import org.apache.spark.shuffle.hash.BlockStoreShuffleFetcher
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * SortShuffleReader assumes that the records within each block are sorted by key, and essentially
 * performs a huge tiered merge between all the map output blocks for a partition.
 *
 * As blocks are fetched, stores them on disk or in memory depending on their size. A single
 * background thread merges these blocks and writes the results to disk.
 *
 * The shape of the tiered merge is controlled by a single parameter, maxMergeWidth, which limits
 * the number of blocks merged at once. At any point during its operation, we can think of the
 * shuffle reader being at a merge "level". The first level merges the fetched map output blocks,
 * at most maxMergeWidth at a time. If the merged blocks created from the first level merge exceed
 * maxMergeWidth, a second level merges the results of the first, and so on. When at last the total
 * blocks fall beneath maxMergeWidth, the final merge feeds into the iterator that read(...)
 * returns.
 */
private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  sealed trait ShuffleBlock
  case class MemoryBlock(blockId: BlockId, blockData: ManagedBuffer) extends ShuffleBlock
  case class FileBlock(blockId: BlockId, mappedFile: File) extends ShuffleBlock

  /**
   * Blocks awaiting merge at the current level. All fetched blocks go here immediately. After the
   * first level merge of fetched blocks has completed, more levels of merge may be required in
   * order to not overwhelm the final merge. In those cases, the already-merged blocks awaiting
   * further merges will go here as well.
   */
  private val blocksAwaitingMerge = new LinkedBlockingQueue[ShuffleBlock]()
  /** Blocks already merged at the current level. */
  private val mergedBlocks = new LinkedBlockingQueue[ShuffleBlock]()
  /** The total number of map output blocks to be fetched. */
  private var numMapBlocks: Int = 0
  private val mergeFinished = new CountDownLatch(1)
  private val mergingThread = new MergingThread()
  private val threadId = Thread.currentThread().getId
  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = _

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val maxMergeWidth = conf.getInt("spark.shuffle.maxMergeWidth", 100)
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

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
    mergingThread.setDaemon(true)
    mergingThread.start()

    for ((blockId, blockData) <- fetchRawBlocks()) {
      if (blockData.isEmpty) {
        throw new IllegalStateException(s"block $blockId is empty for unknown reason")
      }

      val blockSize = blockData.get.size
      // Try to fit block in memory. If this fails, spill it to disk.
      val granted = shuffleMemoryManager.tryToAcquire(blockSize)
      if (granted < blockSize) {
        shuffleMemoryManager.release(granted)
        logInfo(s"Granted memory $granted for block less than its size $blockSize, " +
          s"spilling data to file")
        val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
        val channel = new FileOutputStream(file).getChannel()
        val byteBuffer = blockData.get.nioByteBuffer()
        while (byteBuffer.remaining() > 0) {
          channel.write(byteBuffer)
        }
        channel.close()
        blocksAwaitingMerge.offer(FileBlock(tmpBlockId, file))
      } else {
        blocksAwaitingMerge.offer(MemoryBlock(blockId, blockData.get))
      }

      shuffleRawBlockFetcherItr.currentResult = null
    }

    mergeFinished.await()

    // Merge the final group for combiner to directly feed to the reducer
    val finalMergedPartArray = mergedBlocks.toArray(new Array[ShuffleBlock](mergedBlocks.size()))
    val finalItrGroup = blocksToRecordIterators(finalMergedPartArray)
    val mergedItr = if (dep.aggregator.isDefined) {
      ExternalSorter.mergeWithAggregation(finalItrGroup, dep.aggregator.get.mergeCombiners,
        keyComparator, dep.keyOrdering.isDefined)
    } else {
      ExternalSorter.mergeSort(finalItrGroup, keyComparator)
    }

    mergedBlocks.clear()

    // Release the shuffle used memory of this thread
    shuffleMemoryManager.releaseMemoryForThisThread()

    // Release the in-memory block and on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseUnusedShuffleBlock(finalMergedPartArray))

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
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
    blocksByAddress.foreach { case (_, blocks) =>
      blocks.foreach { case (_, len) => if (len > 0) numMapBlocks += 1 }
    }
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

  private def blocksToRecordIterators(shufflePartGroup: Seq[ShuffleBlock])
      : Seq[Iterator[Product2[K, C]]] = {
     shufflePartGroup.map { part =>
      val itr = part match {
        case MemoryBlock(id, buf) =>
          // Release memory usage
          shuffleMemoryManager.release(buf.size, threadId)
          blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        case FileBlock(id, file) =>
          val blockData = blockManager.diskStore.getBytes(id).getOrElse(
            throw new IllegalStateException(s"cannot get data from block $id"))
          blockManager.dataDeserialize(id, blockData, ser)
      }
      itr.asInstanceOf[Iterator[Product2[K, C]]]
    }.toSeq
  }


  /**
   * Release the left in-memory buffer or on-disk file after merged.
   */
  private def releaseUnusedShuffleBlock(shufflePartGroup: Array[ShuffleBlock]): Unit = {
    shufflePartGroup.map { part =>
      part match {
        case MemoryBlock(id, buf) => buf.release()
        case FileBlock(id, file) =>
          try {
            file.delete()
          } catch {
            // Swallow the exception
            case e: Throwable => logWarning(s"Unexpected errors when deleting file: ${
              file.getAbsolutePath}", e)
          }
      }
    }
  }

  private class MergingThread extends Thread {
    var remainingToMergeAtLevel: Int = _

    override def run() {
      remainingToMergeAtLevel = numMapBlocks
      while (remainingToMergeAtLevel > 0) {
        mergeLevel()
        assert(blocksAwaitingMerge.size() == 0)
        assert(remainingToMergeAtLevel == 0)
        if (mergedBlocks.size() > maxMergeWidth) {
          // End of the current merge level, but not yet ready to proceed to the final merge.
          // Swap the merged group and merging group to proceed to the next merge level,
          blocksAwaitingMerge.addAll(mergedBlocks)
          remainingToMergeAtLevel = blocksAwaitingMerge.size()
          mergedBlocks.clear()
        }
      }
      mergeFinished.countDown()
    }

    /**
     * Carry out the current merge level. I.e. move all blocks out of blocksAwaitingMerge, either by
     * merging them or placing them directly in mergedBlocks.
     */
    private def mergeLevel() {
      while (remainingToMergeAtLevel > 0) {
        if (remainingToMergeAtLevel < maxMergeWidth) {
          // If the remaining blocks awaiting merge at this level don't exceed the maxMergeWidth,
          // pass them all on to the next level.
          while (remainingToMergeAtLevel > 0) {
            val part = blocksAwaitingMerge.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              mergedBlocks.offer(part)
              remainingToMergeAtLevel -= 1
            }
          }
        } else {
          // Because the remaining blocks awaiting merge at this level exceed the maxMergeWidth, a
          // merge is required.

          // Pick a number of blocks to merge that (1) is <= maxMergeWidth, (2) if possible,
          // ensures that the number of blocks in the next level be <= maxMergeWidth so that it can
          // be the final merge, and (3) is as small as possible.
          val mergedBlocksHeadroomAfterMerge = (maxMergeWidth - mergedBlocks.size() - 1)
          // Ideal in the sense that we would move on to the next level after this merge and that
          // level would have exactly maxMergeWidth blocks to merge. This can never be negative,
          // because, at this point, we know blocksAwaitingMerge is at least maxMergeWidth.
          val idealWidth = remainingToMergeAtLevel - mergedBlocksHeadroomAfterMerge
          val mergeWidth = math.min(idealWidth, maxMergeWidth)

          val blocksToMerge = ArrayBuffer[ShuffleBlock]()
          while (blocksToMerge.size < mergeWidth) {
            val part = blocksAwaitingMerge.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              blocksToMerge += part
              remainingToMergeAtLevel -= 1
            }
          }

          // Merge the blocks
          val itrGroup = blocksToRecordIterators(blocksToMerge)
          val partialMergedIter = if (dep.aggregator.isDefined) {
            ExternalSorter.mergeWithAggregation(itrGroup, dep.aggregator.get.mergeCombiners,
              keyComparator, dep.keyOrdering.isDefined)
          } else {
            ExternalSorter.mergeSort(itrGroup, keyComparator)
          }
          // Write merged blocks to disk
          val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
          val fos = new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)
          blockManager.dataSerializeStream(tmpBlockId, fos, partialMergedIter, ser)
          logInfo(s"Merged ${blocksToMerge.size} blocks into file ${file.getName}")

          releaseUnusedShuffleBlock(blocksToMerge.toArray)
          mergedBlocks.add(FileBlock(tmpBlockId, file))
        }
      }
    }
  }
}

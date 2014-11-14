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

package org.apache.spark.util.collection

import java.io.File
import java.util.Comparator
import java.util.concurrent.{PriorityBlockingQueue, CountDownLatch}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage.BlockId
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.CompletionIterator

/**
 * Explain the boundaries of where this starts and why we have second thread
 *
 * Manages blocks of sorted data on disk that need to be merged together. Carries out a tiered
 * merge that will never merge more than spark.shuffle.maxMergeFactor segments at a time.  Except for
 * the final merge, which merges disk blocks to a returned iterator, TieredDiskMerger merges blocks
 * from disk to disk.
 *
 * TieredDiskMerger carries out disk-to-disk merges in a background thread that can run concurrently
 * with blocks being deposited on disk.
 *
 * When deciding which blocks to merge, it first tries to minimize the number of blocks, and then
 * the size of the blocks chosen.
 */
private[spark] class TieredDiskMerger[K, C](
    conf: SparkConf,
    dep: ShuffleDependency[K, _, C],
    keyComparator: Comparator[K],
    context: TaskContext) extends Logging {

  /** Manage the on-disk shuffle block and related file, file size */
  case class DiskShuffleBlock(blockId: BlockId, file: File, len: Long)
      extends Comparable[DiskShuffleBlock] {
    def compareTo(o: DiskShuffleBlock): Int = len.compare(o.len)
  }

  private val maxMergeFactor = conf.getInt("spark.shuffle.maxMergeFactor", 100)
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)

  /** Number of bytes spilled on disk */
  private var _diskBytesSpilled: Long = 0L

  /** PriorityQueue to store the on-disk merging blocks, blocks are merged by size ordering */
  private val onDiskBlocks = new PriorityBlockingQueue[DiskShuffleBlock]()

  /** A merging thread to merge on-disk blocks */
  private val diskToDiskMerger = new DiskToDiskMerger

  /** Signal to block/signal the merge action */
  private val mergeReadyMonitor = new AnyRef()

  private val mergeFinished = new CountDownLatch(1)

  @volatile private var doneRegistering = false

  def registerOnDiskBlock(blockId: BlockId, file: File): Unit = {
    assert(!doneRegistering)
    onDiskBlocks.put(new DiskShuffleBlock(blockId, file, file.length()))

    mergeReadyMonitor.synchronized {
      if (shouldMergeNow()) {
        mergeReadyMonitor.notify()
      }
    }
  }

  def diskBytesSpilled: Long = _diskBytesSpilled

  /**
   * Notify the merger that no more on disk blocks will be registered.
   */
  def doneRegisteringOnDiskBlocks(): Unit = {
    doneRegistering = true
    mergeReadyMonitor.synchronized {
      mergeReadyMonitor.notify()
    }
  }

  def readMerged(): Iterator[Product2[K, C]] = {
    mergeFinished.await()

    // Merge the final group for combiner to directly feed to the reducer
    val finalMergedBlocks = onDiskBlocks.toArray(new Array[DiskShuffleBlock](onDiskBlocks.size()))
    val finalItrGroup = onDiskBlocksToIterators(finalMergedBlocks)
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    onDiskBlocks.clear()

    // Release the on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseShuffleBlocks(finalMergedBlocks))

    new InterruptibleIterator(context, completionItr)
  }

  def start() {
    diskToDiskMerger.start()
  }

  /**
   * Release the left in-memory buffer or on-disk file after merged.
   */
  private def releaseShuffleBlocks(onDiskShuffleGroup: Array[DiskShuffleBlock]): Unit = {
    onDiskShuffleGroup.map { case DiskShuffleBlock(_, file, _) =>
      try {
        logDebug(s"Deleting the unused temp shuffle file: ${file.getName}")
        file.delete()
      } catch {
        // Swallow the exception
        case e: Exception => logWarning(s"Unexpected errors when deleting file: ${
          file.getAbsolutePath}", e)
      }
    }
  }

  private def onDiskBlocksToIterators(shufflePartGroup: Seq[DiskShuffleBlock])
    : Seq[Iterator[Product2[K, C]]] = {
    shufflePartGroup.map { case DiskShuffleBlock(id, _, _) =>
      blockManager.diskStore.getValues(id, ser).get.asInstanceOf[Iterator[Product2[K, C]]]
    }.toSeq
  }

  /**
   * Whether we should carry out a disk-to-disk merge now or wait for more blocks or a done
   * registering notification to come in.
   *
   * We want to avoid merging more blocks than we need to. Our last disk-to-disk merge may
   * merge fewer than maxMergeFactor blocks, as its only requirement is that, after it has been
   * carried out, <= maxMergeFactor blocks remain. E.g., if maxMergeFactor is 10, no more blocks
   * will come in, and we have 13 on-disk blocks, the optimal number of blocks to include in the
   * last disk-to-disk merge is 4.
   *
   * While blocks are still coming in, we don't know the optimal number, so we hold off until we
   * either receive the notification that no more blocks are coming in, or until maxMergeFactor
   * merge is required no matter what.
   *
   * E.g. if maxMergeFactor is 10 and we have 19 or more on-disk blocks, a 10-block merge will put
   * us at 10 or more blocks, so we might as well carry it out now.
   */
  private def shouldMergeNow(): Boolean = doneRegistering ||
    onDiskBlocks.size() >= maxMergeFactor * 2 - 1

  private final class DiskToDiskMerger extends Thread {
    setName(s"tiered-merge-thread-${Thread.currentThread().getId}")
    setDaemon(true)

    override def run() {
      // Each iteration of this loop carries out a disk-to-disk merge. We remain in this loop until
      // no more disk-to-disk merges need to be carried out, i.e. when no more blocks are coming in
      // and the final merge won't need to merge more than maxMergeFactor blocks.
      while (!doneRegistering || onDiskBlocks.size() > maxMergeFactor) {
        while (!shouldMergeNow()) {
          mergeReadyMonitor.synchronized {
            mergeReadyMonitor.wait()
          }
        }

        if (onDiskBlocks.size() > maxMergeFactor) {
          val blocksToMerge = new ArrayBuffer[DiskShuffleBlock]()
          // Try to pick the smallest merge width that will result in the next merge being the final
          // merge.
          val mergeFactor = math.min(onDiskBlocks.size - maxMergeFactor + 1, maxMergeFactor)
          (0 until mergeFactor).foreach {
            blocksToMerge += onDiskBlocks.take()
          }

          // Merge the blocks
          val itrGroup = onDiskBlocksToIterators(blocksToMerge)
          val partialMergedItr =
            MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
          // Write merged blocks to disk
          val (tmpBlockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
          val curWriteMetrics = new ShuffleWriteMetrics()
          var writer =
            blockManager.getDiskWriter(tmpBlockId, file, ser, fileBufferSize, curWriteMetrics)
          var success = false

          try {
            partialMergedItr.foreach(p => writer.write(p))
            success = true
          } finally {
            if (!success) {
              if (writer != null) {
                writer.revertPartialWritesAndClose()
                writer = null
              }
              if (file.exists()) {
                file.delete()
              }
            } else {
              writer.commitAndClose()
              writer = null
            }
            releaseShuffleBlocks(blocksToMerge.toArray)
          }
          _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten

          logInfo(s"Merged ${blocksToMerge.size} on-disk blocks into file ${file.getName}")

          onDiskBlocks.add(DiskShuffleBlock(tmpBlockId, file, file.length()))
        }
      }

      mergeFinished.countDown()
    }
  }
}

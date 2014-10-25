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

import org.apache.spark._
import org.apache.spark.storage.BlockId
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.CompletionIterator

import java.util.Comparator
import java.util.concurrent.{PriorityBlockingQueue, CountDownLatch}
import java.io.{File, FileOutputStream, BufferedOutputStream}

import scala.collection.mutable.ArrayBuffer

/**
 * Explain the boundaries of where this starts and why we have second thread
 *
 * Manages blocks of sorted data on disk that need to be merged together. Carries out a tiered
 * merge that will never merge more than spark.shuffle.maxMergeWidth segments at a time.  Except for
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
    context: TaskContext) extends Logging {

  case class DiskShuffleBlock(blockId: BlockId, file: File, len: Long)
      extends Comparable[DiskShuffleBlock] {
    def compareTo(o: DiskShuffleBlock): Int = len.compare(o.len)
  }

  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      h1 - h2
    }
  })

  private val maxMergeWidth = conf.getInt("spark.shuffle.maxMergeWidth", 10)
  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)

  private val blocks = new PriorityBlockingQueue[DiskShuffleBlock]()

  private val mergeReadyMonitor = new AnyRef()
  private val mergeFinished = new CountDownLatch(1)

  @volatile private var doneRegistering = false

  def registerOnDiskBlock(blockId: BlockId, file: File): Unit = {
    assert(!doneRegistering)
    blocks.put(new DiskShuffleBlock(blockId, file, file.length()))

    mergeReadyMonitor.synchronized {
      if (shouldMergeNow()) {
        mergeReadyMonitor.notify()
      }
    }
  }

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
    val finalMergedPartArray = blocks.toArray(new Array[DiskShuffleBlock](blocks.size()))
    val finalItrGroup = blocksToRecordIterators(finalMergedPartArray)
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    blocks.clear()

    // Release the in-memory block and on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseShuffleBlocks(finalMergedPartArray))

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))

  }

  def start() {
    new DiskToDiskMergingThread().start()
  }

  /**
   * Release the left in-memory buffer or on-disk file after merged.
   */
  private def releaseShuffleBlocks(shufflePartGroup: Array[DiskShuffleBlock]): Unit = {
    shufflePartGroup.map { case DiskShuffleBlock(id, file, length) =>
      try {
        file.delete()
      } catch {
        // Swallow the exception
        case e: Exception => logWarning(s"Unexpected errors when deleting file: ${
          file.getAbsolutePath}", e)
      }
    }
  }

  private def blocksToRecordIterators(shufflePartGroup: Seq[DiskShuffleBlock])
    : Seq[Iterator[Product2[K, C]]] = {
    shufflePartGroup.map { case DiskShuffleBlock(id, file, length) =>
      val blockData = blockManager.diskStore.getBytes(id).getOrElse(
        throw new IllegalStateException(s"cannot get data from block $id"))
      val itr = blockManager.dataDeserialize(id, blockData, ser)
      itr.asInstanceOf[Iterator[Product2[K, C]]]
    }.toSeq
  }

  /**
   * Whether we should carry out a disk-to-disk merge now or wait for more blocks or a done
   * registering notification to come in.
   *
   * We want to avoid merging more blocks than we need to. Our last disk-to-disk merge may
   * merge fewer than maxMergeWidth blocks, as its only requirement is that, after it has been
   * carried out, <= maxMergeWidth blocks remain. E.g., if maxMergeWidth is 10, no more blocks
   * will come in, and we have 13 on-disk blocks, the optimal number of blocks to include in the
   * last disk-to-disk merge is 4.
   *
   * While blocks are still coming in, we don't know the optimal number, so we hold off until we
   * either receive the notification that no more blocks are coming in, or until maxMergeWidth
   * merge is required no matter what.
   *
   * E.g. if maxMergeWidth is 10 and we have 19 or more on-disk blocks, a 10-block merge will put us
   * at 10 or more blocks, so we might as well carry it out now.
   */
  private def shouldMergeNow(): Boolean = doneRegistering || blocks.size() >= maxMergeWidth * 2 - 1

  private class DiskToDiskMergingThread extends Thread {
    // TODO: there will be more than one of these so we need more unique names?
    setName("tiered-merge-thread")
    setDaemon(true)

    override def run() {
      // Each iteration of this loop carries out a disk-to-disk merge. We remain in this loop until
      // no more disk-to-disk merges need to be carried out, i.e. when no more blocks are coming in
      // and the final merge won't need to merge more than maxMergeWidth blocks.
      while (!doneRegistering || blocks.size() > maxMergeWidth) {
        while (!shouldMergeNow()) {
          mergeReadyMonitor.synchronized {
            mergeReadyMonitor.wait()
          }
        }

        if (blocks.size() > maxMergeWidth) {
          val blocksToMerge = new ArrayBuffer[DiskShuffleBlock]()
          // Try to pick the smallest merge width that will result in the next merge being the final
          // merge.
          val mergeWidth = math.min(blocks.size - maxMergeWidth + 1, maxMergeWidth)
          (0 until mergeWidth).foreach {
            blocksToMerge += blocks.take()
          }

          // Merge the blocks
          val itrGroup = blocksToRecordIterators(blocksToMerge)
          val partialMergedIter =
            MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
          // Write merged blocks to disk
          val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
          val fos = new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)
          blockManager.dataSerializeStream(tmpBlockId, fos, partialMergedIter, ser)
          logInfo(s"Merged ${blocksToMerge.size} on-disk blocks into file ${file.getName}")

          releaseShuffleBlocks(blocksToMerge.toArray)
          blocks.add(DiskShuffleBlock(tmpBlockId, file, file.length()))
        }
      }

      mergeFinished.countDown()
    }
  }
}

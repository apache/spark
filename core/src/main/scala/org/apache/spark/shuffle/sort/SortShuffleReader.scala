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
import java.nio.ByteBuffer
import java.util.Comparator
import java.util.concurrent.{CountDownLatch, TimeUnit, LinkedBlockingQueue}

import org.apache.spark.network.ManagedBuffer

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{Logging, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleReader, BaseShuffleHandle}
import org.apache.spark.shuffle.hash.BlockStoreShuffleFetcher
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  sealed trait ShufflePartition
  case class MemoryPartition(blockId: BlockId, blockData: ManagedBuffer) extends ShufflePartition
  case class FilePartition(blockId: BlockId, mappedFile: File) extends ShufflePartition

  private val mergingGroup = new LinkedBlockingQueue[ShufflePartition]()
  private val mergedGroup = new LinkedBlockingQueue[ShufflePartition]()
  private var numSplits: Int = 0
  private val mergeFinished = new CountDownLatch(1)
  private val mergingThread = new MergingThread()
  private val tid = Thread.currentThread().getId
  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = null

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val ioSortFactor = conf.getInt("spark.shuffle.ioSortFactor", 100)
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
    val rawBlockIterator = fetchRawBlock()

    mergingThread.setNumSplits(numSplits)
    mergingThread.setDaemon(true)
    mergingThread.start()

    for ((blockId, blockData) <- rawBlockIterator) {
      if (blockData.isEmpty) {
        throw new IllegalStateException(s"block $blockId is empty for unknown reason")
      }

      val amountToRequest = blockData.get.size
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)
      val shouldSpill = if (granted < amountToRequest) {
        shuffleMemoryManager.release(granted)
        logInfo(s"Grant memory $granted less than the amount to request $amountToRequest, " +
          s"spilling data to file")
        true
      } else {
        false
      }

      if (!shouldSpill) {
        mergingGroup.offer(MemoryPartition(blockId, blockData.get))
      } else {
        val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
        val channel = new FileOutputStream(file).getChannel()
        val byteBuffer = blockData.get.nioByteBuffer()
        while (byteBuffer.remaining() > 0) {
          channel.write(byteBuffer)
        }
        channel.close()
        mergingGroup.offer(FilePartition(tmpBlockId, file))
      }

      shuffleRawBlockFetcherItr.currentResult = null
    }

    mergeFinished.await()

    // Merge the final group for combiner to directly feed to the reducer
    val finalMergedPartArray = mergedGroup.toArray(new Array[ShufflePartition](mergedGroup.size()))
    val finalItrGroup = getIteratorGroup(finalMergedPartArray)
    val mergedItr = if (dep.aggregator.isDefined) {
      ExternalSorter.mergeWithAggregation(finalItrGroup, dep.aggregator.get.mergeCombiners,
        keyComparator, dep.keyOrdering.isDefined)
    } else {
      ExternalSorter.mergeSort(finalItrGroup, keyComparator)
    }

    mergedGroup.clear()

    // Release the shuffle used memory of this thread
    shuffleMemoryManager.releaseMemoryForThisThread()

    // Release the in-memory block and on-disk file when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseUnusedShufflePartition(finalMergedPartArray))

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
  }

  override def stop(): Unit = ???

  private def fetchRawBlock(): Iterator[(BlockId, Option[ManagedBuffer])] = {
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
      blocks.foreach { case (_, len) => if (len > 0) numSplits += 1 }
    }
    logInfo(s"Fetch $numSplits partitions for $tid")

    shuffleRawBlockFetcherItr = new ShuffleRawBlockFetcherIterator(
      context,
      SparkEnv.get.blockTransferService,
      blockManager,
      blocksByAddress,
      SparkEnv.get.conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)

    val completionItr = CompletionIterator[
      (BlockId, Option[ManagedBuffer]),
      Iterator[(BlockId, Option[ManagedBuffer])]](shuffleRawBlockFetcherItr, {
      context.taskMetrics.updateShuffleReadMetrics()
    })

    new InterruptibleIterator[(BlockId, Option[ManagedBuffer])](context, completionItr)
  }

  private def getIteratorGroup(shufflePartGroup: Array[ShufflePartition])
      : Seq[Iterator[Product2[K, C]]] = {
     shufflePartGroup.map { part =>
      val itr = part match {
        case MemoryPartition(id, buf) =>
          // Release memory usage
          shuffleMemoryManager.release(buf.size, tid)
          blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        case FilePartition(id, file) =>
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
  private def releaseUnusedShufflePartition(shufflePartGroup: Array[ShufflePartition]): Unit = {
    shufflePartGroup.map { part =>
      part match {
        case MemoryPartition(id, buf) => buf.release()
        case FilePartition(id, file) =>
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
    private var isLooped = true
    private var leftTobeMerged = 0

    def setNumSplits(numSplits: Int) {
      leftTobeMerged = numSplits
    }

    override def run() {
      while (isLooped) {
        if (leftTobeMerged < ioSortFactor && leftTobeMerged > 0) {
          var count = leftTobeMerged
          while (count > 0) {
            val part = mergingGroup.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              mergedGroup.offer(part)
              count -= 1
              leftTobeMerged -= 1
            }
          }
        } else if (leftTobeMerged >= ioSortFactor) {
          val mergingPartArray = ArrayBuffer[ShufflePartition]()
          var count = if (numSplits / ioSortFactor > ioSortFactor) {
            ioSortFactor
          } else {
            val mergedSize = mergedGroup.size()
            val left = leftTobeMerged - (ioSortFactor - mergedSize - 1)
            if (left <= ioSortFactor) {
              left
            } else {
              ioSortFactor
            }
          }
          val countCopy = count

          while (count > 0) {
            val part = mergingGroup.poll(100, TimeUnit.MILLISECONDS)
            if (part != null) {
              mergingPartArray += part
              count -= 1
              leftTobeMerged -= 1
            }
          }

          // Merge the partitions
          val itrGroup = getIteratorGroup(mergingPartArray.toArray)
          val partialMergedIter = if (dep.aggregator.isDefined) {
            ExternalSorter.mergeWithAggregation(itrGroup, dep.aggregator.get.mergeCombiners,
              keyComparator, dep.keyOrdering.isDefined)
          } else {
            ExternalSorter.mergeSort(itrGroup, keyComparator)
          }
          // Write merged partitions to disk
          val (tmpBlockId, file) = blockManager.diskBlockManager.createTempBlock()
          val fos = new BufferedOutputStream(new FileOutputStream(file), fileBufferSize)
          blockManager.dataSerializeStream(tmpBlockId, fos, partialMergedIter, ser)
          logInfo(s"Merge $countCopy partitions and write into file ${file.getName}")

          releaseUnusedShufflePartition(mergingPartArray.toArray)
          mergedGroup.add(FilePartition(tmpBlockId, file))
        } else {
          val mergedSize = mergedGroup.size()
          if (mergedSize > ioSortFactor) {
            leftTobeMerged = mergedSize

            // Swap the merged group and merging group and do merge again,
            // since file number is still larger than ioSortFactor
            assert(mergingGroup.size() == 0)
            mergingGroup.addAll(mergedGroup)
            mergedGroup.clear()
          } else {
            assert(mergingGroup.size() == 0)
            isLooped = false
            mergeFinished.countDown()
          }
        }
      }
    }
  }
}

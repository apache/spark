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

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.storage.{BlockId, BlockManager, FileSegment}
import org.apache.spark.util.CompletionIterator

/** Manage the fetched in-memory shuffle block and related buffer */
case class MemoryShuffleBlock(blockId: BlockId, blockData: ManagedBuffer)

/**
 * ExternalMerger for SortShuffleReader, it manage all files during the SortShuffleReader.read
 * and spill to Disk when needed.
 *
 * As blocks are fetched, we store them in memory until we fail to acquire space from the
 * TaskMemoryManager. When this occurs, we merge some in-memory blocks to disk.
 *
 * TieredDiskMerger is responsible for managing the merged on-disk blocks and for supplying an
 * iterator with their merged contents. The final iterator that is passed to user code merges this
 * on-disk iterator with the in-memory blocks that have not yet been spilled.
 */
private[spark] class ExternalMerger[K, C](
    context: TaskContext,
    blockManager: BlockManager,
    serializeManager: SerializerManager,
    handle: BaseShuffleHandle[K, _, C])
  extends Spillable[ArrayBuffer[MemoryShuffleBlock]](context.taskMemoryManager()) {

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val serInstance = dep.serializer.newInstance()

  /** A merge thread to merge on-disk blocks */
  private val tieredMerger = new TieredDiskMerger(conf, dep, dep.keyOrdering.get, context)

  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  /** Queue to store in-memory shuffle blocks */
  private var inMemoryBlocks = new ArrayBuffer[MemoryShuffleBlock]()
  private var inMemorySize = 0L

  /** Number of bytes spilled on disk */
  private var _diskBytesSpilled: Long = 0L

  def diskBytesSpilled: Long = _diskBytesSpilled + tieredMerger.diskBytesSpilled

  /** keyComparator for mergeSort, id keyOrdering is not available,
   * using hashcode of key to compare */
  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  /**
   * Called when we've failed to acquire memory for a block we've just fetched. Figure out how many
   * blocks to spill and then spill them.
   */
  def insertBlock(tippingBlock: MemoryShuffleBlock): Unit = {
    inMemoryBlocks += tippingBlock
    inMemorySize += tippingBlock.blockData.size
    addElementsRead()
    if (maybeSpill(inMemoryBlocks, inMemorySize, 1)) {
      inMemoryBlocks = new ArrayBuffer[MemoryShuffleBlock]()
      inMemorySize = 0
    } else if (tippingBlock.blockData.isDirect) {
      // If the shuffle block is allocated on a direct buffer, copy it to an on-heap buffer,
      // otherwise off heap memory will be increased to the shuffle memory size.
      val onHeapBuffer = ByteBuffer.allocate(tippingBlock.blockData.size.toInt)
      onHeapBuffer.put(tippingBlock.blockData.nioByteBuffer)
      inMemoryBlocks -= tippingBlock
      inMemoryBlocks += MemoryShuffleBlock(tippingBlock.blockId,
        new NioManagedBuffer(onHeapBuffer))
      tippingBlock.blockData.release()
    }
  }

  def merge(): CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]] = {
    // Before merge, register all files
    tieredMerger.doneRegisteringOnDiskBlocks()
    // Merge on-disk blocks with in-memory blocks to directly feed to the reducer.
    val finalItrGroup = inMemoryBlocksToIterators(inMemoryBlocks) ++ Seq(tieredMerger.readMerged())
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    def releaseFinalShuffleMemory(): Unit = {
      inMemoryBlocks.foreach { block =>
        block.blockData.release()
        freeMemory(block.blockData.size)
      }
      inMemoryBlocks.clear()
    }
    context.addTaskCompletionListener(_ => releaseFinalShuffleMemory())

    // Release the in-memory block when iteration is completed.
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseFinalShuffleMemory())
  }

  def inMemoryBlocksToIterators(blocks: Seq[MemoryShuffleBlock]): Seq[Iterator[Product2[K, C]]] = {
    blocks.map{ case MemoryShuffleBlock(id, buf) =>
      val wrappedStream = serializeManager.wrapStream(id, buf.createInputStream())
      serInstance.deserializeStream(wrappedStream).asKeyValueIterator
        .asInstanceOf[Iterator[Product2[K, C]]]
    }
  }

  private def spillSingleBlock(file: File, block: MemoryShuffleBlock): Unit = {
    val fos = new FileOutputStream(file)
    val buffer = block.blockData.nioByteBuffer()
    var channel = fos.getChannel
    var success = false

    try {
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      success = true
    } finally {
      if (channel != null) {
        channel.close()
        channel = null
      }
      if (!success) {
        if (file.exists()) {
          file.delete()
        }
      } else {
        _diskBytesSpilled += file.length()
      }
      // When we spill a single block, it's the single tipping block that we never acquired memory
      // from the shuffle memory manager for, so we don't need to release any memory from there.
      block.blockData.release()
    }
  }

  /**
   * Merge multiple in-memory blocks to a single on-disk file.
   */
  private def spillMultipleBlocks(file: File, tmpBlockId: BlockId,
      blocksToSpill: Seq[MemoryShuffleBlock]): Unit = {
    val itrGroup = inMemoryBlocksToIterators(blocksToSpill)
    val partialMergedItr =
      MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
    val curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(
      tmpBlockId, file, dep.serializer.newInstance(), fileBufferSize, curWriteMetrics)
    var fileSegment: FileSegment = null
    var success = false

    try {
      partialMergedItr.foreach(itr => writer.write(itr._1, itr._2))
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
        fileSegment = writer.commitAndGet()
        writer = null
      }
      for (block <- blocksToSpill) {
        block.blockData.release()
      }
    }
    _diskBytesSpilled += fileSegment.length
  }

  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  override protected def spill(collection: ArrayBuffer[MemoryShuffleBlock]): Unit = {
    val (tmpBlockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
    if (collection.size > 1) {
      spillMultipleBlocks(file, tmpBlockId, collection)
    } else {
      spillSingleBlock(file, collection.head)
    }
    tieredMerger.registerOnDiskBlock(tmpBlockId, file)
    logInfo(s"Merged ${collection.size} in-memory blocks into file ${file.getName}")
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected def forceSpill(): Boolean = {
    logInfo(s"ForceSpill called by TaskMemoryManager," +
      s"spill inMemoryBlocks(size: $inMemorySize) to disk.")
    spill(inMemoryBlocks)
    inMemoryBlocks = new ArrayBuffer[MemoryShuffleBlock]()
    inMemorySize = 0
    true
  }
}

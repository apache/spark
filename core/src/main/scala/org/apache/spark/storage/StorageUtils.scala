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

package org.apache.spark.storage

import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.Map
import scala.collection.mutable

import sun.misc.Unsafe

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.Utils

/**
 * Storage information for each BlockManager.
 *
 * This class assumes BlockId and BlockStatus are immutable, such that the consumers of this
 * class cannot mutate the source of the information. Accesses are not thread-safe.
 */
private[spark] class StorageStatus(
    val blockManagerId: BlockManagerId,
    val maxMemory: Long,
    val maxOnHeapMem: Option[Long],
    val maxOffHeapMem: Option[Long]) {

  /**
   * Internal representation of the blocks stored in this block manager.
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  private case class RddStorageInfo(memoryUsage: Long, diskUsage: Long, level: StorageLevel)
  private val _rddStorageInfo = new mutable.HashMap[Int, RddStorageInfo]

  private case class NonRddStorageInfo(var onHeapUsage: Long, var offHeapUsage: Long,
      var diskUsage: Long)
  private val _nonRddStorageInfo = NonRddStorageInfo(0L, 0L, 0L)

  /** Create a storage status with an initial set of blocks, leaving the source unmodified. */
  def this(
      bmid: BlockManagerId,
      maxMemory: Long,
      maxOnHeapMem: Option[Long],
      maxOffHeapMem: Option[Long],
      initialBlocks: Map[BlockId, BlockStatus]) = {
    this(bmid, maxMemory, maxOnHeapMem, maxOffHeapMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   * Return the blocks stored in this block manager.
   *
   * @note This is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * contains, get, and size.
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks

  /**
   * Return the RDD blocks stored in this block manager.
   *
   * @note This is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * getting the memory, disk, and off-heap memory sizes occupied by this RDD.
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }

  /** Add the given block to this storage status. If it already exists, overwrite it. */
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /**
   * Return the given block stored in this block manager in O(1) time.
   *
   * @note This is much faster than `this.blocks.get`, which is O(blocks) time.
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).flatMap(_.get(blockId))
      case _ =>
        _nonRddBlocks.get(blockId)
    }
  }

  /** Return the max memory can be used by this block manager. */
  def maxMem: Long = maxMemory

  /** Return the memory remaining in this block manager. */
  def memRemaining: Long = maxMem - memUsed

  /** Return the memory used by this block manager. */
  def memUsed: Long = onHeapMemUsed.getOrElse(0L) + offHeapMemUsed.getOrElse(0L)

  /** Return the on-heap memory remaining in this block manager. */
  def onHeapMemRemaining: Option[Long] =
    for (m <- maxOnHeapMem; o <- onHeapMemUsed) yield m - o

  /** Return the off-heap memory remaining in this block manager. */
  def offHeapMemRemaining: Option[Long] =
    for (m <- maxOffHeapMem; o <- offHeapMemUsed) yield m - o

  /** Return the on-heap memory used by this block manager. */
  def onHeapMemUsed: Option[Long] = onHeapCacheSize.map(_ + _nonRddStorageInfo.onHeapUsage)

  /** Return the off-heap memory used by this block manager. */
  def offHeapMemUsed: Option[Long] = offHeapCacheSize.map(_ + _nonRddStorageInfo.offHeapUsage)

  /** Return the memory used by on-heap caching RDDs */
  def onHeapCacheSize: Option[Long] = maxOnHeapMem.map { _ =>
    _rddStorageInfo.collect {
      case (_, storageInfo) if !storageInfo.level.useOffHeap => storageInfo.memoryUsage
    }.sum
  }

  /** Return the memory used by off-heap caching RDDs */
  def offHeapCacheSize: Option[Long] = maxOffHeapMem.map { _ =>
    _rddStorageInfo.collect {
      case (_, storageInfo) if storageInfo.level.useOffHeap => storageInfo.memoryUsage
    }.sum
  }

  /** Return the disk space used by this block manager. */
  def diskUsed: Long = _nonRddStorageInfo.diskUsage + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** Return the disk space used by the given RDD in this block manager in O(1) time. */
  def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_.diskUsage).getOrElse(0L)

  /**
   * Update the relevant storage info, taking into account any existing status for this block.
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val level = newBlockStatus.storageLevel

    // Compute new info from old info
    val (oldMem, oldDisk) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case RddStorageInfo(mem, disk, _) => (mem, disk) }
          .getOrElse((0L, 0L))
      case _ if !level.useOffHeap =>
        (_nonRddStorageInfo.onHeapUsage, _nonRddStorageInfo.diskUsage)
      case _ =>
        (_nonRddStorageInfo.offHeapUsage, _nonRddStorageInfo.diskUsage)
    }
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)

    // Set the correct info
    blockId match {
      case RDDBlockId(rddId, _) =>
        // If this RDD is no longer persisted, remove it
        if (newMem + newDisk == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = RddStorageInfo(newMem, newDisk, level)
        }
      case _ =>
        if (!level.useOffHeap) {
          _nonRddStorageInfo.onHeapUsage = newMem
        } else {
          _nonRddStorageInfo.offHeapUsage = newMem
        }
        _nonRddStorageInfo.diskUsage = newDisk
    }
  }
}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils extends Logging {

  private val bufferCleaner: ByteBuffer => Unit = {
    val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
    buffer: ByteBuffer => unsafe.invokeCleaner(buffer)
  }

  /**
   * Attempt to clean up a ByteBuffer if it is direct or memory-mapped. This uses an *unsafe* Sun
   * API that will cause errors if one attempts to read from the disposed buffer. However, neither
   * the bytes allocated to direct buffers nor file descriptors opened for memory-mapped buffers put
   * pressure on the garbage collector. Waiting for garbage collection may lead to the depletion of
   * off-heap memory or huge numbers of open files. There's unfortunately no standard API to
   * manually dispose of these kinds of buffers.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Disposing of $buffer")
      bufferCleaner(buffer)
    }
  }

  /**
   * Get the port used by the external shuffle service. In Yarn mode, this may be already be
   * set through the Hadoop configuration as the server is launched in the Yarn NM.
   */
  def externalShuffleServicePort(conf: SparkConf): Int = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, config.SHUFFLE_SERVICE_PORT.key,
      config.SHUFFLE_SERVICE_PORT.defaultValueString).toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get(config.SHUFFLE_SERVICE_PORT.key).toInt
    } else {
      tmpPort
    }
  }
}

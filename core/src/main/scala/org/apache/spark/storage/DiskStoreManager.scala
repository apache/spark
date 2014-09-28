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

import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.SizeEstimator
import java.io.{RandomAccessFile, File}
import java.nio.channels.FileChannel.MapMode

/**
 * Manage DiskStores.
 */
class DiskStoreManager(blockManager: BlockManager)
  extends BlockStore(blockManager) with Logging {

  private val defaultID = "defaultDiskStore1"
  private val defaultDiskStoreClass = "org.apache.spark.storage.DefaultDiskStore"

  def conf = blockManager.conf

  val diskStoreIDs = conf.getOption("spark.diskStore.ids").map(_.split(","))
  val minMemoryMapBytes = blockManager.conf.getLong("spark.storage.memoryMapThreshold", 2 * 4096L)

  val allStoreInfos = diskStoreIDs match {
    case Some(ids) =>
      ids.map { id =>
        val className = conf.get("spark.diskStore." + id + ".class")
        val size = conf.getLong("spark.diskStore." + id + ".size", 0)
        val clazz = Class.forName(className)
        val cons = clazz.getConstructor(classOf[String], classOf[BlockManager])
        val store = cons.newInstance(id, blockManager).asInstanceOf[DiskStore]
        new StoreInfo(id, store, size)
      }

    case None =>
      val clazz = Class.forName(defaultDiskStoreClass)
      val cons = clazz.getConstructor(classOf[String], classOf[BlockManager])
      val store = cons.newInstance(defaultID, blockManager).asInstanceOf[DiskStore]
      Array(new StoreInfo(defaultID, store, 0))
  }

  // if there is only one store in the store chain, use singleStore for shortcut path
  val singleStore = {
    if (allStoreInfos.length > 1) null else allStoreInfos.last.store
  }

  // the last store in the store chains.
  val lastStore = allStoreInfos.last.store

  // Shuffle data related code read/write file directly a lot and is hard to predict
  // data size. Without heavy modification (including shuffle map out, spill, fetcher etc.),
  // it won't be possible to use a hierarchy store. So put all shuffle data on last store for now.
  val shuffleStore = lastStore

  // store info in the chain except the last one
  val priorityStoreInfos = allStoreInfos.take(allStoreInfos.length - 1)

  if (allStoreInfos.last.totalSize != 0) {
    logWarning(("The last disk store : %s should not have a limited size quota.").
      format(allStoreInfos.last.id))
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel) : PutResult = {
    if (singleStore != null) {
      return singleStore.putBytes(blockId, _bytes, level)
    }

    for (info <- priorityStoreInfos) {
      if (info.tryUse(_bytes.limit())) {
        val result = info.store.putBytes(blockId, _bytes, level)
        if (result.size <= 0) {
          info.free(_bytes.limit())
        } else {
          return result
        }
      }
    }

    lastStore.putBytes(blockId, _bytes, level)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : PutResult = {

    if (singleStore != null) {
      return singleStore.putIterator(blockId, values, level, returnValues)
    }

    // The size estimate approaching here is ugly and not accurate.
    // But I really could not figure out an efficient way to make it accurate.

    // sizeEstimate might for most case be several times larger than the serialized data's size.
    // This might lead to some disk quota waste problems when a single block is really large,
    // and we decide that it couldn't be fit, while actually it can.
    // However, better than over run the quota.

    val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])

    for (info <- priorityStoreInfos) {
      if (info.tryUse(sizeEstimate)) {
        val result = info.store.putIterator(blockId, values, level, returnValues)
        if (result.size <= 0) {
          info.free(sizeEstimate)
        } else {
          // now correct the size with actual value
          val sizeDiff = sizeEstimate - result.size
          if (sizeDiff > 0) {
            info.free(sizeDiff)
          } else {
            // Should be rare to get here.
            // Call use instead of tryUse here, since we already written to disk.
            // Even this might actually go beyond the size quota.
            info.use(math.abs(sizeDiff))
          }
          return result
        }
      }
    }

    lastStore.putIterator(blockId, values, level, returnValues)
  }

  override def getSize(blockId: BlockId): Long = {
    if (singleStore != null) {
      return singleStore.getSize(blockId)
    }

    for (info <- priorityStoreInfos) {
      if (info.store.contains(blockId)) {
        return info.store.getSize(blockId)
      }
    }

    lastStore.getSize(blockId)
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    if (singleStore != null) {
      return singleStore.getBytes(blockId)
    }

    for (info <- priorityStoreInfos) {
      if (info.store.contains(blockId)) {
        return info.store.getBytes(blockId)
      }
    }

    lastStore.getBytes(blockId)
  }

  private def getBytes(file: File, offset: Long, length: Long): Option[ByteBuffer] = {
    val channel = new RandomAccessFile(file, "r").getChannel

    try {
      // For small files, directly read rather than memory map
      if (length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(length.toInt)
        channel.read(buf, offset)
        buf.flip()
        Some(buf)
      } else {
        Some(channel.map(MapMode.READ_ONLY, offset, length))
      }
    } finally {
      channel.close()
    }
  }

  def getBytes(segment: FileSegment): Option[ByteBuffer] = {
    getBytes(segment.file, segment.offset, segment.length)
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * A version of getValues that allows a custom serializer. This is used as part of the
   * shuffle short-circuit code.
   */
  def getValues(blockId: BlockId, serializer: Serializer): Option[Iterator[Any]] = {
    // TODO: Should bypass getBytes and use a stream based implementation, so that
    // we won't use a lot of memory during e.g. external sort merge.
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes, serializer))
  }

  override def remove(blockId: BlockId): Boolean = {
    if (singleStore != null) {
      return singleStore.remove(blockId)
    }

    for (info <- priorityStoreInfos) {
      if (info.store.contains(blockId)) {
        return info.store.remove(blockId)
      }
    }

    lastStore.remove(blockId)
  }

  override def contains(blockId: BlockId): Boolean = {
    for (info <- allStoreInfos) {
      if (info.store.contains(blockId)) {
        return true
      }
    }
    false
  }

  override def clear() = {
    for (info <- allStoreInfos) {
      info.store.clear()
    }
  }

  def getAllBlocks(): Seq[BlockId] = {
    allStoreInfos.toSeq.flatMap(_.store.getDiskBlockManager.getAllBlocks)
  }

  private[spark] def stop() {
    allStoreInfos.foreach(_.store.getDiskBlockManager.stop)
  }

}

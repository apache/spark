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

import java.util.LinkedHashMap
import java.util.concurrent.ArrayBlockingQueue
import java.nio.ByteBuffer
import collection.mutable.ArrayBuffer
import org.apache.spark.util.{SizeEstimator, Utils}

/**
 * Stores blocks in memory, either as ArrayBuffers of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  case class Entry(value: Any, size: Long, deserialized: Boolean)

  private val entries = new LinkedHashMap[BlockId, Entry](32, 0.75f, true)
  @volatile private var currentMemory = 0L
  // Object used to ensure that only one thread is putting blocks and if necessary, dropping
  // blocks from the memory store.
  private val putLock = new Object()

  logInfo("MemoryStore started with capacity %s.".format(Utils.bytesToString(maxMemory)))

  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel) {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      tryToPut(blockId, elements, sizeEstimate, true)
    } else {
      tryToPut(blockId, bytes, bytes.limit, false)
    }
  }

  override def putValues(
      blockId: BlockId,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : PutResult = {

    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      tryToPut(blockId, values, sizeEstimate, true)
      PutResult(sizeEstimate, Left(values.iterator))
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      tryToPut(blockId, bytes, bytes.limit, false)
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[ArrayBuffer[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate())   // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[ArrayBuffer[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        currentMemory -= entry.size
        logInfo("Block %s of size %d dropped from memory (free %d)".format(
          blockId, entry.size, freeMemory))
        true
      } else {
        false
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
      currentMemory = 0
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an ArrayBuffer if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated)
   * size must also be passed by the caller.
   *
   * Locks on the object putLock to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   */
  private def tryToPut(blockId: BlockId, value: Any, size: Long, deserialized: Boolean): Boolean = {
    // TODO: Its possible to optimize the locking by locking entries only when selecting blocks
    // to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has been
    // released, it must be ensured that those to-be-dropped blocks are not double counted for
    // freeing up more space for another block that needs to be put. Only then the actually dropping
    // of blocks (and writing to disk if necessary) can proceed in parallel.
    putLock.synchronized {
      if (ensureFreeSpace(blockId, size)) {
        val entry = new Entry(value, size, deserialized)
        entries.synchronized {
          entries.put(blockId, entry)
          currentMemory += size
        }
        if (deserialized) {
          logInfo("Block %s stored as values to memory (estimated size %s, free %s)".format(
            blockId, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
        } else {
          logInfo("Block %s stored as bytes to memory (size %s, free %s)".format(
            blockId, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
        }
        true
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        val data = if (deserialized) {
          Left(value.asInstanceOf[ArrayBuffer[Any]])
        } else {
          Right(value.asInstanceOf[ByteBuffer].duplicate())
        }
        blockManager.dropFromMemory(blockId, data)
        false
      }
    }
  }

  /**
   * Tries to free up a given amount of space to store a particular block, but can fail and return
   * false if either the block is bigger than our memory or it would require replacing another
   * block from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   *
   * Assumes that a lock is held by the caller to ensure only one thread is dropping blocks.
   * Otherwise, the freed space may fill up before the caller puts in their new value.
   */
  private def ensureFreeSpace(blockIdToAdd: BlockId, space: Long): Boolean = {

    logInfo("ensureFreeSpace(%d) called with curMem=%d, maxMem=%d".format(
      space, currentMemory, maxMemory))

    if (space > maxMemory) {
      logInfo("Will not store " + blockIdToAdd + " as it is larger than our memory limit")
      return false
    }

    if (maxMemory - currentMemory < space) {
      val rddToAdd = getRddId(blockIdToAdd)
      val selectedBlocks = new ArrayBuffer[BlockId]()
      var selectedMemory = 0L

      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (maxMemory - (currentMemory - selectedMemory) < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          if (rddToAdd.isDefined && rddToAdd == getRddId(blockId)) {
            logInfo("Will not store " + blockIdToAdd + " as it would require dropping another " +
              "block from the same RDD")
            return false
          }
          selectedBlocks += blockId
          selectedMemory += pair.getValue.size
        }
      }

      if (maxMemory - (currentMemory - selectedMemory) >= space) {
        logInfo(selectedBlocks.size + " blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one thread should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[ArrayBuffer[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            blockManager.dropFromMemory(blockId, data)
          }
        }
        return true
      } else {
        return false
      }
    }
    true
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
}


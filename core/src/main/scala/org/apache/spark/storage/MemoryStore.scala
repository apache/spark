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
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean, var dropping: Boolean = false)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val conf = blockManager.conf
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  @volatile private var currentMemory = 0L

  private var pendingUnrollMemory = 0L

  // Ensure only one thread is putting, and if necessary, dropping blocks at any given time
  private val accountingLock = new Object

  // A mapping from thread ID to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()

  /**
   * The amount of space ensured for unrolling values in memory, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   */
  private val maxUnrollMemory: Long = {
    val unrollFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
    (maxMemory * unrollFraction).toLong
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Free memory not occupied by existing blocks. Note that this does not include unroll memory. */
  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      putIterator(blockId, values, level, returnValues = true)
    } else {
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      val putAttempt = tryToPut(blockId, values, sizeEstimate, deserialized = true)
      PutResult(sizeEstimate, Left(values.iterator), putAttempt.droppedBlocks)
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
   *   (1) the block's storage level specifies useDisk, and
   *   (2) `allowPersistToDisk` is true.
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val unrolledValues = unrollSafely(blockId, values, droppedBlocks)
    unrolledValues match {
      case Left(arrayValues) =>
        // Values are fully unrolled in memory, so store them as an array
        val res = putArray(blockId, arrayValues, level, returnValues)
        droppedBlocks ++= res.droppedBlocks
        PutResult(res.size, res.data, droppedBlocks)
      case Right(iteratorValues) =>
        // Not enough space to unroll this block; drop to disk if applicable
        logWarning(s"Not enough space to store block $blockId in memory! " +
          s"Free memory is $freeMemory bytes.")
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data, droppedBlocks)
        } else {
          PutResult(0, Left(iteratorValues), droppedBlocks)
        }
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
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
        logInfo(s"Block $blockId of size ${entry.size} dropped from memory (free $freeMemory)")
        true
      } else {
        false
      }
    }
  }

  def removeWithoutUpdateMemorySize(blockId: BlockId) = {
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        entry.size
      } else {
        0L
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
   * Unroll the given block in memory safely.
   *
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
   */
  def unrollSafely(
      blockId: BlockId,
      values: Iterator[Any],
      droppedBlocks: ArrayBuffer[(BlockId, BlockStatus)])
    : Either[Array[Any], Iterator[Any]] = {

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-thread memory to request for unrolling blocks (bytes). Exposed for testing.
    val initialMemoryThreshold = conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this thread for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Previous unroll memory held by this thread, for releasing later (only at the very end)
    val previousMemoryReserved = currentUnrollMemoryForThisThread
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisThread(initialMemoryThreshold)

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      while (values.hasNext && keepUnrolling) {
        vector += values.next()
        if (elementsUnrolled % memoryCheckPeriod == 0) {
          // If our vector's size has exceeded the threshold, request more memory
          val currentSize = vector.estimateSize()
          if (currentSize >= memoryThreshold) {
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
            val spaceToEnsure = accountingLock.synchronized {
              if (reserveUnrollMemoryForThisThread(amountToRequest)) {
                0L
              } else if (amountToRequest > maxMemory) {
                keepUnrolling = false
                0L
              } else {
                val memoryNeeded = currentUnrollMemory + amountToRequest + pendingUnrollMemory - freeMemory
                val memoryAvailable = maxUnrollMemory - currentUnrollMemory - pendingUnrollMemory
                pendingUnrollMemory += amountToRequest
                math.min(memoryNeeded, memoryAvailable)
              }
            }
            if (spaceToEnsure > 0) {
              val task = planFreeSpace(blockId, spaceToEnsure, true)
              if (task.isDefined) {
                try {
                  droppedBlocks ++= task.get.runTask()
                } finally {
                  task.get.updateCurrentMemorySizeInTransaction {
                    pendingUnrollMemory -= amountToRequest
                    keepUnrolling = reserveUnrollMemoryForThisThread(amountToRequest)
                  }
                }
              } else {
                keepUnrolling = reserveUnrollMemoryForThisThread(amountToRequest)
              }
            }
            // New threshold is currentSize * memoryGrowthFactor
            memoryThreshold += amountToRequest
          }
        }
        elementsUnrolled += 1
      }

      if (keepUnrolling) {
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        Right(vector.iterator ++ values)
      }

    } finally {
      // If we return an array, the values returned do not depend on the underlying vector and
      // we can immediately free up space for other threads. Otherwise, if we return an iterator,
      // we release the memory claimed by this thread later on when the task finishes.
      if (keepUnrolling) {
        val amountToRelease = currentUnrollMemoryForThisThread - previousMemoryReserved
        releaseUnrollMemoryForThisThread(amountToRelease)
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * Synchronize on `accountingLock` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   *
   * Return whether put was successful, along with the blocks dropped in the process.
   */
  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {

    var putSuccess = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    val task = planFreeSpace(blockId, size)
    if (task.isDefined) {
      droppedBlocks ++= task.get.runTask()
      task.get.updateCurrentMemorySizeInTransaction {
        val entry = new MemoryEntry(value, size, deserialized)
        entries.put(blockId, entry)
        currentMemory += size
      }
      val valuesOrBytes = if (deserialized) "values" else "bytes"
      logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
        blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
      putSuccess = true
    } else {
      // Tell the block manager that we couldn't put it in memory so that it can drop it to
      // disk if the block allows disk storage.
      val data = if (deserialized) {
        Left(value.asInstanceOf[Array[Any]])
      } else {
        Right(value.asInstanceOf[ByteBuffer].duplicate())
      }
      val droppedBlockResult = blockManager.dropFromMemory(blockId, data)
      droppedBlockResult.foreach { r =>
        assert(r._2 == 0)
        droppedBlocks += ((blockId, r._1))
      }
    }
    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to free up a given amount of space to store a particular block, but can fail if
   * either the block is bigger than our memory or it would require replacing another block
   * from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   *
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   *
   * Return whether there is enough free space, along with the blocks dropped in the process.
   */
  private def planFreeSpace(
      blockIdToAdd: BlockId,
      space: Long,
      mustDrop: Boolean = false): Option[DroppingTask] = {
    logInfo(s"ensureFreeSpace($space) called with curMem=$currentMemory, maxMem=$maxMemory")

    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return None
    }

    accountingLock.synchronized {
      // Take into account the amount of memory currently occupied by unrolling blocks
      val actualFreeMemory = if (mustDrop) 0L else freeMemory - currentUnrollMemory

      if (actualFreeMemory < space) {
        val rddToAdd = getRddId(blockIdToAdd)
        val selectedBlocks = new ArrayBuffer[BlockId]
        var selectedMemory = 0L

        // This is synchronized to ensure that the set of entries is not changed
        // (because of getValue or getBytes) while traversing the iterator, as that
        // can lead to exceptions.
        entries.synchronized {
          val iterator = entries.entrySet().iterator()
          while (actualFreeMemory + selectedMemory < space && iterator.hasNext) {
            val pair = iterator.next()
            val blockId = pair.getKey
            val entry = pair.getValue()
            if (!entry.dropping && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))) {
              selectedBlocks += blockId
              selectedMemory += entry.size
            }
          }
        }

        if (actualFreeMemory + selectedMemory >= space) {
          logInfo(s"${selectedBlocks.size} blocks selected for dropping")
          val toBeDroppedBlocks = new ArrayBuffer[ToBeDroppedBlock]
          for (blockId <- selectedBlocks) {
            val entry = entries.synchronized { entries.get(blockId) }
            // This should never be null as only one thread should be dropping
            // blocks and removing entries. However the check is still here for
            // future safety.
            if (entry != null) {
              val data = if (entry.deserialized) {
                Left(entry.value.asInstanceOf[Array[Any]])
              } else {
                Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
              }
              toBeDroppedBlocks += ToBeDroppedBlock(blockId, data)
              entry.dropping = true
            }
          }
          Some(new DroppingTask(toBeDroppedBlocks))
        } else {
          logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
            "from the same RDD")
          None
        }
      } else {
        Some(new DroppingTask(Nil))
      }
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  /**
   * Reserve additional memory for unrolling blocks used by this thread.
   * Return whether the request is granted.
   */
  private[spark] def reserveUnrollMemoryForThisThread(memory: Long): Boolean = {
    accountingLock.synchronized {
      val granted = freeMemory > currentUnrollMemory + memory + pendingUnrollMemory
      if (granted) {
        val threadId = Thread.currentThread().getId
        unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, 0L) + memory
      }
      granted
    }
  }

  /**
   * Release memory used by this thread for unrolling blocks.
   * If the amount is not specified, remove the current thread's allocation altogether.
   */
  private[spark] def releaseUnrollMemoryForThisThread(memory: Long = -1L): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      if (memory < 0) {
        unrollMemoryMap.remove(threadId)
      } else {
        unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, memory) - memory
        // If this thread claims no more unroll memory, release it completely
        if (unrollMemoryMap(threadId) <= 0) {
          unrollMemoryMap.remove(threadId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all threads.
   */
  private[spark] def currentUnrollMemory: Long = accountingLock.synchronized {
    unrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this thread.
   */
  private[spark] def currentUnrollMemoryForThisThread: Long = accountingLock.synchronized {
    unrollMemoryMap.getOrElse(Thread.currentThread().getId, 0L)
  }

  private class DroppingTask(blocks: Seq[ToBeDroppedBlock]) {
    private var droppedMemorySize = 0L

    def runTask() = {
      val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      var droppingDoneCount = 0
      try {
        blocks.foreach { block =>
          val dropResult = blockManager.dropFromMemory(block.id, block.data)
          dropResult.foreach { r =>
            droppedBlocks += block.id -> r._1
            droppedMemorySize += r._2
          }
          droppingDoneCount += 1
        }
        droppedBlocks
      } catch {
        case e: Exception =>
          blocks.drop(droppingDoneCount).foreach { block =>
            entries.synchronized{
              val entry = entries.get(block.id)
              if (entry != null) entry.dropping = false
            }
          }
          if (droppedMemorySize > 0) {
            entries.synchronized { currentMemory -= droppedMemorySize }
            droppedMemorySize = 0
          }
          throw e
      }
    }

    def updateCurrentMemorySizeInTransaction(f: => Unit): Unit = {
      entries.synchronized {
        currentMemory -= droppedMemorySize
        f
      }
    }
  }
}

private[spark] case class ResultWithDroppedBlocks(
    success: Boolean,
    droppedBlocks: Seq[(BlockId, BlockStatus)])

private[spark] case class ToBeDroppedBlock(id: BlockId, data: Either[Array[Any], ByteBuffer])

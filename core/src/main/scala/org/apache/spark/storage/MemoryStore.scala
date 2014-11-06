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
import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val conf = blockManager.conf
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)
  // a Set maintains old blocks that will be dropped
  private val tobeDroppedBlocksSet = new HashSet[BlockId]

  // currentMemory is actually memory that is already used for caching blocks
  @volatile private var currentMemory = 0L

  // Ensure only one thread updating the information, information including memory to drop,
  // memory unrolled, etc.
  private val accountingLock = new Object

  // A mapping from thread ID to amount of memory used for unrolling a block (in bytes).
  // The memory is only for unrolling, not actually occupied by blocks.
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  
  // A mapping from thread ID to amount of memory that is free but is reserved for Unroll
  // or tryToPut. The memory is free but has been reserved by current thread so that other
  // thread would not know this free memory. After Unroll or tryToPut, this free memory will
  // be used or released for other threads.
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val reservedFreeMemoryMap = mutable.HashMap[Long, Long]()
  
  // A mapping from thread ID to amount of memory reserved for unrolling part of a block (in 
  // bytes), when the block is not able to fully put into memory, will return an iterator when
  // unrolling, but the memory still need to reserved before the block is dropping from memory.
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val iteratorUnrollMemoryMap = mutable.HashMap[Long, Long]()
  
  // A mapping from thread ID to a blockId Set that to be dropped for new blocks.
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`  
  private val toDropBlocksMap = mutable.HashMap[Long, HashSet[BlockId]]()
  
  // A mapping from thread ID to amount of memory reserved by new blocks to put (in bytes).
  // The memory is reserved before blocks actually put into.
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  private val tryToPutMemoryMap = mutable.HashMap[Long, Long]()
  
  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** 
   *  Free memory not occupied by existing blocks. Note that this includes all memory that is
   *  in free state logically.
   */
  def freeMemory: Long = maxMemory - currentMemory
  
  /**
   *  Free memory that can be used when new blocks are trying to put into memory. The value
   *  includes unroll memory.
   */
  def freeMemoryForTryToPut: Long = maxMemory - (
    currentMemory + currentTryToPutMemory + currentIteratorUnrollMemory +
    currentReservedFreeMemory)

  /**
   * Free memory that can used when new blocks are unrolling to the memory. The memory only 
   * includes the memory that is in free state physically.
   */
  def freeMemoryForUnroll: Long = maxMemory - (
    currentMemory + currentTryToPutMemory + currentIteratorUnrollMemory +
    currentReservedFreeMemory + currentUnrollMemory)

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

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   */
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    lazy val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]
    val putAttempt = tryToPut(blockId, () => bytes, size, deserialized = false)
    val data =
      if (putAttempt.success) {
        assert(bytes.limit == size)
        Right(bytes.duplicate())
      } else {
        null
      }
    PutResult(size, data, putAttempt.droppedBlocks)
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
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, 
              iteratorValues, level, returnValues)
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
      // Every time when removing blocks from memory, the information about blocks that to be
      // dropped need to be refreshed.
      tobeDroppedBlocksSet.remove(blockId)
      delToDropBlocksMapForThisThread(blockId)
      val entry = entries.remove(blockId)
      if (entry != null) {
        // all memory that obtained by dropping old blocks should be reserved if necessary.
        if (reservedFreeMemoryMap.contains(Thread.currentThread().getId)) {
          increaseReservedFreeMemoryForThisThread(entry.size)
        }
        currentMemory -= entry.size
        logInfo(s"Block $blockId of size ${entry.size} dropped from memory")
        true
      } else {
        false
      }
    }
  }

  override def clear() {
    entries.synchronized {
      entries.clear()
      tobeDroppedBlocksSet.clear()
      currentMemory = 0
      unrollMemoryMap.clear()
      reservedFreeMemoryMap.clear()
      iteratorUnrollMemoryMap.clear()
      toDropBlocksMap.clear()
      tryToPutMemoryMap.clear()
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
   * When there is not enough memory for unrolling blocks, old blocks will be dropped from
   * memory. The dropping operation is in parallel to fully utilized the disk throughput
   * when there are multiple disks. Each thread will drop blocks selected by itself, and the
   * freed memory by dropping old blocks can only be used by this thread before it finish 
   * unrolling.
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
    var elementsUnrolled = 0L
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this thread for this particular unrolling operation
    // Initial value is 0 means don't reserve memory originally, only reserve dynamically
    var memoryThreshold = 0L
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]
    
    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      while (values.hasNext && keepUnrolling) {
        vector += values.next()
        // Every checking period reaches or the iterator is exhausted, we check whether extra
        // memory is needed.
        if (elementsUnrolled % memoryCheckPeriod == 0 || !values.hasNext) {
          // If our vector's size has exceeded the threshold, request more memory
          val currentSize = vector.estimateSize()
          if (currentSize > memoryThreshold) {
            val amountToRequest = values.hasNext match {
              case true =>
                (currentSize * memoryGrowthFactor - memoryThreshold).toLong
              case false => 
                // no need to request more memory than needed if iterator is exhausted
                (currentSize - memoryThreshold).toLong
            }
            if (freeMemoryForUnroll < amountToRequest) {
              keepUnrolling = false
              val selectedBlocks = new ArrayBuffer[BlockId]()
              val ensureSpaceResult = ensureFreeSpace(
                  blockId, amountToRequest, freeMemoryForUnroll)
              val enoughFreeSpace = ensureSpaceResult.success
              
              if (enoughFreeSpace) {
                selectedBlocks ++= ensureSpaceResult.toDropBlocksId
                if (!selectedBlocks.isEmpty) {
                  droppedBlocks ++= doDrop(selectedBlocks)
                }
                // blocks that selected "to-be-dropped" has been dropped, and memory is free for
                // unroll. At the same time, memory that reserved for unroll ("to-be-dropped" 
                // block memory size) should be released.
                increaseUnrollMemoryForThisThread(amountToRequest)
                removeReservedFreeMemoryForThisThread()
                keepUnrolling = true
              }
            } else {
              increaseUnrollMemoryForThisThread(amountToRequest)
            }
            if (keepUnrolling) {
              memoryThreshold += amountToRequest
            }
          }
        }
        elementsUnrolled += 1
      }

      if (keepUnrolling) {
        // to free up memory that requested more than needed, the value might be negative
        decreaseUnrollMemoryForThisThread(memoryThreshold - SizeEstimator.estimate(
            vector.toArray.asInstanceOf[AnyRef]))
        logInfo(s"Successfully unrolloing the block ${blockId} to memory, block size is " + 
            s"${SizeEstimator.estimate(vector.toArray.asInstanceOf[AnyRef])}")
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, vector.estimateSize())
        Right(vector.iterator ++ values)
      }
    } finally {
      accountingLock.synchronized {
        // If we return an iterator, that means the blocks is not able to put into memory, and 
        // will be dropped to disk if it can or just dropped from memory. The memory reserved
        // for unrolling will not be released because it depending on the underlying vector.
        // The memory size will be maintained from unrollMemoryMap to iteratorUnrollMemoryMap.
        if (!keepUnrolling) {
          reserveIteratorUnrollMemoryForThisThread()
          removeUnrollMemoryForThisThread()
        }
        // whatever we return, blocks that marked"to-be-dropped" should always have been dropped
        cleanToDropBlocksMapForThisThread()
        // We will finally reset the ReservedUnrollMemory for current thread. The memory should 
        // always be 0 after unrolling.
        removeReservedFreeMemoryForThisThread()
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {
    tryToPut(blockId, () => value, size, deserialized)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * The tryToPut operation is processed in parallel like Unroll process. In most case, the free
   * memory are ready for tryToPut after Unroll.
   * 
   * Return whether put was successful, along with the blocks dropped in the process.
   */

  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {

    var putSuccess = false
    var enoughFreeSpace = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val selectedBlocks = new ArrayBuffer[BlockId]()

    val freeSpaceResult = ensureFreeSpace(blockId, size, freeMemoryForTryToPut)
    enoughFreeSpace = freeSpaceResult.success
    if (enoughFreeSpace) {
      selectedBlocks ++= freeSpaceResult.toDropBlocksId
      try {
        if (!selectedBlocks.isEmpty) {
          droppedBlocks ++= doDrop(selectedBlocks)
        }
        increaseTryToPutMemoryForThisThread(size)
        decreaseUnrollMemoryForThisThread(size)
      } catch {
        // if there is exception, the current block will never put into Memory
        case e: Exception => {
          decreaseTryToPutMemoryForThisThread(size)
          throw e
        }
      } finally {
        // whatever there is exception or not, blocks selected by this thread to drop should 
        // already been dropped, and Unrolled Memory for this thread should also be 0.
        removeUnrollMemoryForThisThread()
        removeReservedFreeMemoryForThisThread()
        cleanToDropBlocksMapForThisThread()
      }
      
      val entry = new MemoryEntry(value, size, deserialized)
      entries.synchronized {
        entries.put(blockId, entry)
        decreaseTryToPutMemoryForThisThread(size)
        currentMemory += size
      }
      val valuesOrBytes = if (deserialized) "values" else "bytes"
      logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
        blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
      putSuccess = true
    } else {
      logInfo(s"Failed to put block ${blockId} to memory, block size is ${size}.")
      // For some reason, blocks might still not be able to put into memory even unroll
      // successfully. If so, we need to clear reserved unroll memory and to-be-dropped blocks for 
      // this thread. 
      removeUnrollMemoryForThisThread()
      removeReservedFreeMemoryForThisThread()
      cleanToDropBlocksMapForThisThread()
      // Tell the block manager that we couldn't put it in memory so that it can drop it to
      // disk if the block allows disk storage.
      val data = if (deserialized) {
        Left(value.asInstanceOf[Array[Any]])
      } else {
        Right(value.asInstanceOf[ByteBuffer].duplicate())
      }
      val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
      droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
    }
    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to free up a given amount of space to store a particular block, but can fail if
   * either the block is bigger than our memory or it would require replacing another block
   * from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   *
   * In this method each thread will select blocks that can be dropped until the selected blocks
   * memory is enough, and then in the caller, each thread will drop the blocks itself. The 
   * freed memory by dropping old blocks can only be used by current thread, other threads are
   * not aware of the freed memory.
   * 
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   *
   * Return whether there is enough free space, along with the blocks marked as "to-be-dropped"  
   * and the memory that can be freed if the "to-be-dropped" blocks are actually dropped.
   */
  private def ensureFreeSpace(
    blockIdToAdd: BlockId,
    size: Long,
    memoryFree: Long): ResultBlocksIdMemory = {
    logInfo(s"ensureFreeSpace($size) called with curMem=$currentMemory, maxMem=$maxMemory")

    var putSuccess = false
    var enoughFreeSpace = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    var selectedMemory = 0L
    val selectedBlocks = new ArrayBuffer[BlockId]()

    if (size > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      ResultBlocksIdMemory(enoughFreeSpace, selectedBlocks.toSeq)
    } else {
      // This is synchronized with two purposes, one is to ensure that the set of entries 
      // is not changed (because of getValue or getBytes) while traversing the iterator, 
      // as that can lead to exceptions. The other is to ensure that only one thread is 
      // traversing the entry to select "to-be-dropped" blocks and update the map information, 
      // to avoid same block is selected by multiple threads.
      entries.synchronized {
        if (memoryFree < size) {
          val rddToAdd = getRddId(blockIdToAdd)
          val iterator = entries.entrySet().iterator()
          while (memoryFree + selectedMemory < size && iterator.hasNext) {
            val pair = iterator.next()
            val blockId = pair.getKey
            // only blocks that has not been selected can be selected
            if (!tobeDroppedBlocksSet(blockId)) {
              if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
                selectedBlocks += blockId
                selectedMemory += pair.getValue.size
              }
            }
          }
        }
        if (memoryFree + selectedMemory >= size) {
          tobeDroppedBlocksSet ++= selectedBlocks
          addToDropBlocksMapForThisThread(selectedBlocks.toArray)
          // reserve the free memory that can only be used by current thread
          if (size - selectedMemory > 0) {
            increaseReservedFreeMemoryForThisThread(size - selectedMemory)
          }
          enoughFreeSpace = true
          logInfo(selectedBlocks.size + " blocks selected for dropping")
          ResultBlocksIdMemory(enoughFreeSpace, selectedBlocks.toSeq)
        } else {
          logInfo(s"Will not store $blockIdToAdd as it would require" + 
              s" dropping another block from the same RDD")
          ResultBlocksIdMemory(enoughFreeSpace, selectedBlocks.toSeq)
        }
      }
    }
  }

  /**
   * dropping the blocks from memory, blocks will be dropped to disk if storage is useDisk
   */
  private def doDrop(toDropBlocks: Seq[BlockId]): ArrayBuffer[(BlockId, BlockStatus)] = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    for (blocks <- toDropBlocks) {
      val entry = entries.synchronized { entries.get(blocks) }
      // drop old block to free memory for new blocks to put.
      if (entry != null) {
        val data = if (entry.deserialized) {
          Left(entry.value.asInstanceOf[Array[Any]])
        } else {
          Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
        }
        val droppedBlockStatus = blockManager.dropFromMemory(blocks, data)
        droppedBlockStatus.foreach { status => droppedBlocks += ((blocks, status)) }
      }
    }
    droppedBlocks
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  def cleanupForThisThread(): Unit = {
    removeUnrollMemoryForThisThread()
    removeIteratorUnrollMemoryForThisThread()
    cleanToDropBlocksMapForThisThread()
    removeTryToPutMemoryForThisThread()
  }

  /**
   * Add more old blocks as "to-be-dropped" for this thread.
   */
  def addToDropBlocksMapForThisThread(blocksId: Array[BlockId]): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      toDropBlocksMap.getOrElse(threadId, new HashSet[BlockId]()) ++= blocksId
    }
  }
  
  /**
   * Remove a specified block from the map that marked as "to-be-dropped" from this thread, 
   * which means the blocks has been dropped from the memory.
   */
  def delToDropBlocksMapForThisThread(blockId: BlockId): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      toDropBlocksMap.getOrElse(threadId, new HashSet[BlockId]()).remove(blockId)
    }
  }
  
  /**
   * Remove all blocks that selected as "to-be-dropped" for this thread, which means
   * either the blocks has been dropped from memory or there are some exceptions.
   */
  def cleanToDropBlocksMapForThisThread(): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      val blockIdSet = toDropBlocksMap.getOrElse(threadId, new HashSet[BlockId]())
      if (!blockIdSet.isEmpty) {
        val itr = blockIdSet.iterator
        while (itr.hasNext) {
          val blockId = itr.next()
          tobeDroppedBlocksSet.remove(blockId)
        }
        toDropBlocksMap.remove(threadId)
      }      
    }
  }

  /**
   * Remove the current thread's allocation for unrolling.
   */
  def releaseUnrollMemoryForThisThread(): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      unrollMemoryMap.remove(threadId)
    }
  }
  
  /**
   * Get additional memory for unrolling for this thread.
   */
  def increaseUnrollMemoryForThisThread(memory: Long): Unit = {
    accountingLock.synchronized {
      val threadId = Thread.currentThread().getId
      unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Decrease memory reserved by this thread for unrolling blocks.
   */
  def decreaseUnrollMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      unrollMemoryMap(threadId) = unrollMemoryMap.getOrElse(threadId, 0L) - memory
      // If this thread claims no more unroll memory, release it completely
      if (unrollMemoryMap(threadId) < 0) {
        unrollMemoryMap.remove(threadId)
      }
    }
  }
  
  /**
   * Return the amount of memory currently totally reserved for unrolling blocks across 
   * all threads. The unrolling blocks are blocks that have been confirmed can putting into
   * the memory.
   */
  def currentUnrollMemory: Long = accountingLock.synchronized {
    unrollMemoryMap.values.sum + pendingUnrollMemoryMap.values.sum
  }

  /**
   * Remove the memory from unrolling from the map, which means either the blocks has been 
   * selected as "try-to-put" or blocks has been put into memory or the unrolled memory 
   * is invalid.
   */
  def removeUnrollMemoryForThisThread(): Unit = accountingLock.synchronized {
    unrollMemoryMap.remove(Thread.currentThread().getId)
  }

  /**
   * Increase reserved free memory for this thread. This only happen when there is not enough 
   * space for unrolling new block and need to drop old block for more space. Once the selected
   * blocks of this thread is dropped from memory, the freed memory should be reserved by this
   * thread to avoid being used by other threads. reservedFreeMemory should also be refreshed 
   * after finished unrolling or tryToPut
   */
  def increaseReservedFreeMemoryForThisThread(memory: Long): Unit = {
    accountingLock.synchronized {
      val threadId = Thread.currentThread().getId
      reservedFreeMemoryMap(threadId) = reservedFreeMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Return the amount of free memory reserved by all threads
   */
  def currentReservedFreeMemory: Long = accountingLock.synchronized {
    reservedFreeMemoryMap.values.sum
  }

  /**
   * clean the reservedFreeMemoryMap for this thread, each time after the unrolling process and
   * tryToPutprocess, this method need to be called.
   */
  def removeReservedFreeMemoryForThisThread()
    : Unit = accountingLock.synchronized {
    reservedFreeMemoryMap.remove(Thread.currentThread().getId)
  }
  
  /**
   * When a block can not unroll into memory, the memory size that used to store the computed
   * part should maintained in iteratorUnrollMemoryMap.
   */
  def reserveIteratorUnrollMemoryForThisThread(): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      val unrolledMem =  unrollMemoryMap.getOrElse(threadId, 0L)
      iteratorUnrollMemoryMap(threadId) = iteratorUnrollMemoryMap.getOrElse(
          threadId, 0L) + unrolledMem
    }
  }
  
  /**
   * Return the amount of memory currently totally used for part of blocks that can not put
   * into the memory (will drop to disk or just drop from meory in future) across all threads.
   */
  def currentIteratorUnrollMemory: Long = accountingLock.synchronized {
    iteratorUnrollMemoryMap.values.sum
  }
  
  /**
   * After the block put into disk or dropped from memory, reservedIteratorUnrollMemoryMap should
   * refresh the value for this thread.
   */
  def removeIteratorUnrollMemoryForThisThread()
    : Unit = accountingLock.synchronized {
    iteratorUnrollMemoryMap.remove(Thread.currentThread().getId)
  }
    
  /**
   * Reserve additional memory for putting blocks for this thread. That means more blocks are 
   * waiting to put into memory, and before putting into memory, it will reserve some memory first.
   */
  def increaseTryToPutMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      tryToPutMemoryMap(threadId) = tryToPutMemoryMap.getOrElse(threadId, 0L) + memory
    }
  }

  /**
   * Release memory used by this thread for putting new blocks, which means new block has been 
   * finished putting into memory already.
   */
  def decreaseTryToPutMemoryForThisThread(memory: Long): Unit = {
    val threadId = Thread.currentThread().getId
    accountingLock.synchronized {
      tryToPutMemoryMap(threadId) = tryToPutMemoryMap.getOrElse(threadId, memory) - memory
      // If this thread claims no more unroll memory, release it completely
      if (tryToPutMemoryMap(threadId) < 0) {
        tryToPutMemoryMap.remove(threadId)
      }
    }
  }

  /**
   * Return the amount of memory currently reserved for putting new blocks across all threads.
   */
  def currentTryToPutMemory: Long = accountingLock.synchronized {
    tryToPutMemoryMap.values.sum
  }
  
  /**
   * Clean all memory reserved for putting new blocks.
   */
  def removeTryToPutMemoryForThisThread(): Unit = {
    tryToPutMemoryMap.remove(Thread.currentThread().getId)
  }

  /**
   * Return the number of threads currently unrolling blocks.
   */
  def numThreadsUnrolling: Int = accountingLock.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   */
  def logMemoryUsage(): Unit = {
    val blocksMemory = currentMemory
    val unrollMemory = currentUnrollMemory
    val totalMemory = blocksMemory + unrollMemory
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemory)} (blocks) + " +
      s"${Utils.bytesToString(unrollMemory)} (scratch space shared across " +
      s"$numThreadsUnrolling thread(s)) = ${Utils.bytesToString(totalMemory)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

private[spark] case class ResultWithDroppedBlocks(
    success: Boolean,
    droppedBlocks: Seq[(BlockId, BlockStatus)])
    
private[spark] case class ResultBlocksIdMemory(
    success: Boolean,    
    toDropBlocksId: Seq[BlockId])

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

package org.apache.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.util.collection.SizeTrackingAppendOnlyBuffer

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded. */
  private val loading = new mutable.HashSet[RDDBlockId]

  /**
   * The amount of space ensured for unrolling partitions, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   * It must be a lazy val in order to access a mocked BlockManager's conf in tests properly.
   */
  private lazy val globalBufferMemory = BlockManager.getBufferMemory(blockManager.conf)

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached. */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {
      case Some(values) =>
        // Partition is already materialized, so just return its values
        new InterruptibleIterator(context, values.asInstanceOf[Iterator[T]])

      case None =>
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        try {
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // If the task is running locally, do not persist the result
          if (context.runningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          context.taskMetrics.updatedBlocks = Some(updatedBlocks)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {
        // If the partition is free, acquire its lock to compute its value
        loading.add(id)
        None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {
          try {
            loading.wait()
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.asInstanceOf[Iterator[T]])
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses
   * of other blocks along the way.
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      storageLevel: StorageLevel,
      updatedBlocks: ArrayBuffer[(BlockId, BlockStatus)]): Iterator[T] = {

    if (!storageLevel.useMemory) {
      /* This RDD is not to be cached in memory, so we can just pass the computed values
       * as an iterator directly to the BlockManager, rather than first fully unrolling
       * it in memory. The latter option potentially uses much more memory and risks OOM
       * exceptions that can be avoided. */
      updatedBlocks ++= blockManager.put(key, values, storageLevel, tellMaster = true)
      blockManager.get(key) match {
        case Some(v) => v.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {
      /* This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back, e.g.
       * when the entirety of the RDD does not fit in memory. */

      var count = 0                   // The number of elements unrolled so far
      var dropPartition = false       // Whether to drop the new partition from memory
      var previousSize = 0L           // Previous estimate of the size of our buffer
      val memoryRequestPeriod = 1000  // How frequently we request for more memory for our buffer

      val threadId = Thread.currentThread().getId
      val cacheMemoryMap = SparkEnv.get.cacheMemoryMap
      var buffer = new SizeTrackingAppendOnlyBuffer[Any]

      /* While adding values to the in-memory buffer, periodically check whether the memory
       * restrictions for unrolling partitions are still satisfied. If not, stop immediately,
       * and persist the partition to disk if specified by the storage level. This check is
       * a safeguard against the scenario when a single partition does not fit in memory. */
      while (values.hasNext && !dropPartition) {
        buffer += values.next()
        count += 1
        if (count % memoryRequestPeriod == 1) {
          // Calculate the amount of memory to request from the global memory pool
          val currentSize = buffer.estimateSize()
          val delta = math.max(currentSize - previousSize, 0)
          val memoryToRequest = currentSize + delta
          previousSize = currentSize

          // Atomically check whether there is sufficient memory in the global pool to continue
          cacheMemoryMap.synchronized {
            val previouslyOccupiedMemory = cacheMemoryMap.get(threadId).getOrElse(0L)
            val otherThreadsMemory = cacheMemoryMap.values.sum - previouslyOccupiedMemory

            // Request for memory for the local buffer, and return whether request is granted
            def requestForMemory(): Boolean = {
              val availableMemory = blockManager.memoryStore.freeMemory - otherThreadsMemory
              val granted = availableMemory > memoryToRequest
              if (granted) { cacheMemoryMap(threadId) = memoryToRequest }
              granted
            }

            // If the first request is not granted, try again after ensuring free space
            // If there is still not enough space, give up and drop the partition
            if (!requestForMemory()) {
              val result = blockManager.memoryStore.ensureFreeSpace(key, globalBufferMemory)
              updatedBlocks ++= result.droppedBlocks
              dropPartition = !requestForMemory()
            }
          }
        }
      }

      if (!dropPartition) {
        // We have successfully unrolled the entire partition, so cache it in memory
        updatedBlocks ++= blockManager.put(key, buffer.array, storageLevel, tellMaster = true)
        buffer.iterator.asInstanceOf[Iterator[T]]
      } else {
        // We have exceeded our collective quota. This partition will not be cached in memory.
        val persistToDisk = storageLevel.useDisk
        logWarning(s"Failed to cache $key in memory! There is not enough space to unroll the " +
          s"entire partition. " + (if (persistToDisk) "Persisting to disk instead." else ""))
        var newValues = (buffer.iterator ++ values).asInstanceOf[Iterator[T]]
        if (persistToDisk) {
          val newLevel = StorageLevel(storageLevel.useDisk, useMemory = false,
            storageLevel.useOffHeap, storageLevel.deserialized, storageLevel.replication)
          newValues = putInBlockManager[T](key, newValues, newLevel, updatedBlocks)
          // Free up buffer for other threads
          buffer = null
          cacheMemoryMap.synchronized {
            cacheMemoryMap(threadId) = 0
          }
        }
        newValues
      }
    }
  }

}

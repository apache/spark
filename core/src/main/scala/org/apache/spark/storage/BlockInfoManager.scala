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

import java.lang
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.ConcurrentHashMultiset

import org.apache.spark.{Logging, TaskContext}


/**
 * Tracks metadata for an individual block.
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {

  /**
   * The size of the block (in bytes)
   */
  var size: Long = 0

  /**
   * The number of times that this block has been locked for reading.
   */
  var readerCount: Int = 0

  /**
   * The task attempt id of the task which currently holds the write lock for this block, or -1
   * if this block is not locked for writing.
   */
  var writerTask: Long = -1

  // Invariants:
  //     (writerTask != -1) implies (readerCount == 0)
  //     (readerCount != 0) implies (writerTask == -1)

  /**
   * True if this block has been removed from the BlockManager and false otherwise.
   * This field is used to communicate block deletion to blocked readers / writers (see its usage
   * in [[BlockInfoManager]]).
   */
  var removed: Boolean = false
}

/**
 * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 *
 * The locking interface exposed by this class is readers-writers lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 *
 * This class is thread-safe.
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[putAndLockForWritingIfAbsent()]]) and are removed
   * by [[remove()]].
   */
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
   * Tracks the set of blocks that each task has locked for writing.
   */
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId]

  /**
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant). This is thread-safe.
   */
  private[this] val readLocksByTask: LoadingCache[lang.Long, ConcurrentHashMultiset[BlockId]] = {
    // We need to explicitly box as java.lang.Long to avoid a type mismatch error:
    val loader = new CacheLoader[java.lang.Long, ConcurrentHashMultiset[BlockId]] {
      override def load(t: java.lang.Long) = ConcurrentHashMultiset.create[BlockId]()
    }
    CacheBuilder.newBuilder().build(loader)
  }

  // ----------------------------------------------------------------------------------------------

  /**
   * Returns the current tasks's task attempt id (which uniquely identifies the task), or -1024
   * if called outside of a task (-1024 was chosen because it's different than the -1 which is used
   * in [[BlockInfo.writerTask]] to denote the absence of a write lock).
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1024L)
  }

  /**
   * Todo: document blocking / locking semantics.
   *
   * @param blockId
   * @return
   */
  def getAndLockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    infos.get(blockId).map { info =>
      while (info.writerTask != -1) {
        if (info.removed) return None
        if (blocking) wait() else return None
      }
      // TODO: try to remember why you need actualInfo / the extra get() here.
      val actualInfo = infos.get(blockId)
      actualInfo.foreach { i =>
        i.readerCount += 1
        readLocksByTask(currentTaskAttemptId).add(blockId)
      }
      logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
      info
    }
  }

  def getAndLockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    infos.get(blockId).map { info =>
      if (info.writerTask != currentTaskAttemptId) {
        while (info.writerTask != -1 || info.readerCount != 0) {
          if (info.removed) return None
          if (blocking) wait() else return None
        }
      }
      val actualInfo = infos.get(blockId)
      actualInfo.foreach { i =>
        i.writerTask = currentTaskAttemptId
        writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
      }
      logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
      info
    }
  }

  def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    // TODO: refactor this code so that log messages aren't confusing.
    val info = get(blockId).get
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold")
    releaseLock(blockId)
    getAndLockForReading(blockId, blocking = false)
    notifyAll()
  }

  def releaseLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId releasing lock for $blockId")
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    if (info.writerTask != -1) {
      info.writerTask = -1
      writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
    } else {
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      info.readerCount -= 1
      val countsForTask = readLocksByTask.get(currentTaskAttemptId)
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      assert(newPinCountForTask >= 0,
        s"Task $currentTaskAttemptId release lock on block $blockId more times than it acquired it")
    }
    notifyAll()
  }

  def putAndLockForWritingIfAbsent(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    val actualInfo = infos.getOrElseUpdate(blockId, newBlockInfo)
    if (actualInfo eq newBlockInfo) {
      actualInfo.writerTask = currentTaskAttemptId
      writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
      true
    } else {
      false
    }
  }

  /**
   * Release all pins held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the number of pins released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Int = {
    synchronized {
      writeLocksByTask.remove(taskAttemptId).foreach { locks =>
        for (blockId <- locks) {
          infos.get(blockId).foreach { info =>
            assert(info.writerTask == taskAttemptId)
            info.writerTask = -1
          }
        }
      }
      notifyAll()
    }
    val readLocks = readLocksByTask.get(taskAttemptId)
    readLocksByTask.invalidate(taskAttemptId)
    val totalPinCountForTask = readLocks.size()
    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      synchronized {
        get(blockId).foreach { info =>
          info.readerCount -= lockCount
          assert(info.readerCount >= 0)
        }
      }
    }
    synchronized {
      notifyAll()
    }
    totalPinCountForTask
  }


  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def  getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size() +
      readLocksByTask.asMap().asScala.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  // This implicitly drops all locks.
  def remove(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    // TODO: Should probably have safety checks here
    infos.remove(blockId).foreach { info =>
      info.removed = true
    }
    notifyAll()
  }

  def clear(): Unit = synchronized {
    infos.clear()
    readLocksByTask.invalidateAll()
    writeLocksByTask.clear()
  }

  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.iterator.toArray.toIterator
  }

}

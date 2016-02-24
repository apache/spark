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

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.ConcurrentHashMultiset

import org.apache.spark.{Logging, SparkException, TaskContext}


/**
 * Tracks metadata for an individual block.
 *
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
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
  def size: Long = _size
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * The number of times that this block has been locked for reading.
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
   * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = 0

  /**
   * True if this block has been removed from the BlockManager and false otherwise.
   * This field is used to communicate block deletion to blocked readers / writers (see its usage
   * in [[BlockInfoManager]]).
   */
  def removed: Boolean = _removed
  def removed_=(r: Boolean): Unit = {
    _removed = r
    checkInvariants()
  }
  private[this] var _removed: Boolean = false

  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
    // If a block is removed then it is not locked:
    assert(!_removed || (_readerCount == 0 && _writerTask == BlockInfo.NO_WRITER))
  }

  checkInvariants()
}

private[storage] object BlockInfo {

  /**
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
   */
  val NO_WRITER: Long = -1

  /**
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 *
 * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 *
 * This class is thread-safe.
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
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
   * that a block has been locked (since our read locks are re-entrant).
   */
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
   * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    infos.get(blockId).map { info =>
      while (info.writerTask != BlockInfo.NO_WRITER) {
        if (blocking) wait() else return None
      }
      if (info.removed) return None
      info.readerCount += 1
      readLocksByTask(currentTaskAttemptId).add(blockId)
      logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
      info
    }
  }

  /**
   * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   *
   * If this is called by a task which already holds the block's exclusive write lock, then this
   * method will throw an exception.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    infos.get(blockId).map { info =>
      if (info.writerTask == currentTaskAttemptId) {
        throw new IllegalStateException(
          s"Task $currentTaskAttemptId has already locked $blockId for writing")
      } else {
        while (info.writerTask != BlockInfo.NO_WRITER || info.readerCount != 0) {
          if (blocking) wait() else return None
        }
        if (info.removed) return None
      }
      info.writerTask = currentTaskAttemptId
      writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
      logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
      info
    }
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    infos.get(blockId) match {
      case Some(info) =>
        if (info.writerTask != currentTaskAttemptId) {
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    val info = get(blockId).get
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    unlock(blockId)
    val lockOutcome = lockForReading(blockId, blocking = false)
    assert(lockOutcome.isDefined)
  }

  /**
   * Release a lock on the given block.
   */
  def unlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId releasing lock for $blockId")
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    if (info.writerTask != BlockInfo.NO_WRITER) {
      info.writerTask = BlockInfo.NO_WRITER
      writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
    } else {
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      info.readerCount -= 1
      val countsForTask = readLocksByTask(currentTaskAttemptId)
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      assert(newPinCountForTask >= 0,
        s"Task $currentTaskAttemptId release lock on block $blockId more times than it acquired it")
    }
    notifyAll()
  }

  /**
   * Atomically create metadata for a block and acquire a write lock for it, if it doesn't already
   * exist.
   *
   * @param blockId the block id.
   * @param newBlockInfo the block info for the new block.
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         no new locks are acquired. If this returns true, a write lock on the new block will
   *         be held.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    if (!infos.contains(blockId)) {
      infos(blockId) = newBlockInfo
      newBlockInfo.writerTask = currentTaskAttemptId
      writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
      logTrace(s"Task $currentTaskAttemptId successfully locked new block $blockId")
      true
    } else {
      logTrace(s"Task $currentTaskAttemptId did not create and lock block $blockId " +
        s"because that block already exists")
      false
    }
  }

  /**
   * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   *
   * @return the ids of blocks whose pins were released
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = {
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    val readLocks = synchronized {
      readLocksByTask.remove(taskAttemptId).get
    }
    val writeLocks = synchronized {
      writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)
    }

    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }
    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
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
    blocksWithReleasedLocks
  }

  /**
   * Returns the number of blocks tracked.
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    infos.get(blockId) match {
      case Some(blockInfo) =>
        if (blockInfo.writerTask != currentTaskAttemptId) {
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else {
          infos.remove(blockId)
          blockInfo.readerCount = 0
          blockInfo.writerTask = BlockInfo.NO_WRITER
          blockInfo.removed = true
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
   */
  def clear(): Unit = synchronized {
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
      blockInfo.removed = true
    }
    infos.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    notifyAll()
  }

}

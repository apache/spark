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

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{Condition, Lock}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.google.common.collect.{ConcurrentHashMultiset, ImmutableMultiset}
import com.google.common.util.concurrent.Striped

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging


/**
 * Tracks metadata for an individual block.
 *
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param classTag the block's [[ClassTag]], used to select the serializer
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(
    val level: StorageLevel,
    val classTag: ClassTag[_],
    val tellMaster: Boolean) {

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
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0)
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  checkInvariants()
}

/**
 * Group of blocks that share some common trait (e.g. same broadcast, or same RDD). If there is no
 * obvious grouping, a block has its own unique group. The benefit of grouping blocks is that we can
 * do group level operations. This is especially useful when we need to clean-up groups.
 */
private[storage] abstract class BlockInfoGroup {
  /**
   * Get all the Block to Info entries contained by this group.
   *
   * This method is thread safe. However when you want to modify a [[BlockInfo]] instance, you
   * need to take out the lock for the block it maps to.
   */
  def infos: Seq[(BlockId, BlockInfoWrapper)]

  /**
   * Get the number of blocks inside this group.
   *
   * This method is thread safe.
   */
  def size: Int

  /**
   * Get the (wrapped) [[BlockInfo]] associated with the `blockId`.*
   */
  def get(blockId: BlockId): Option[BlockInfoWrapper]

  /**
   * Clear all entries from this group.
   */
  def clear(): Unit

  /**
   * Associate `blockInfo` with `blockId` if no mapping exists. This method returns `true` if the
   * mapping was added, `false` otherwise.
   */
  def putIfAbsent(blockId: BlockId, blockInfo: BlockInfoWrapper): Boolean

  /**
   * Remove `blockId` from this group. This method returns `true` when the group is empty after
   * removal, `false` otherwise.
   */
  def remove(blockId: BlockId): Boolean
}

class BlockInfoWrapper(
    val info: BlockInfo,
    private val lock: Lock,
    private val condition: Condition) {
  def this(info: BlockInfo, lock: Lock) = this(info, lock, lock.newCondition())

  def withLock[T](f: (BlockInfo, Condition) => T): T = {
    lock.lock()
    try f(info, condition) finally {
      lock.unlock()
    }
  }

  def tryLock(f: (BlockInfo, Condition) => Unit): Unit = {
    if (lock.tryLock()) {
      try f(info, condition) finally {
        lock.unlock()
      }
    }
  }
}

object BlockInfoGroup {
  /**
   * Group of zero or one blocks. This is for blocks for which we do not have a natural mapping.
   */
  class Singleton extends BlockInfoGroup {
    private var blockId: BlockId = _
    private var info: BlockInfoWrapper = _
    override def size: Int = {
      if (blockId != null) 1
      else 0
    }
    override def infos: Seq[(BlockId, BlockInfoWrapper)] = {
      if (blockId != null) blockId -> info :: Nil
      else Nil
    }
    override def clear(): Unit = {
      blockId = null
      info = null
    }
    override def get(blockIdToGet: BlockId): Option[BlockInfoWrapper] = {
      if (blockId == blockIdToGet) Option(info)
      else None
    }
    override def putIfAbsent(blockIdToAdd: BlockId, infoToAdd: BlockInfoWrapper): Boolean = {
      if (blockId == null) {
        blockId = blockIdToAdd
        info = infoToAdd
        true
      } else {
        false
      }
    }
    override def remove(blockIdToRemove: BlockId): Boolean = {
      if (blockId == blockIdToRemove) {
        blockId = null
        info = null
      }
      blockId == null
    }
  }

  /**
   * Group for 0..n blocks. This for blocks that have a natural grouping.
   */
  class Collection extends BlockInfoGroup {
    private val infoMap = new ConcurrentHashMap[BlockId, BlockInfoWrapper]()

    override def infos: Seq[(BlockId, BlockInfoWrapper)] = {
      infoMap.asScala.toSeq
    }

    override def size: Int = infoMap.size()

    override def get(blockId: BlockId): Option[BlockInfoWrapper] = {
      Option(infoMap.get(blockId))
    }

    override def clear(): Unit = {
      infoMap.clear()
    }

    override def putIfAbsent(blockId: BlockId, blockInfo: BlockInfoWrapper): Boolean = {
      infoMap.putIfAbsent(blockId, blockInfo) == null
    }

    override def remove(blockId: BlockId): Boolean = {
      infoMap.remove(blockId)
      infoMap.isEmpty
    }
  }
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
private[storage] class BlockInfoManager(trackingCacheVisibility: Boolean = false) extends Logging {

  private type TaskAttemptId = Long

  /**
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
   */
  private[this] val blockInfoGroups = new ConcurrentHashMap[BlockId, BlockInfoGroup]

  // Cache mappings to avoid O(n) scans in remove operations.
  private[this] val rddToBlockIds =
    new ConcurrentHashMap[Int, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()
  private[this] val broadcastToBlockIds =
    new ConcurrentHashMap[Long, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()
  private[this] val sessionToBlockIds =
    new ConcurrentHashMap[String, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()

  /**
   * Record invisible rdd blocks stored in the block manager, entries will be removed when blocks
   * are marked as visible or blocks are removed by [[removeBlock()]].
   */
  private[this] val invisibleRDDBlocks = new mutable.HashSet[RDDBlockId]

  /**
   * Stripe used to control multi-threaded access to block information.
   *
   * We are using this instead of the synchronizing on the [[BlockInfo]] objects to avoid race
   * conditions in the `lockNewBlockForWriting` method. When this method returns successfully it is
   * assumed that the passed in [[BlockInfo]] object is persisted by the info manager and that it is
   * safe to modify it. The only way we can guarantee this is by having a unique lock per block ID
   * that has a longer lifespan than the blocks' info object.
   */
  private[this] val locks = Striped.lock(1024)

  /**
   * Tracks the set of blocks that each task has locked for writing.
   */
  private[this] val writeLocksByTask = new ConcurrentHashMap[TaskAttemptId, util.Set[BlockId]]

  /** Get the group id that `blockId` belongs to. */
  protected def getGroupId(blockId: BlockId): BlockId = blockId match {
    case BroadcastBlockId(broadcastId, _) => BroadcastBlockId(broadcastId, "group")
    case RDDBlockId(rddId, _) => RDDBlockId(rddId, -1)
    case CacheId(cacheId, _) => CacheId(cacheId, "group")
    case _ => blockId
  }

  def createBlockInfoGroup(groupId: BlockId): BlockInfoGroup = groupId match {
    case _: BroadcastBlockId | _: RDDBlockId | _: CacheId => new BlockInfoGroup.Collection
    case _ => new BlockInfoGroup.Singleton
  }

  /**
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
   */
  private[this] val readLocksByTask =
    new ConcurrentHashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  // Exposed for test only.
  private[storage] def containsInvisibleRDDBlock(blockId: RDDBlockId): Boolean = {
    invisibleRDDBlocks.synchronized {
      invisibleRDDBlocks.contains(blockId)
    }
  }

  private[spark] def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    if (trackingCacheVisibility) {
      invisibleRDDBlocks.synchronized {
        val groupId = getGroupId(blockId)
        val group = blockInfoGroups.get(groupId)
        val blockExists = Option(group).exists(g => g.get(blockId).isDefined)
        blockExists && !invisibleRDDBlocks.contains(blockId)
      }
    } else {
      // Always be visible if the feature flag is disabled.
      true
    }
  }

  private[spark] def tryMarkBlockAsVisible(blockId: RDDBlockId): Unit = {
    if (trackingCacheVisibility) {
      invisibleRDDBlocks.synchronized {
        invisibleRDDBlocks.remove(blockId)
      }
    }
  }

  /**
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = {
    writeLocksByTask.putIfAbsent(taskAttemptId, util.Collections.synchronizedSet(new util.HashSet))
    readLocksByTask.putIfAbsent(taskAttemptId, ConcurrentHashMultiset.create())
  }

  /**
   * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * Helper for lock acquisistion.
   */
  private def acquireLock(
      blockId: BlockId,
      blocking: Boolean)(
      f: BlockInfo => Boolean): Option[BlockInfo] = {
    var done = false
    var result: Option[BlockInfo] = None
    val groupId = getGroupId(blockId)
    while (!done) {
      val group = blockInfoGroups.get(groupId)
      if (group == null) {
        done = true
      } else {
        group.get(blockId) match {
          case Some(wrapper) =>
            wrapper.withLock { (info, condition) =>
              if (f(info)) {
                result = Some(info)
                done = true
              } else if (!blocking) {
                done = true
              } else {
                condition.await()
              }
            }
          case None =>
            done = true
        }
      }
    }
    result
  }

  /**
   * Apply function `f` on the [[BlockInfo]] object and the acquisition [[Condition]] for `blockId`.
   * Function `f` will be executed while holding the lock for the [[BlockInfo]] object. If `blockId`
   * was not registered, an error will be thrown.
   */
  private def blockInfo[T](blockId: BlockId)(f: (BlockInfo, Condition) => T): T = {
    val group = blockInfoGroups.get(getGroupId(blockId))
    if (group == null) {
      throw SparkCoreErrors.blockDoesNotExistError(blockId)
    }
    group.get(blockId) match {
      case Some(wrapper) => wrapper.withLock(f)
      case None => throw SparkCoreErrors.blockDoesNotExistError(blockId)
    }
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
      blocking: Boolean = true): Option[BlockInfo] = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId trying to acquire read lock for $blockId")
    acquireLock(blockId, blocking) { info =>
      val acquire = info.writerTask == BlockInfo.NO_WRITER
      if (acquire) {
        info.readerCount += 1
        readLocksByTask.get(taskAttemptId).add(blockId)
        logTrace(s"Task $taskAttemptId acquired read lock for $blockId")
      }
      acquire
    }
  }

  /**
   * Lock a block for writing and return its metadata.
   *
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId trying to acquire write lock for $blockId")
    acquireLock(blockId, blocking) { info =>
      val acquire = info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0
      if (acquire) {
        info.writerTask = taskAttemptId
        writeLocksByTask.get(taskAttemptId).add(blockId)
        logTrace(s"Task $taskAttemptId acquired write lock for $blockId")
      }
      acquire
    }
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = {
    val taskAttemptId = currentTaskAttemptId
    blockInfo(blockId) { (info, _) =>
      if (info.writerTask != taskAttemptId) {
        throw SparkCoreErrors.taskHasNotLockedBlockError(currentTaskAttemptId, blockId)
      } else {
        info
      }
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = {
    val group = blockInfoGroups.get(getGroupId(blockId))
    if (group == null) {
      return None
    }
    group.get(blockId).map(_.info)
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId downgrading write lock for $blockId")
    blockInfo(blockId) { (info, _) =>
      require(info.writerTask == taskAttemptId,
        s"Task $taskAttemptId tried to downgrade a write lock that it does not hold on" +
          s" block $blockId")
      unlock(blockId)
      val lockOutcome = lockForReading(blockId, blocking = false)
      assert(lockOutcome.isDefined)
    }
  }

  /**
   * Release a lock on the given block.
   * In case a TaskContext is not propagated properly to all child threads for the task, we fail to
   * get the TID from TaskContext, so we have to explicitly pass the TID value to release the lock.
   *
   * See SPARK-18406 for more discussion of this issue.
   */
  def unlock(blockId: BlockId, taskAttemptIdOption: Option[TaskAttemptId] = None): Unit = {
    val taskAttemptId = taskAttemptIdOption.getOrElse(currentTaskAttemptId)
    logTrace(s"Task $taskAttemptId releasing lock for $blockId")
    blockInfo(blockId) { (info, condition) =>
      if (info.writerTask != BlockInfo.NO_WRITER) {
        info.writerTask = BlockInfo.NO_WRITER
        val blockIds = writeLocksByTask.get(taskAttemptId)
        if (blockIds != null) {
          blockIds.remove(blockId)
        }
      } else {
        // There can be a race between unlock and releaseAllLocksForTask which causes negative
        // reader counts. We need to check if the readLocksByTask per tasks are present, if they
        // are not then we know releaseAllLocksForTask has already cleaned up the read lock.
        val countsForTask = readLocksByTask.get(taskAttemptId)
        if (countsForTask != null) {
          assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
          info.readerCount -= 1
          val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
          assert(newPinCountForTask >= 0,
            s"Task $taskAttemptId release lock on block $blockId more times than it acquired it")
        }
      }
      condition.signalAll()
    }
  }

  /**
   * Attempt to acquire the appropriate lock for writing a new block.
   *
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
   *
   * @return true if the block did not already exist, false otherwise.
   *         If this returns true, a write lock on the new block will be held.
   *         If this returns false then a read lock will be held iff keepReadLock == true.
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo,
      keepReadLock: Boolean = true): Boolean = {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    // Get the lock that will be associated with the to-be written block and lock it for the entire
    // duration of this operation. This way we prevent race conditions when two threads try to write
    // the same block at the same time.
    val lock = locks.get(blockId)
    val groupId = getGroupId(blockId)
    lock.lock()
    try {
      val wrapper = new BlockInfoWrapper(newBlockInfo, lock)
      while (true) {
        val group = blockInfoGroups.computeIfAbsent(groupId, _ => {
          createBlockInfoGroup(groupId)
        })
        if (group.putIfAbsent(blockId, wrapper)) {
          if (trackingCacheVisibility) {
            invisibleRDDBlocks.synchronized {
              // Added to invisible blocks if it doesn't exist before.
              blockId.asRDDId.foreach(invisibleRDDBlocks.add)
            }
          }
          // New block lock it for writing.
          val result = lockForWriting(blockId, blocking = false)
          assert(result.isDefined)
          return true
        } else if (!keepReadLock) {
          return false
        } else {
          // Block already exists. This could happen if another thread races with us to compute
          // the same block. In this case we try to acquire a read lock, if the locking succeeds
          // return `false` (the write lock was not acquired) to the caller, if locking fails we
          // retry this entire operation because it means the block was removed.
          if (lockForReading(blockId).isDefined) {
            return false
          }
        }
      }
      false
    } finally {
      lock.unlock()
    }
  }

  /**
   * Get all the blocks that are currently tracked for a group.
   */
  def getBlockIdsForGroup(blockId: BlockId): Seq[BlockId] = {
    val group = blockInfoGroups.get(getGroupId(blockId))
    if (group == null) {
      return Nil
    }
    group.infos.map(_._1)
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

    val writeLocks = Option(writeLocksByTask.remove(taskAttemptId)).getOrElse(util.Set.of())
    writeLocks.forEach { blockId =>
      blockInfo(blockId) { (info, condition) =>
        // Check the existence of `blockId` because `unlock` may have already removed it
        // concurrently.
        if (writeLocks.contains(blockId)) {
          blocksWithReleasedLocks += blockId
          assert(info.writerTask == taskAttemptId)
          info.writerTask = BlockInfo.NO_WRITER
          condition.signalAll()
        }
      }
    }

    val readLocks = Option(readLocksByTask.remove(taskAttemptId))
      .getOrElse(ImmutableMultiset.of[BlockId])
    readLocks.entrySet().forEach { entry =>
      val blockId = entry.getElement
      blockInfo(blockId) { (info, condition) =>
        // Calculating lockCount by readLocks.count instead of entry.getCount is intentional. See
        // discussion in SPARK-50771 and the corresponding PR.
        val lockCount = readLocks.count(blockId)

        // lockCount can be 0 if read locks for `blockId` are released in `unlock` concurrently.
        if (lockCount > 0) {
          blocksWithReleasedLocks += blockId
          info.readerCount -= lockCount
          assert(info.readerCount >= 0)
          condition.signalAll()
        }
      }
    }

    blocksWithReleasedLocks.toSeq
  }

  /** Returns the number of locks held by the given task.  Used only for testing. */
  private[storage] def getTaskLockCount(taskAttemptId: TaskAttemptId): Int = {
    Option(readLocksByTask.get(taskAttemptId)).map(_.size()).getOrElse(0) +
      Option(writeLocksByTask.get(taskAttemptId)).map(_.size).getOrElse(0)
  }

  /**
   * Returns the number of blocks tracked.
   */
  def size: Int = blockInfoGroups.values().asScala.iterator.map(_.size).sum

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = {
    size +
      readLocksByTask.size +
      readLocksByTask.asScala.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.asScala.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  def entries: Iterator[(BlockId, BlockInfo)] = {
    blockInfoGroups.values().asScala.flatMap(_.infos.map(kv => kv._1 -> kv._2.info)).iterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  def removeBlock(blockId: BlockId): Unit = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId trying to remove block $blockId")
    val groupId = getGroupId(blockId)
    val group = blockInfoGroups.get(groupId)
    if (group == null) {
      throw SparkCoreErrors.blockDoesNotExistError(blockId)
    }
    blockInfo(blockId) { (info, condition) =>
      if (info.writerTask != taskAttemptId) {
        throw SparkException.internalError(
          s"Task $taskAttemptId called remove() on block $blockId without a write lock",
          category = "STORAGE")
      } else {
        invisibleRDDBlocks.synchronized {
          blockId.asRDDId.foreach(invisibleRDDBlocks.remove)
        }
        if (group.remove(blockId)) {
          blockInfoGroups.remove(groupId)
        }
        info.readerCount = 0
        info.writerTask = BlockInfo.NO_WRITER
        writeLocksByTask.get(taskAttemptId).remove(blockId)
      }
      condition.signalAll()
    }
  }

  /**
   * Delete all state. Called during shutdown.
   */
  def clear(): Unit = {
    blockInfoGroups.values().forEach { group =>
      group.infos.foreach {
        case (_, wrapper) =>
          wrapper.tryLock { (info, condition) =>
            info.readerCount = 0
            info.writerTask = BlockInfo.NO_WRITER
            condition.signalAll()
          }
      }
      group.clear()
    }
    blockInfoGroups.clear()
    rddToBlockIds.clear()
    broadcastToBlockIds.clear()
    sessionToBlockIds.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    invisibleRDDBlocks.synchronized {
      invisibleRDDBlocks.clear()
    }
  }
}

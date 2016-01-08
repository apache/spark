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

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.google.common.collect.ConcurrentHashMultiset

private[storage] class MemoryStoreReferenceCounter {

  type TaskAttemptId = java.lang.Long

  private[this] val allReferencedBlocks = ConcurrentHashMultiset.create[BlockId]()
  private[this] val blocksReferencedByTask = {
    val loader = new CacheLoader[TaskAttemptId, ConcurrentHashMultiset[BlockId]] {
      override def load(t: TaskAttemptId) = ConcurrentHashMultiset.create[BlockId]()
    }
    CacheBuilder.newBuilder().build(loader)
  }

  /**
   * Return the number of map entries in this reference counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = {
    allReferencedBlocks.size() +
      blocksReferencedByTask.size() +
      blocksReferencedByTask.asMap().asScala.map(_._2.size()).sum
  }

  def getReferenceCountForBlock(blockId: BlockId): Int = allReferencedBlocks.count(blockId)

  def retainBlockForTask(taskAttemptId: TaskAttemptId, blockId: BlockId): Unit = {
    blocksReferencedByTask.get(taskAttemptId).add(blockId)
    allReferencedBlocks.add(blockId)
  }

  def releaseBlockForTask(taskAttemptId: TaskAttemptId, blockId: BlockId): Unit = {
    val countsForTask = blocksReferencedByTask.get(taskAttemptId)
    val newReferenceCountForTask: Int = countsForTask.remove(blockId, 1) - 1
    val newTotalReferenceCount: Int = allReferencedBlocks.remove(blockId, 1) - 1
    if (newReferenceCountForTask < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId block $blockId more times than it was retained")
    }
    if (newTotalReferenceCount < 0) {
      throw new IllegalStateException(
        s"Task $taskAttemptId block $blockId more times than it was retained")
    }
  }

  def releaseAllBlocksForTaskAttempt(taskAttemptId: TaskAttemptId): Unit = {
    val referenceCounts = blocksReferencedByTask.get(taskAttemptId)
    blocksReferencedByTask.invalidate(taskAttemptId)
    referenceCounts.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val taskRefCount = entry.getCount
      val newRefCount = allReferencedBlocks.remove(blockId, taskRefCount) - taskRefCount
      if (newRefCount < 0) {
        throw new IllegalStateException(
          s"Task $taskAttemptId block $blockId more times than it was retained")
      }
    }
  }
}

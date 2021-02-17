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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

class TestMemoryManager(conf: SparkConf)
  extends MemoryManager(conf, numCores = 1, Long.MaxValue, Long.MaxValue) {

  @GuardedBy("this")
  private var consequentOOM = 0
  @GuardedBy("this")
  private var available = Long.MaxValue
  @GuardedBy("this")
  private val memoryForTask = mutable.HashMap[Long, Long]().withDefaultValue(0L)

  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    require(numBytes >= 0)
    val acquired = {
      if (consequentOOM > 0) {
        consequentOOM -= 1
        0
      } else if (available >= numBytes) {
        available -= numBytes
        numBytes
      } else {
        val grant = available
        available = 0
        grant
      }
    }
    memoryForTask(taskAttemptId) = memoryForTask.getOrElse(taskAttemptId, 0L) + acquired
    acquired
  }

  override private[memory] def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    require(numBytes >= 0)
    available += numBytes
    val existingMemoryUsage = memoryForTask.getOrElse(taskAttemptId, 0L)
    val newMemoryUsage = existingMemoryUsage - numBytes
    require(
      newMemoryUsage >= 0,
      s"Attempting to free $numBytes of memory for task attempt $taskAttemptId, but it only " +
      s"allocated $existingMemoryUsage bytes of memory")
    memoryForTask(taskAttemptId) = newMemoryUsage
  }

  override private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = {
    memoryForTask.remove(taskAttemptId).getOrElse(0L)
  }

  override private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    require(numBytes >= 0)
    true
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    require(numBytes >= 0)
    true
  }

  override def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = {
    require(numBytes >= 0)
  }

  override def maxOnHeapStorageMemory: Long = Long.MaxValue

  override def maxOffHeapStorageMemory: Long = 0L

  /**
   * Causes the next call to [[acquireExecutionMemory()]] to fail to allocate
   * memory (returning `0`), simulating low-on-memory / out-of-memory conditions.
   */
  def markExecutionAsOutOfMemoryOnce(): Unit = {
    markconsequentOOM(1)
  }

  /**
   * Causes the next `n` calls to [[acquireExecutionMemory()]] to fail to allocate
   * memory (returning `0`), simulating low-on-memory / out-of-memory conditions.
   */
  def markconsequentOOM(n: Int): Unit = synchronized {
    consequentOOM += n
  }

  /**
   * Undos the effects of [[markExecutionAsOutOfMemoryOnce]] and [[markconsequentOOM]] and lets
   * calls to [[acquireExecutionMemory()]] (if there is enough memory available).
   */
  def resetConsequentOOM(): Unit = synchronized {
    consequentOOM = 0
  }

  def limit(avail: Long): Unit = synchronized {
    require(avail >= 0)
    available = avail
  }
}

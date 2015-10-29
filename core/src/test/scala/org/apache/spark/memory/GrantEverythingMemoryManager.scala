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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

class GrantEverythingMemoryManager(conf: SparkConf)
    extends MemoryManager(conf, numCores = 1, Long.MaxValue) {
  override private[memory] def acquireOnHeapExecutionMemory(
    numBytes: Long,
    taskAttemptId: Long): Long = {
    if (oom) {
      oom = false
      0
    } else {
      numBytes
    }
  }
  override private[memory] def acquireOffHeapExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long): Long = {
    acquireOnHeapExecutionMemory(numBytes, taskAttemptId)
  }
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true
  override def releaseStorageMemory(numBytes: Long): Unit = { }
  override private[memory] def releaseOnHeapExecutionMemory(numBytes: Long, tid: Long): Unit = {}
  override private[memory] def releaseOffHeapExecutionMemory(numBytes: Long, tid: Long): Unit = {}
  override def maxStorageMemory: Long = Long.MaxValue

  private var oom = false
  def markExecutionAsOutOfMemory(): Unit = {
    oom = true
  }
}

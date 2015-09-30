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

import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.storage.BlockManager


/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 */
private[spark] class StaticMemoryManager(conf: SparkConf) extends MemoryManager {
  private val maxExecutionMemory = ShuffleMemoryManager.getMaxMemory(conf)
  private val maxStorageMemory = BlockManager.getMaxMemory(conf)
  @volatile private var executionMemoryUsed: Long = 0
  @volatile private var storageMemoryUsed: Long = 0

  /**
   * Acquire N bytes of memory for execution.
   * @return whether all N bytes are successfully granted.
   */
  override def acquireExecutionMemory(numBytes: Long): Boolean = {
    if (executionMemoryUsed + numBytes <= maxExecutionMemory) {
      executionMemoryUsed += numBytes
      true
    } else {
      false
    }
  }

  /**
   * Acquire N bytes of memory for storage.
   * @return whether all N bytes are successfully granted.
   */
  override def acquireStorageMemory(numBytes: Long): Boolean = {
    if (storageMemoryUsed + numBytes <= maxStorageMemory) {
      storageMemoryUsed += numBytes
      true
    } else {
      false
    }
  }

  /**
   * Release N bytes of execution memory.
   */
  override def releaseExecutionMemory(numBytes: Long): Unit = {
    executionMemoryUsed -= numBytes
  }

  /**
   * Release N bytes of storage memory.
   */
  override def releaseStorageMemory(numBytes: Long): Unit = {
    storageMemoryUsed -= numBytes
  }

}

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


/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 */
private[spark] class StaticMemoryManager(conf: SparkConf = new SparkConf)
  extends MemoryManager with Logging {

  private val _maxExecutionMemory: Long = StaticMemoryManager.getMaxExecutionMemory(conf)
  private val _maxStorageMemory: Long = StaticMemoryManager.getMaxStorageMemory(conf)
  private val executionMemoryLock = new Object
  private val storageMemoryLock = new Object

  // All accesses must be synchronized on `executionMemoryLock`
  private var executionMemoryUsed: Long = 0

  // All accesses must be synchronized on `storageMemoryLock`
  private var storageMemoryUsed: Long = 0

  /**
   * Total available memory for execution, in bytes.
   */
  override def maxExecutionMemory: Long = _maxExecutionMemory

  /**
   * Total available memory for storage, in bytes.
   */
  override def maxStorageMemory: Long = _maxStorageMemory

  /**
   * Acquire N bytes of memory for execution.
   * @return whether the number bytes successfully granted (<= N).
   */
  override def acquireExecutionMemory(numBytes: Long): Long = {
    executionMemoryLock.synchronized {
      assert(_maxExecutionMemory >= executionMemoryUsed)
      val bytesToGrant = math.min(numBytes, _maxExecutionMemory - executionMemoryUsed)
      executionMemoryUsed += bytesToGrant
      bytesToGrant
    }
  }

  /**
   * Acquire N bytes of memory for storage.
   * @return whether the number bytes successfully granted (<= N).
   */
  override def acquireStorageMemory(numBytes: Long): Long = {
    storageMemoryLock.synchronized {
      assert(_maxStorageMemory >= storageMemoryUsed)
      val bytesToGrant = math.min(numBytes, _maxStorageMemory - storageMemoryUsed)
      storageMemoryUsed += bytesToGrant
      bytesToGrant
    }
  }

  /**
   * Release N bytes of execution memory.
   */
  override def releaseExecutionMemory(numBytes: Long): Unit = {
    executionMemoryLock.synchronized {
      if (numBytes > executionMemoryUsed) {
        logWarning(s"Attempted to release $numBytes bytes of execution " +
          s"memory when we only have $executionMemoryUsed bytes")
        executionMemoryUsed = 0
      } else {
        executionMemoryUsed -= numBytes
      }
    }
  }

  /**
   * Release N bytes of storage memory.
   */
  override def releaseStorageMemory(numBytes: Long): Unit = {
    storageMemoryLock.synchronized {
      if (numBytes > storageMemoryUsed) {
        logWarning(s"Attempted to release $numBytes bytes of storage " +
          s"memory when we only have $storageMemoryUsed bytes")
        storageMemoryUsed = 0
      } else {
        storageMemoryUsed -= numBytes
      }
    }
  }

}


private object StaticMemoryManager {

  /**
   * Return the total amount of memory available for the storage region, in bytes.
   */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }


  /**
   * Return the total amount of memory available for the execution region, in bytes.
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

}

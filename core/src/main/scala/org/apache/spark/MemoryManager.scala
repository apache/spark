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
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster.
 */
private[spark] abstract class MemoryManager {

  /**
   * Acquire N bytes of memory for execution.
   * @return whether all N bytes are successfully granted.
   */
  def acquireExecutionMemory(numBytes: Long): Boolean

  /**
   * Acquire N bytes of memory for storage.
   * @return whether all N bytes are successfully granted.
   */
  def acquireStorageMemory(numBytes: Long): Boolean

  /**
   * Release N bytes of execution memory.
   */
  def releaseExecutionMemory(numBytes: Long): Unit

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long): Unit

}

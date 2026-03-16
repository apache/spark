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

/**
 * Identifier for an unmanaged memory consumer.
 *
 * @param componentType The type of component (e.g., "RocksDB", "NativeLibrary")
 * @param instanceKey A unique key to identify this specific instance of the component.
 *                    For shared memory consumers, this should be a common key across
 *                    all instances to avoid double counting.
 */
case class UnmanagedMemoryConsumerId(
    componentType: String,
    instanceKey: String
)

/**
 * Interface for components that consume memory outside of Spark's unified memory management.
 *
 * Components implementing this trait can register themselves with the memory manager
 * to have their memory usage tracked and factored into memory allocation decisions.
 * This helps prevent OOM errors when unmanaged components use significant memory.
 *
 * Examples of unmanaged memory consumers:
 * - RocksDB state stores in structured streaming
 * - Native libraries with custom memory allocation
 * - Off-heap caches managed outside of Spark
 */
trait UnmanagedMemoryConsumer {
  /**
   * Returns the unique identifier for this memory consumer.
   * The identifier is used to track and manage the consumer in the memory tracking system.
   */
  def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId

  /**
   * Returns the memory mode (ON_HEAP or OFF_HEAP) that this consumer uses.
   * This is used to ensure unmanaged memory usage only affects the correct memory pool.
   */
  def memoryMode: MemoryMode

  /**
   * Returns the current memory usage in bytes.
   *
   * This method is called periodically by the memory polling mechanism to track
   * memory usage over time. Implementations should return the current total memory
   * consumed by this component.
   *
   * @return Current memory usage in bytes. Should return 0 if no memory is currently used.
   *         Return -1L to indicate this consumer is no longer active and should be
   *         automatically removed from tracking.
   * @throws Exception if memory usage cannot be determined. The polling mechanism
   *                   will handle exceptions gracefully and log warnings.
   */
  def getMemBytesUsed: Long
}

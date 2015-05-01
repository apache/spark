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

package org.apache.spark.unsafe.memory;

/**
 * Manages memory for an executor. Individual operators / tasks allocate memory through
 * {@link TaskMemoryManager} objects, which obtain their memory from ExecutorMemoryManager.
 */
public class ExecutorMemoryManager {

  /**
   * Allocator, exposed for enabling untracked allocations of temporary data structures.
   */
  public final MemoryAllocator allocator;

  /**
   * Tracks whether memory will be allocated on the JVM heap or off-heap using sun.misc.Unsafe.
   */
  final boolean inHeap;

  /**
   * Construct a new ExecutorMemoryManager.
   *
   * @param allocator the allocator that will be used
   */
  public ExecutorMemoryManager(MemoryAllocator allocator) {
    this.inHeap = allocator instanceof HeapMemoryAllocator;
    this.allocator = allocator;
  }

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary).
   */
  MemoryBlock allocate(long size) throws OutOfMemoryError {
    return allocator.allocate(size);
  }

  void free(MemoryBlock memory) {
    allocator.free(memory);
  }

}

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

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;

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

  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<Long, LinkedList<WeakReference<MemoryBlock>>>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

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
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    // At some point, we should explore supporting pooling for off-heap memory, but for now we'll
    // ignore that case in the interest of simplicity.
    return size >= POOLING_THRESHOLD_BYTES && allocator instanceof HeapMemoryAllocator;
  }

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary).
   */
  MemoryBlock allocate(long size) throws OutOfMemoryError {
    if (shouldPool(size)) {
      synchronized (this) {
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) {
              assert (memory.size() == size);
              return memory;
            }
          }
          bufferPoolsBySize.remove(size);
        }
      }
      return allocator.allocate(size);
    } else {
      return allocator.allocate(size);
    }
  }

  void free(MemoryBlock memory) {
    final long size = memory.size();
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<WeakReference<MemoryBlock>>();
          bufferPoolsBySize.put(size, pool);
        }
        pool.add(new WeakReference<MemoryBlock>(memory));
      }
    } else {
      allocator.free(memory);
    }
  }

}

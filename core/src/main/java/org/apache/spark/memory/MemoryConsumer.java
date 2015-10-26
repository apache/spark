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

package org.apache.spark.memory;


import java.io.IOException;

import org.apache.spark.unsafe.memory.MemoryBlock;


/**
 * An memory consumer of TaskMemoryManager, which support spilling.
 */
public class MemoryConsumer {

  private TaskMemoryManager memoryManager;
  private long pageSize;

  protected MemoryConsumer(TaskMemoryManager memoryManager, long pageSize) {
    this.memoryManager = memoryManager;
    this.pageSize = pageSize;
  }

  protected MemoryConsumer(TaskMemoryManager memoryManager) {
    this(memoryManager, memoryManager.pageSizeBytes());
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * @param size the amount of memory should be released
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public long spill(long size) throws IOException {
    return 0L;
  }

  /**
   * Acquire `size` bytes memory.
   *
   * If there is not enough memory, throws IOException.
   *
   * @throws IOException
   */
  protected void acquireMemory(long size) throws IOException {
    long got = memoryManager.acquireExecutionMemory(size, this);
    if (got < size) {
      throw new IOException("Could not acquire " + size + " bytes of memory " + got);
    }
  }

  /**
   * Release amount of memory.
   */
  protected void releaseMemory(long size) {
    memoryManager.releaseExecutionMemory(size, this);
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * Throws IOException if there is not enough memory.
   *
   * @throws IOException
   */
  protected MemoryBlock allocatePage(long required) throws IOException {
    MemoryBlock page = memoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      if (page != null) {
        freePage(page);
      }
      throw new IOException("Unable to acquire " + required + " bytes of memory");
    }
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    memoryManager.freePage(page, this);
  }
}

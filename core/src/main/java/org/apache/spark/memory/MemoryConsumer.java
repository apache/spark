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
public abstract class MemoryConsumer {

  private final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  private long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.used = 0;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes());
  }

  /**
   * Returns the size of used memory in bytes.
   */
  long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   *
   * For testing.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Acquire `size` bytes memory.
   *
   * If there is not enough memory, throws OutOfMemoryError.
   */
  protected void acquireMemory(long size) {
    long got = taskMemoryManager.acquireExecutionMemory(size, this);
    if (got < size) {
      taskMemoryManager.releaseExecutionMemory(got, this);
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Could not acquire " + size + " bytes of memory, got " + got);
    }
    used += got;
  }

  /**
   * Release `size` bytes memory.
   */
  protected void releaseMemory(long size) {
    used -= size;
    taskMemoryManager.releaseExecutionMemory(size, this);
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * Throws IOException if there is not enough memory.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        freePage(page);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += page.size();
    return page;
  }

  /**
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }
}

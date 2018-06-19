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

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.unsafe.memory.MemoryBlock;

import java.io.IOException;

public class TestMemoryConsumer extends MemoryConsumer {
  public TestMemoryConsumer(TaskMemoryManager memoryManager, MemoryMode mode) {
    super(memoryManager, 1024L, mode);
  }
  public TestMemoryConsumer(TaskMemoryManager memoryManager) {
    this(memoryManager, MemoryMode.ON_HEAP);
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    long used = getUsed();
    free(used);
    return used;
  }

  void use(long size) {
    long got = taskMemoryManager.acquireExecutionMemory(size, this);
    used += got;
  }

  void free(long size) {
    used -= size;
    taskMemoryManager.releaseExecutionMemory(size, this);
  }

  @VisibleForTesting
  public void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }
}



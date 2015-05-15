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

import org.junit.Assert;
import org.junit.Test;

public class TaskMemoryManagerSuite {

  @Test
  public void leakedNonPageMemoryIsDetected() {
    final TaskMemoryManager manager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
    manager.allocate(1024);  // leak memory
    Assert.assertEquals(1024, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void leakedPageMemoryIsDetected() {
    final TaskMemoryManager manager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
    manager.allocatePage(4096);  // leak memory
    Assert.assertEquals(4096, manager.cleanUpAllAllocatedMemory());
  }

  @Test
  public void encodePageNumberAndOffsetOffHeap() {
    final TaskMemoryManager manager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.UNSAFE));
    final MemoryBlock dataPage = manager.allocatePage(256);
    // In off-heap mode, an offset is an absolute address that may require more than 51 bits to
    // encode. This test exercises that corner-case:
    final long offset = ((1L << TaskMemoryManager.OFFSET_BITS) + 10);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, offset);
    Assert.assertEquals(null, manager.getPage(encodedAddress));
    Assert.assertEquals(offset, manager.getOffsetInPage(encodedAddress));
  }

  @Test
  public void encodePageNumberAndOffsetOnHeap() {
    final TaskMemoryManager manager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
    final MemoryBlock dataPage = manager.allocatePage(256);
    final long encodedAddress = manager.encodePageNumberAndOffset(dataPage, 64);
    Assert.assertEquals(dataPage.getBaseObject(), manager.getPage(encodedAddress));
    Assert.assertEquals(64, manager.getOffsetInPage(encodedAddress));
  }

}

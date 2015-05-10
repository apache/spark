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

package org.apache.spark.shuffle.unsafe;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

public class PackedRecordPointerSuite {

  @Test
  public void heap() {
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP));
    final MemoryBlock page0 = memoryManager.allocatePage(100);
    final MemoryBlock page1 = memoryManager.allocatePage(100);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1, 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    Assert.assertEquals(360, packedPointer.getPartitionId());
    Assert.assertEquals(addressInPage1, packedPointer.getRecordPointer());
    memoryManager.cleanUpAllAllocatedMemory();
  }

  @Test
  public void offHeap() {
    final TaskMemoryManager memoryManager =
      new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.UNSAFE));
    final MemoryBlock page0 = memoryManager.allocatePage(100);
    final MemoryBlock page1 = memoryManager.allocatePage(100);
    final long addressInPage1 = memoryManager.encodePageNumberAndOffset(page1, 42);
    PackedRecordPointer packedPointer = new PackedRecordPointer();
    packedPointer.set(PackedRecordPointer.packPointer(addressInPage1, 360));
    Assert.assertEquals(360, packedPointer.getPartitionId());
    Assert.assertEquals(addressInPage1, packedPointer.getRecordPointer());
    memoryManager.cleanUpAllAllocatedMemory();
  }
}

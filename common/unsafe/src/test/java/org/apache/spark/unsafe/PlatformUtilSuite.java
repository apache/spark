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

package org.apache.spark.unsafe;

import org.apache.spark.unsafe.memory.HeapMemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.junit.Assert;
import org.junit.Test;

public class PlatformUtilSuite {

  @Test
  public void overlappingCopyMemory() {
    byte[] data = new byte[3 * 1024 * 1024];
    int size = 2 * 1024 * 1024;
    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte)i;
    }

    Platform.copyMemory(data, Platform.BYTE_ARRAY_OFFSET, data, Platform.BYTE_ARRAY_OFFSET, size);
    for (int i = 0; i < data.length; ++i) {
      Assert.assertEquals((byte)i, data[i]);
    }

    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET + 1,
        data,
        Platform.BYTE_ARRAY_OFFSET,
        size);
    for (int i = 0; i < size; ++i) {
      Assert.assertEquals((byte)(i + 1), data[i]);
    }

    for (int i = 0; i < data.length; ++i) {
      data[i] = (byte)i;
    }
    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET,
        data,
        Platform.BYTE_ARRAY_OFFSET + 1,
        size);
    for (int i = 0; i < size; ++i) {
      Assert.assertEquals((byte)i, data[i + 1]);
    }
  }

  @Test
  public void onHeapMemoryAllocatorPoolingReUsesLongArrays() {
    MemoryBlock block1 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object baseObject1 = block1.getBaseObject();
    MemoryAllocator.HEAP.free(block1);
    MemoryBlock block2 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object baseObject2 = block2.getBaseObject();
    Assert.assertSame(baseObject1, baseObject2);
    MemoryAllocator.HEAP.free(block2);
  }

  @Test
  public void freeingOnHeapMemoryBlockResetsBaseObjectAndOffset() {
    MemoryBlock block = MemoryAllocator.HEAP.allocate(1024);
    Assert.assertNotNull(block.getBaseObject());
    MemoryAllocator.HEAP.free(block);
    Assert.assertNull(block.getBaseObject());
    Assert.assertEquals(0, block.getBaseOffset());
    Assert.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, block.pageNumber);
  }

  @Test
  public void freeingOffHeapMemoryBlockResetsOffset() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(1024);
    Assert.assertNull(block.getBaseObject());
    Assert.assertNotEquals(0, block.getBaseOffset());
    MemoryAllocator.UNSAFE.free(block);
    Assert.assertNull(block.getBaseObject());
    Assert.assertEquals(0, block.getBaseOffset());
    Assert.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, block.pageNumber);
  }

  @Test
  public void onHeapMemoryAllocatorThrowsAssertionErrorOnDoubleFree() {
    MemoryBlock block = MemoryAllocator.HEAP.allocate(1024);
    MemoryAllocator.HEAP.free(block);
    Assert.assertThrows(AssertionError.class, () -> MemoryAllocator.HEAP.free(block));
  }

  @Test
  public void offHeapMemoryAllocatorThrowsAssertionErrorOnDoubleFree() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(1024);
    MemoryAllocator.UNSAFE.free(block);
    Assert.assertThrows(AssertionError.class, () -> MemoryAllocator.UNSAFE.free(block));
  }

  @Test
  public void memoryDebugFillEnabledInTest() {
    Assert.assertTrue(MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED);
    MemoryBlock onheap = MemoryAllocator.HEAP.allocate(1);
    Assert.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE,
      Platform.getByte(onheap.getBaseObject(), onheap.getBaseOffset()));

    MemoryBlock onheap1 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object onheap1BaseObject = onheap1.getBaseObject();
    long onheap1BaseOffset = onheap1.getBaseOffset();
    MemoryAllocator.HEAP.free(onheap1);
    Assert.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE,
      Platform.getByte(onheap1BaseObject, onheap1BaseOffset));
    MemoryBlock onheap2 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Assert.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE,
      Platform.getByte(onheap2.getBaseObject(), onheap2.getBaseOffset()));

    MemoryBlock offheap = MemoryAllocator.UNSAFE.allocate(1);
    Assert.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE,
      Platform.getByte(offheap.getBaseObject(), offheap.getBaseOffset()));
    MemoryAllocator.UNSAFE.free(offheap);
  }

  @Test
  public void heapMemoryReuse() {
    MemoryAllocator heapMem = new HeapMemoryAllocator();
    // The size is less than `HeapMemoryAllocator.POOLING_THRESHOLD_BYTES`,
    // allocate new memory every time.
    MemoryBlock onheap1 = heapMem.allocate(513);
    Object obj1 = onheap1.getBaseObject();
    heapMem.free(onheap1);
    MemoryBlock onheap2 = heapMem.allocate(514);
    Assert.assertNotEquals(obj1, onheap2.getBaseObject());

    // The size is greater than `HeapMemoryAllocator.POOLING_THRESHOLD_BYTES`,
    // reuse the previous memory which has released.
    MemoryBlock onheap3 = heapMem.allocate(1024 * 1024 + 1);
    Assert.assertEquals(1024 * 1024 + 1, onheap3.size());
    Object obj3 = onheap3.getBaseObject();
    heapMem.free(onheap3);
    MemoryBlock onheap4 = heapMem.allocate(1024 * 1024 + 7);
    Assert.assertEquals(1024 * 1024 + 7, onheap4.size());
    Assert.assertEquals(obj3, onheap4.getBaseObject());
  }
}

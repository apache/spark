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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
      Assertions.assertEquals((byte)i, data[i]);
    }

    Platform.copyMemory(
        data,
        Platform.BYTE_ARRAY_OFFSET + 1,
        data,
        Platform.BYTE_ARRAY_OFFSET,
        size);
    for (int i = 0; i < size; ++i) {
      Assertions.assertEquals((byte)(i + 1), data[i]);
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
      Assertions.assertEquals((byte)i, data[i + 1]);
    }
  }

  @Test
  public void onHeapMemoryAllocatorPoolingReUsesLongArrays() {
    MemoryBlock block1 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object baseObject1 = block1.getBaseObject();
    MemoryAllocator.HEAP.free(block1);
    MemoryBlock block2 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object baseObject2 = block2.getBaseObject();
    Assertions.assertSame(baseObject1, baseObject2);
    MemoryAllocator.HEAP.free(block2);
  }

  @Test
  public void freeingOnHeapMemoryBlockResetsBaseObjectAndOffset() {
    MemoryBlock block = MemoryAllocator.HEAP.allocate(1024);
    Assertions.assertNotNull(block.getBaseObject());
    MemoryAllocator.HEAP.free(block);
    Assertions.assertNull(block.getBaseObject());
    Assertions.assertEquals(0, block.getBaseOffset());
    Assertions.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, block.pageNumber);
  }

  @Test
  public void freeingOffHeapMemoryBlockResetsOffset() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(1024);
    Assertions.assertNull(block.getBaseObject());
    Assertions.assertNotEquals(0, block.getBaseOffset());
    MemoryAllocator.UNSAFE.free(block);
    Assertions.assertNull(block.getBaseObject());
    Assertions.assertEquals(0, block.getBaseOffset());
    Assertions.assertEquals(MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER, block.pageNumber);
  }

  @Test
  public void onHeapMemoryAllocatorThrowsAssertionErrorOnDoubleFree() {
    MemoryBlock block = MemoryAllocator.HEAP.allocate(1024);
    MemoryAllocator.HEAP.free(block);
    Assertions.assertThrows(AssertionError.class, () -> MemoryAllocator.HEAP.free(block));
  }

  @Test
  public void offHeapMemoryAllocatorThrowsAssertionErrorOnDoubleFree() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(1024);
    MemoryAllocator.UNSAFE.free(block);
    Assertions.assertThrows(AssertionError.class, () -> MemoryAllocator.UNSAFE.free(block));
  }

  @Test
  public void memoryDebugFillEnabledInTest() {
    Assertions.assertTrue(MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED);
    MemoryBlock onheap = MemoryAllocator.HEAP.allocate(1);
    Assertions.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE,
      Platform.getByte(onheap.getBaseObject(), onheap.getBaseOffset()));

    MemoryBlock onheap1 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Object onheap1BaseObject = onheap1.getBaseObject();
    long onheap1BaseOffset = onheap1.getBaseOffset();
    MemoryAllocator.HEAP.free(onheap1);
    Assertions.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE,
      Platform.getByte(onheap1BaseObject, onheap1BaseOffset));
    MemoryBlock onheap2 = MemoryAllocator.HEAP.allocate(1024 * 1024);
    Assertions.assertEquals(
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE,
      Platform.getByte(onheap2.getBaseObject(), onheap2.getBaseOffset()));

    MemoryBlock offheap = MemoryAllocator.UNSAFE.allocate(1);
    Assertions.assertEquals(
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
    Assertions.assertNotEquals(obj1, onheap2.getBaseObject());

    // The size is greater than `HeapMemoryAllocator.POOLING_THRESHOLD_BYTES`,
    // reuse the previous memory which has released.
    MemoryBlock onheap3 = heapMem.allocate(1024 * 1024 + 1);
    Assertions.assertEquals(1024 * 1024 + 1, onheap3.size());
    Object obj3 = onheap3.getBaseObject();
    heapMem.free(onheap3);
    MemoryBlock onheap4 = heapMem.allocate(1024 * 1024 + 7);
    Assertions.assertEquals(1024 * 1024 + 7, onheap4.size());
    Assertions.assertEquals(obj3, onheap4.getBaseObject());
  }

  @Test
  public void cleanerCreateMethodIsDefined() {
    // Regression test for SPARK-45508: we don't expect the "no cleaner" fallback
    // path to be hit in normal usage.
    Assertions.assertTrue(Platform.cleanerCreateMethodIsDefined());
  }

  @Test
  public void reallocateMemoryGrow() {
    long address = Platform.allocateMemory(1024);
    try {
      for (int i = 0; i < 1024; i++) {
        Platform.putByte(null, address + i, (byte) i);
      }

      address = Platform.reallocateMemory(address, 1024, 2048);
      for (int i = 0; i < 1024; i++) {
        Assertions.assertEquals((byte) i, Platform.getByte(null, address + i));
      }
      for (int i = 1024; i < 2048; i++) {
        Platform.putByte(null, address + i, (byte) i);
      }
      for (int i = 1024; i < 2048; i++) {
        Assertions.assertEquals((byte) i, Platform.getByte(null, address + i));
      }
    } finally {
      Platform.freeMemory(address);
    }
  }

  @Test
  public void reallocateMemoryShrinkDoesNotOverflow() {
    final long OLD_SIZE = 1024L;
    final long NEW_SIZE = 512L;
    final long SENTINEL_SIZE = OLD_SIZE - NEW_SIZE;
    final byte SENTINEL_VALUE = (byte) 0xAB;
    final byte SOURCE_VALUE   = (byte) 0xCD;
    final int  PAIRS = 1000;

    long[] sources = new long[PAIRS];
    long[] sentinels = new long[PAIRS];

    try {
      // Allocate all pairs
      for (int i = 0; i < PAIRS; i++) {
        sources[i] = Platform.allocateMemory(OLD_SIZE);
        sentinels[i] = Platform.allocateMemory(SENTINEL_SIZE);

        Platform.setMemory(sources[i], SOURCE_VALUE, OLD_SIZE);
        Platform.setMemory(sentinels[i], SENTINEL_VALUE, SENTINEL_SIZE);
      }

      // Reallocate source blocks
      for (int i = 0; i < PAIRS; i++) {
        long oldAddr = sources[i];
        long newAddr = Platform.reallocateMemory(oldAddr, OLD_SIZE, NEW_SIZE);
        sources[i] = newAddr;
      }

      // Verify dest content
      for (int i = 0; i < PAIRS; i++) {
        for (long j = 0; j < NEW_SIZE; j++) {
          Assertions.assertEquals(SOURCE_VALUE, Platform.getByte(null, sources[i] + j),
            "dest block content corrupted at pair " + i);
        }
      }

      // Verify sentinel content
      for (int i = 0; i < PAIRS; i++) {
        for (long j = 0; j < SENTINEL_SIZE; j++) {
          Assertions.assertEquals(SENTINEL_VALUE, Platform.getByte(null, sentinels[i] + j),
            "sentinel corrupted – overflow write detected at pair " + i);
        }
      }

    } finally {
      // Free all memory
      for (long addr : sources) {
        if (addr != 0) Platform.freeMemory(addr);
      }
      for (long addr : sentinels) {
        if (addr != 0) Platform.freeMemory(addr);
      }
    }
  }
}

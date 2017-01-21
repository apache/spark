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

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryAllocator;

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
  public void memoryDebugFillEnabledInTest() {
    Assert.assertTrue(MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED);
    MemoryBlock onheap = MemoryAllocator.HEAP.allocate(1);
    MemoryBlock offheap = MemoryAllocator.UNSAFE.allocate(1);
    Assert.assertEquals(
      Platform.getByte(onheap.getBaseObject(), onheap.getBaseOffset()),
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    Assert.assertEquals(
      Platform.getByte(offheap.getBaseObject(), offheap.getBaseOffset()),
      MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }

  /**
   * This test ensures that double access is handled correctly for the host platform.
   * On platforms that require double access to be aligned to 8-byte boundaries, this
   * test should pass and not cause the JVM to segfault.
   */
  @Test
  public void unalignedDoublePutAndGet() {
    MemoryBlock testBuffer = MemoryBlock.fromLongArray(new long[20]);

    // write a double to an unaligned location
    long unalignedOffset = testBuffer.getBaseOffset() + 3;
    // double check unalignment just to be sure:
    if (unalignedOffset % 8 == 0) {
      unalignedOffset++;
    }

    Platform.putDouble(testBuffer.getBaseObject(), unalignedOffset, 3.14159);

    double foundDouble = Platform.getDouble(testBuffer.getBaseObject(), unalignedOffset);

    Assert.assertEquals(3.14159, foundDouble, 0.000001);
  }
}

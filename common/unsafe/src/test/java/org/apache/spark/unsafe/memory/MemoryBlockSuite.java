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

import org.apache.spark.unsafe.Platform;
import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.unsafe.memory.LongArrayMemoryBlock;

public class MemoryBlockSuite {

  private void check(MemoryBlock memory, Object obj, long offset, int length) {
    memory.setPageNumber(1);
    memory.fill((byte)-1);
    MemoryBlock.putBoolean(memory, offset, true);
    MemoryBlock.putByte(memory, offset + 1, (byte)127);
    MemoryBlock.putShort(memory, offset + 2, (short)257);
    MemoryBlock.putInt(memory, offset + 4, 0x20000002);
    MemoryBlock.putLong(memory, offset + 8, -1L);
    MemoryBlock.putFloat(memory, offset + 16, 1.0F);
    MemoryBlock.putDouble(memory, offset + 20, 2.0);
    MemoryBlock.copyMemory(memory, offset, memory, offset + 28, 4);

    Assert.assertEquals(obj, memory.getBaseObject());
    Assert.assertEquals(offset, memory.getBaseOffset());
    Assert.assertEquals(length, memory.size());
    Assert.assertEquals(1, memory.getPageNumber());
    Assert.assertEquals(true, MemoryBlock.getBoolean(memory, offset));
    Assert.assertEquals((byte)127, MemoryBlock.getByte(memory, offset + 1 ));
    Assert.assertEquals((short)257, MemoryBlock.getShort(memory, offset + 2));
    Assert.assertEquals(0x20000002, MemoryBlock.getInt(memory, offset + 4));
    Assert.assertEquals(-1L, MemoryBlock.getLong(memory, offset + 8));
    Assert.assertEquals(1.0F, MemoryBlock.getFloat(memory, offset + 16), 0);
    Assert.assertEquals(2.0, MemoryBlock.getDouble(memory, offset + 20), 0);
    Assert.assertEquals(true, MemoryBlock.getBoolean(memory, offset + 28));
    Assert.assertEquals((byte)127, MemoryBlock.getByte(memory, offset + 29 ));
    Assert.assertEquals((short)257, MemoryBlock.getShort(memory, offset + 30));
    for (int i = 32; i < memory.size(); i++) {
      Assert.assertEquals((byte) -1, MemoryBlock.getByte(memory, offset + i));
    }
  }

  @Test
  public void ByteArrayMemoryBlockTest() {
    byte[] obj = new byte[36];
    long offset = Platform.BYTE_ARRAY_OFFSET;
    int length = obj.length;
    MemoryBlock memory = new ByteArrayMemoryBlock(obj, offset, length);

    check(memory, obj, offset, length);
  }

  @Test
  public void IntArrayMemoryBlockTest() {
    int[] obj = new int[9];
    long offset = Platform.INT_ARRAY_OFFSET;
    int length = obj.length;
    MemoryBlock memory = new IntArrayMemoryBlock(obj, offset, length);

    check(memory, obj, offset, length);
  }

  @Test
  public void LongArrayMemoryBlockTest() {
    long[] obj = new long[5];
    long offset = Platform.LONG_ARRAY_OFFSET;
    int length = obj.length;
    MemoryBlock memory = new LongArrayMemoryBlock(obj, offset, length);

    check(memory, obj, offset, length);
  }

  @Test
  public void OffHeapArrayMemoryBlockTest() {
    MemoryAllocator memoryAllocator = new UnsafeMemoryAllocator();
    MemoryBlock memory = memoryAllocator.allocate(36);
    Object obj = memory.getBaseObject();
    long offset = memory.getBaseOffset();
    int length = 36;

    check(memory, obj, offset, length);
  }
}

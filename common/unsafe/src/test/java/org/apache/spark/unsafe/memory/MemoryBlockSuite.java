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

import java.nio.ByteOrder;

public class MemoryBlockSuite {
  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private void check(MemoryBlock memory, Object obj, long offset, int length) {
    memory.setPageNumber(1);
    memory.fill((byte)-1);
    memory.putBoolean(offset, true);
    memory.putByte(offset + 1, (byte)127);
    memory.putShort(offset + 2, (short)257);
    memory.putInt(offset + 4, 0x20000002);
    memory.putLong(offset + 8, -1L);
    memory.putFloat(offset + 16, 1.0F);
    memory.putDouble(offset + 20, 2.0);
    MemoryBlock.copyMemory(memory, 0L, memory, 28, 4);
    int[] a = new int[2];
    a[0] = 0x12345678;
    a[1] = 0x13579BDF;
    memory.copyFrom(a, Platform.INT_ARRAY_OFFSET, offset + 32, 8);
    byte[] b = new byte[8];
    memory.writeTo(offset + 32, b, Platform.BYTE_ARRAY_OFFSET, 8);

    Assert.assertEquals(obj, memory.getBaseObject());
    Assert.assertEquals(offset, memory.getBaseOffset());
    Assert.assertEquals(length, memory.size());
    Assert.assertEquals(1, memory.getPageNumber());
    Assert.assertEquals(true, memory.getBoolean(offset));
    Assert.assertEquals((byte)127, memory.getByte(offset + 1 ));
    Assert.assertEquals((short)257, memory.getShort(offset + 2));
    Assert.assertEquals(0x20000002, memory.getInt(offset + 4));
    Assert.assertEquals(-1L, memory.getLong(offset + 8));
    Assert.assertEquals(1.0F, memory.getFloat(offset + 16), 0);
    Assert.assertEquals(2.0, memory.getDouble(offset + 20), 0);
    Assert.assertEquals(true, memory.getBoolean(offset + 28));
    Assert.assertEquals((byte)127, memory.getByte(offset + 29 ));
    Assert.assertEquals((short)257, memory.getShort(offset + 30));
    Assert.assertEquals(a[0], memory.getInt(offset + 32));
    Assert.assertEquals(a[1], memory.getInt(offset + 36));
    if (bigEndianPlatform) {
      Assert.assertEquals(a[0],
        ((int)b[0] & 0xff) << 24 | ((int)b[1] & 0xff) << 16 |
        ((int)b[2] & 0xff) << 8 | ((int)b[3] & 0xff));
      Assert.assertEquals(a[1],
        ((int)b[4] & 0xff) << 24 | ((int)b[5] & 0xff) << 16 |
        ((int)b[6] & 0xff) << 8 | ((int)b[7] & 0xff));
    } else {
      Assert.assertEquals(a[0],
        ((int)b[3] & 0xff) << 24 | ((int)b[2] & 0xff) << 16 |
        ((int)b[1] & 0xff) << 8 | ((int)b[0] & 0xff));
      Assert.assertEquals(a[1],
        ((int)b[7] & 0xff) << 24 | ((int)b[6] & 0xff) << 16 |
        ((int)b[5] & 0xff) << 8 | ((int)b[4] & 0xff));
    }
    for (int i = 40; i < memory.size(); i++) {
      Assert.assertEquals((byte) -1, memory.getByte(offset + i));
    }
  }

  @Test
  public void ByteArrayMemoryBlockTest() {
    byte[] obj = new byte[48];
    long offset = Platform.BYTE_ARRAY_OFFSET;
    int length = obj.length;

    MemoryBlock memory = new ByteArrayMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);

    memory = ByteArrayMemoryBlock.fromArray(obj);
    check(memory, obj, offset, length);
  }

  @Test
  public void OnHeapMemoryBlockTest() {
    long[] obj = new long[6];
    long offset = Platform.LONG_ARRAY_OFFSET;
    int length = obj.length * 8;

    MemoryBlock memory = new OnHeapMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);

    memory = OnHeapMemoryBlock.fromArray(obj);
    check(memory, obj, offset, length);
  }

  @Test
  public void OffHeapArrayMemoryBlockTest() {
    MemoryAllocator memoryAllocator = new UnsafeMemoryAllocator();
    MemoryBlock memory = memoryAllocator.allocate(48);
    Object obj = memory.getBaseObject();
    long offset = memory.getBaseOffset();
    int length = 48;

    check(memory, obj, offset, length);
  }
}

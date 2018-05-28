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

import static org.hamcrest.core.StringContains.containsString;

public class MemoryBlockSuite {
  private static final boolean bigEndianPlatform =
    ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private void check(MemoryBlock memory, Object obj, long offset, int length) {
    memory.setPageNumber(1);
    memory.fill((byte)-1);
    memory.putBoolean(0, true);
    memory.putByte(1, (byte)127);
    memory.putShort(2, (short)257);
    memory.putInt(4, 0x20000002);
    memory.putLong(8, 0x1234567089ABCDEFL);
    memory.putFloat(16, 1.0F);
    memory.putLong(20, 0x1234567089ABCDEFL);
    memory.putDouble(28, 2.0);
    MemoryBlock.copyMemory(memory, 0L, memory, 36, 4);
    int[] a = new int[2];
    a[0] = 0x12345678;
    a[1] = 0x13579BDF;
    memory.copyFrom(a, Platform.INT_ARRAY_OFFSET, 40, 8);
    byte[] b = new byte[8];
    memory.writeTo(40, b, Platform.BYTE_ARRAY_OFFSET, 8);

    Assert.assertEquals(obj, memory.getBaseObject());
    Assert.assertEquals(offset, memory.getBaseOffset());
    Assert.assertEquals(length, memory.size());
    Assert.assertEquals(1, memory.getPageNumber());
    Assert.assertEquals(true, memory.getBoolean(0));
    Assert.assertEquals((byte)127, memory.getByte(1 ));
    Assert.assertEquals((short)257, memory.getShort(2));
    Assert.assertEquals(0x20000002, memory.getInt(4));
    Assert.assertEquals(0x1234567089ABCDEFL, memory.getLong(8));
    Assert.assertEquals(1.0F, memory.getFloat(16), 0);
    Assert.assertEquals(0x1234567089ABCDEFL, memory.getLong(20));
    Assert.assertEquals(2.0, memory.getDouble(28), 0);
    Assert.assertEquals(true, memory.getBoolean(36));
    Assert.assertEquals((byte)127, memory.getByte(37 ));
    Assert.assertEquals((short)257, memory.getShort(38));
    Assert.assertEquals(a[0], memory.getInt(40));
    Assert.assertEquals(a[1], memory.getInt(44));
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
    for (int i = 48; i < memory.size(); i++) {
      Assert.assertEquals((byte) -1, memory.getByte(i));
    }

    assert(memory.subBlock(0, memory.size()) == memory);

    try {
      memory.subBlock(-8, 8);
      Assert.fail();
    } catch (Exception expected) {
      Assert.assertThat(expected.getMessage(), containsString("non-negative"));
    }

    try {
      memory.subBlock(0, -8);
      Assert.fail();
    } catch (Exception expected) {
      Assert.assertThat(expected.getMessage(), containsString("non-negative"));
    }

    try {
      memory.subBlock(0, length + 8);
      Assert.fail();
    } catch (Exception expected) {
      Assert.assertThat(expected.getMessage(), containsString("should not be larger than"));
    }

    try {
      memory.subBlock(8, length - 4);
      Assert.fail();
    } catch (Exception expected) {
      Assert.assertThat(expected.getMessage(), containsString("should not be larger than"));
    }

    try {
      memory.subBlock(length + 8, 4);
      Assert.fail();
    } catch (Exception expected) {
      Assert.assertThat(expected.getMessage(), containsString("should not be larger than"));
    }

    memory.setPageNumber(MemoryBlock.NO_PAGE_NUMBER);
  }

  @Test
  public void testByteArrayMemoryBlock() {
    byte[] obj = new byte[56];
    long offset = Platform.BYTE_ARRAY_OFFSET;
    int length = obj.length;

    MemoryBlock memory = new ByteArrayMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);

    memory = ByteArrayMemoryBlock.fromArray(obj);
    check(memory, obj, offset, length);

    obj = new byte[112];
    memory = new ByteArrayMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);
  }

  @Test
  public void testOnHeapMemoryBlock() {
    long[] obj = new long[7];
    long offset = Platform.LONG_ARRAY_OFFSET;
    int length = obj.length * 8;

    MemoryBlock memory = new OnHeapMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);

    memory = OnHeapMemoryBlock.fromArray(obj);
    check(memory, obj, offset, length);

    obj = new long[14];
    memory = new OnHeapMemoryBlock(obj, offset, length);
    check(memory, obj, offset, length);
  }

  @Test
  public void testOffHeapArrayMemoryBlock() {
    MemoryAllocator memoryAllocator = new UnsafeMemoryAllocator();
    MemoryBlock memory = memoryAllocator.allocate(56);
    Object obj = memory.getBaseObject();
    long offset = memory.getBaseOffset();
    int length = 56;

    check(memory, obj, offset, length);
    memoryAllocator.free(memory);

    long address = Platform.allocateMemory(112);
    memory = new OffHeapMemoryBlock(address, length);
    obj = memory.getBaseObject();
    offset = memory.getBaseOffset();
    check(memory, obj, offset, length);
    Platform.freeMemory(address);
  }
}

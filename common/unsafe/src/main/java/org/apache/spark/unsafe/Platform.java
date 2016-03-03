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

import java.lang.reflect.Field;

import org.apache.spark.unsafe.memory.MemoryBlock;
import sun.misc.Unsafe;

public final class Platform {

  private static final Unsafe _UNSAFE;

  public static final int BYTE_ARRAY_OFFSET;

  public static final int SHORT_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int FLOAT_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  public static int getInt(MemoryBlock object, long offset) {
    return _UNSAFE.getInt(object.getBaseObject(), offset);
  }

  public static int getInt(byte[] object, long offset) {
    return _UNSAFE.getInt(object, offset);
  }

  public static void putInt(MemoryBlock object, long offset, int value) {
    _UNSAFE.putInt(object.getBaseObject(), offset, value);
  }

  public static void putInt(byte[] object, long offset, int value) {
    _UNSAFE.putInt(object, offset, value);
  }

  public static boolean getBoolean(MemoryBlock object, long offset) {
    return _UNSAFE.getBoolean(object.getBaseObject(), offset);
  }

  public static void putBoolean(MemoryBlock object, long offset, boolean value) {
    _UNSAFE.putBoolean(object.getBaseObject(), offset, value);
  }

  public static byte getByte(MemoryBlock object, long offset) {
    return _UNSAFE.getByte(object.getBaseObject(), offset);
  }

  public static byte getByte(byte[] object, long offset) {
    return _UNSAFE.getByte(object, offset);
  }

  public static void putByte(MemoryBlock object, long offset, byte value) {
    _UNSAFE.putByte(object.getBaseObject(), offset, value);
  }

  public static short getShort(MemoryBlock object, long offset) {
    return _UNSAFE.getShort(object.getBaseObject(), offset);
  }

  public static void putShort(MemoryBlock object, long offset, short value) {
    _UNSAFE.putShort(object.getBaseObject(), offset, value);
  }

  public static long getLong(MemoryBlock object, long offset) {
    return _UNSAFE.getLong(object.getBaseObject(), offset);
  }

  public static long getLong(byte[] object, long offset) {
    return _UNSAFE.getLong(object, offset);
  }

  public static void putLong(MemoryBlock object, long offset, long value) {
    _UNSAFE.putLong(object.getBaseObject(), offset, value);
  }

  public static float getFloat(MemoryBlock object, long offset) {
    return _UNSAFE.getFloat(object.getBaseObject(), offset);
  }

  public static float getFloat(byte[] object, long offset) {
    return _UNSAFE.getFloat(object, offset);
  }

  public static void putFloat(MemoryBlock object, long offset, float value) {
    _UNSAFE.putFloat(object.getBaseObject(), offset, value);
  }

  public static double getDouble(MemoryBlock object, long offset) {
    return _UNSAFE.getDouble(object.getBaseObject(), offset);
  }

  public static double getDouble(byte[] object, long offset) {
    return _UNSAFE.getDouble(object, offset);
  }

  public static void putDouble(MemoryBlock object, long offset, double value) {
    _UNSAFE.putDouble(object.getBaseObject(), offset, value);
  }

  public static Object getObjectVolatile(MemoryBlock object, long offset) {
    return _UNSAFE.getObjectVolatile(object.getBaseObject(), offset);
  }

  public static void putObjectVolatile(MemoryBlock object, long offset, Object value) {
    _UNSAFE.putObjectVolatile(object.getBaseObject(), offset, value);
  }

  public static long allocateMemory(long size) {
    return _UNSAFE.allocateMemory(size);
  }

  public static void freeMemory(long address) {
    _UNSAFE.freeMemory(address);
  }

  public static long reallocateMemory(long address, long oldSize, long newSize) {
    long newMemory = _UNSAFE.allocateMemory(newSize);
    copyMemory0(null, address, null, newMemory, oldSize);
    freeMemory(address);
    return newMemory;
  }

  public static void setMemory(long address, byte value, long size) {
    _UNSAFE.setMemory(address, size, value);
  }

  static void copyMemory0(
          Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }

    }
  }

  public static void copyMemory(
          MemoryBlock src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src.getBaseObject(), srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          short[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          int[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          long[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          float[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          double[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
          MemoryBlock src, long srcOffset, byte[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, byte[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, short[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, int[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, long[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, float[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
          byte[] src, long srcOffset, double[] dst, long dstOffset, long length) {
    Platform.copyMemory0(src, srcOffset, dst, dstOffset, length);
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;

    if (_UNSAFE != null) {
      BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
      SHORT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(short[].class);
      INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
      FLOAT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(float[].class);
      DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
    } else {
      BYTE_ARRAY_OFFSET = 0;
      SHORT_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      FLOAT_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }
}

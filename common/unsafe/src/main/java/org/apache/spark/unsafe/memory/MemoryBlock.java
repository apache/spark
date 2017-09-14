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

import javax.annotation.Nullable;

/**
 * A declaration of interfaces of MemoryBlock classes .
 */
public abstract class MemoryBlock {
  /** Special `pageNumber` value for pages which were not allocated by TaskMemoryManagers */
  public static final int NO_PAGE_NUMBER = -1;

  /**
   * Special `pageNumber` value for marking pages that have been freed in the TaskMemoryManager.
   * We set `pageNumber` to this value in TaskMemoryManager.freePage() so that MemoryAllocator
   * can detect if pages which were allocated by TaskMemoryManager have been freed in the TMM
   * before being passed to MemoryAllocator.free() (it is an error to allocate a page in
   * TaskMemoryManager and then directly free it in a MemoryAllocator without going through
   * the TMM freePage() call).
   */
  public static final int FREED_IN_TMM_PAGE_NUMBER = -2;

  /**
   * Special `pageNumber` value for pages that have been freed by the MemoryAllocator. This allows
   * us to detect double-frees.
   */
  public static final int FREED_IN_ALLOCATOR_PAGE_NUMBER = -3;

  @Nullable
  protected Object obj;

  protected long offset;

  public MemoryBlock(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  public MemoryBlock() {
    this(null, 0);
  }

  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }

  public final void resetObjAndOffset() {
    this.obj = null;
    this.offset = 0;
  }

  /**
   * Returns the size of the memory block.
   */
  public abstract long size();

  public abstract void setPageNumber(int pageNum);

  public abstract int getPageNumber();

  /**
   * Fills the memory block with the specified byte value.
   */
  public abstract void fill(byte value);

  /**
   * Instantiate the same type of MemoryBlock with new offset and size
   */
  public abstract MemoryBlock allocate(long offset, long size);


  public static final int getInt(MemoryBlock object, long offset) {
    return Platform.getInt(object.getBaseObject(), offset);
  }

  public static final void putInt(MemoryBlock object, long offset, int value) {
    Platform.putInt(object.getBaseObject(), offset, value);
  }


  public static final boolean getBoolean(MemoryBlock object, long offset) {
    return Platform.getBoolean(object.getBaseObject(), offset);
  }

  public static final void putBoolean(MemoryBlock object, long offset, boolean value) {
    Platform.putBoolean(object.getBaseObject(), offset, value);
  }

  public static final byte getByte(MemoryBlock object, long offset) {
    return Platform.getByte(object.getBaseObject(), offset);
  }

  public static final void putByte(MemoryBlock object, long offset, byte value) {
    Platform.putByte(object.getBaseObject(), offset, value);
  }

  public static final short getShort(MemoryBlock object, long offset) {
    return Platform.getShort(object.getBaseObject(), offset);
  }

  public static final void putShort(MemoryBlock object, long offset, short value) {
    Platform.putShort(object.getBaseObject(), offset, value);
  }

  public static final long getLong(MemoryBlock object, long offset) {
    return Platform.getLong(object.getBaseObject(), offset);
  }

  public static final void putLong(MemoryBlock object, long offset, long value) {
    Platform.putLong(object.getBaseObject(), offset, value);
  }

  public static final float getFloat(MemoryBlock object, long offset) {
    return Platform.getFloat(object.getBaseObject(), offset);
  }

  public static final void putFloat(MemoryBlock object, long offset, float value) {
    Platform.putFloat(object.getBaseObject(), offset, value);
  }

  public static final double getDouble(MemoryBlock object, long offset) {
    return Platform.getDouble(object.getBaseObject(), offset);
  }

  public static final void putDouble(MemoryBlock object, long offset, double value) {
    Platform.putDouble(object.getBaseObject(), offset, value);
  }

  public static final Object getObjectVolatile(MemoryBlock object, long offset) {
    return Platform.getObjectVolatile(object.getBaseObject(), offset);
  }

  public static final void putObjectVolatile(MemoryBlock object, long offset, Object value) {
    Platform.putObjectVolatile(object.getBaseObject(), offset, value);
  }

  public static final void copyMemory(
      MemoryBlock src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static final void copyMemory(
      byte[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static final void copyMemory(
      short[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static final void copyMemory(
      int[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static final void copyMemory(
      long[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
      float[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
      double[] src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    Platform.copyMemory(src, srcOffset, dst.getBaseObject(), dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, byte[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, short[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, int[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, long[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, float[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }

  public static void copyMemory(
      MemoryBlock src, long srcOffset, double[] dst, long dstOffset, long length) {
    Platform.copyMemory(src.getBaseObject(), srcOffset, dst, dstOffset, length);
  }
}

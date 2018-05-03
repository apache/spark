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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A representation of a consecutive memory block in Spark. It defines the common interfaces
 * for memory accessing and mutating.
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

  protected long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field can be updated using setPageNumber method so that
   * this can be modified by the TaskMemoryManager, which lives in a different package.
   */
  private int pageNumber = NO_PAGE_NUMBER;

  protected MemoryBlock(@Nullable Object obj, long offset, long length) {
    if (offset < 0 || length < 0) {
      throw new IllegalArgumentException(
        "Length " + length + " and offset " + offset + "must be non-negative");
    }
    this.obj = obj;
    this.offset = offset;
    this.length = length;
  }

  protected MemoryBlock() {
    this(null, 0, 0);
  }

  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }

  public void resetObjAndOffset() {
    this.obj = null;
    this.offset = 0;
  }

  /**
   * Returns the size of the memory block.
   */
  public final long size() {
    return length;
  }

  public final void setPageNumber(int pageNum) {
    pageNumber = pageNum;
  }

  public final int getPageNumber() {
    return pageNumber;
  }

  /**
   * Fills the memory block with the specified byte value.
   */
  public final void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }

  /**
   * Instantiate MemoryBlock for given object type with new offset
   */
  public static final MemoryBlock allocateFromObject(Object obj, long offset, long length) {
    MemoryBlock mb = null;
    if (obj instanceof byte[]) {
      byte[] array = (byte[])obj;
      mb = new ByteArrayMemoryBlock(array, offset, length);
    } else if (obj instanceof long[]) {
      long[] array = (long[])obj;
      mb = new OnHeapMemoryBlock(array, offset, length);
    } else if (obj == null) {
      // we assume that to pass null pointer means off-heap
      mb = new OffHeapMemoryBlock(offset, length);
    } else {
      throw new UnsupportedOperationException(
        "Instantiate MemoryBlock for type " + obj.getClass() + " is not supported now");
    }
    return mb;
  }

  /**
   * Just instantiate the sub-block with the same type of MemoryBlock with the new size and relative
   * offset from the original offset. The data is not copied.
   * If parameters are invalid, an exception is thrown.
   */
  public abstract MemoryBlock subBlock(long offset, long size);

  protected void checkSubBlockRange(long offset, long size) {
    if (offset < 0 || size < 0) {
      throw new ArrayIndexOutOfBoundsException(
        "Size " + size + " and offset " + offset + " must be non-negative");
    }
    if (offset + size > length) {
      throw new ArrayIndexOutOfBoundsException("The sum of size " + size + " and offset " +
        offset + " should not be larger than the length " + length + " in the MemoryBlock");
    }
  }

  /**
   * getXXX/putXXX does not ensure guarantee behavior if the offset is invalid. e.g  cause illegal
   * memory access, throw an exception, or etc.
   * getXXX/putXXX uses an index based on this.offset that includes the size of metadata such as
   * JVM object header. The offset is 0-based and is expected as an logical offset in the memory
   * block.
   */
  public abstract int getInt(long offset);

  public abstract void putInt(long offset, int value);

  public abstract boolean getBoolean(long offset);

  public abstract void putBoolean(long offset, boolean value);

  public abstract byte getByte(long offset);

  public abstract void putByte(long offset, byte value);

  public abstract short getShort(long offset);

  public abstract void putShort(long offset, short value);

  public abstract long getLong(long offset);

  public abstract void putLong(long offset, long value);

  public abstract float getFloat(long offset);

  public abstract void putFloat(long offset, float value);

  public abstract double getDouble(long offset);

  public abstract void putDouble(long offset, double value);

  public static final void copyMemory(
      MemoryBlock src, long srcOffset, MemoryBlock dst, long dstOffset, long length) {
    assert(srcOffset + length <= src.length && dstOffset + length <= dst.length);
    Platform.copyMemory(src.getBaseObject(), src.getBaseOffset() + srcOffset,
      dst.getBaseObject(), dst.getBaseOffset() + dstOffset, length);
  }

  public static final void copyMemory(MemoryBlock src, MemoryBlock dst, long length) {
    assert(length <= src.length && length <= dst.length);
    Platform.copyMemory(src.getBaseObject(), src.getBaseOffset(),
      dst.getBaseObject(), dst.getBaseOffset(), length);
  }

  public final void copyFrom(Object src, long srcOffset, long dstOffset, long length) {
    assert(length <= this.length - srcOffset);
    Platform.copyMemory(src, srcOffset, obj, offset + dstOffset, length);
  }

  public final void writeTo(long srcOffset, Object dst, long dstOffset, long length) {
    assert(length <= this.length - srcOffset);
    Platform.copyMemory(obj, offset + srcOffset, dst, dstOffset, length);
  }
}

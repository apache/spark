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

package org.apache.spark.sql.catalyst.expressions;

import java.nio.ByteBuffer;
import javax.annotation.Nonnull;

import com.google.common.primitives.Ints;

import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.ByteArrayMemoryBlock;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * An Unsafe implementation of Map which is backed by raw memory instead of Java objects.
 *
 * Currently we just use 2 UnsafeArrayData to represent UnsafeMapData, with extra 8 bytes at head
 * to indicate the number of bytes of the unsafe key array.
 * [unsafe key array numBytes] [unsafe key array] [unsafe value array]
 */
// TODO: Use a more efficient format which doesn't depend on unsafe array.
public final class UnsafeMapData extends MapData {

  @Nonnull
  private MemoryBlock base;

  // The size of this map's backing data, in bytes.
  // The 4-bytes header of key array `numBytes` is also included, so it's actually equal to
  // 4 + key array numBytes + value array numBytes.
  private int sizeInBytes;

  public Object getBaseObject() { return base.getBaseObject(); }
  public long getBaseOffset() { return base.getBaseOffset(); }
  public int getSizeInBytes() { return sizeInBytes; }

  private final UnsafeArrayData keys;
  private final UnsafeArrayData values;

  /**
   * Construct a new UnsafeMapData. The resulting UnsafeMapData won't be usable until
   * `pointTo()` has been called, since the value returned by this constructor is equivalent
   * to a null pointer.
   */
  public UnsafeMapData() {
    keys = new UnsafeArrayData();
    values = new UnsafeArrayData();
  }

  /**
   * Update this UnsafeMapData to point to different backing data.
   *
   * @param baseObject the base object
   * @param baseOffset the offset within the base object
   * @param sizeInBytes the size of this map's backing data, in bytes
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    MemoryBlock block = MemoryBlock.allocateFromObject(baseObject, baseOffset, sizeInBytes);
    pointTo(block);
  }

  public void pointTo(MemoryBlock base) {
    long baseOffset = base.getBaseOffset();
    this.base = base;
    this.sizeInBytes = Ints.checkedCast(base.size());

    // Read the numBytes of key array from the first 8 bytes.
    final long keyArraySize = base.getLong(baseOffset);
    assert keyArraySize >= 0 : "keyArraySize (" + keyArraySize + ") should >= 0";
    assert keyArraySize <= Integer.MAX_VALUE :
      "keyArraySize (" + keyArraySize + ") should <= Integer.MAX_VALUE";
    final int valueArraySize = sizeInBytes - (int)keyArraySize - 8;
    assert valueArraySize >= 0 : "valueArraySize (" + valueArraySize + ") should >= 0";

    keys.pointTo(base.subBlock(8, keyArraySize));
    values.pointTo(base.subBlock(8 + keyArraySize, valueArraySize));

    assert keys.numElements() == values.numElements();
  }

  @Override
  public int numElements() {
    return keys.numElements();
  }

  @Override
  public UnsafeArrayData keyArray() {
    return keys;
  }

  @Override
  public UnsafeArrayData valueArray() {
    return values;
  }

  public void writeToMemory(Object target, long targetOffset) {
    base.writeTo(0, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  @Override
  public UnsafeMapData copy() {
    UnsafeMapData mapCopy = new UnsafeMapData();
    MemoryBlock mb = ByteArrayMemoryBlock.fromArray(new byte[sizeInBytes]);
    MemoryBlock.copyMemory(base, mb, sizeInBytes);
    mapCopy.pointTo(mb);
    return mapCopy;
  }
}

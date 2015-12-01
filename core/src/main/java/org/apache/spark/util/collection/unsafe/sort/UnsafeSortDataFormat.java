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

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.SortDataFormat;

/**
 * Supports sorting an array of (record pointer, key prefix) pairs.
 * Used in {@link UnsafeInMemorySorter}.
 * <p>
 * Within each long[] buffer, position {@code 2 * i} holds a pointer pointer to the record at
 * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
 */
final class UnsafeSortDataFormat extends SortDataFormat<RecordPointerAndKeyPrefix, LongArray> {

  public static final UnsafeSortDataFormat INSTANCE = new UnsafeSortDataFormat();

  private UnsafeSortDataFormat() { }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordPointerAndKeyPrefix newKey() {
    return new RecordPointerAndKeyPrefix();
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos, RecordPointerAndKeyPrefix reuse) {
    reuse.recordPointer = data.get(pos * 2);
    reuse.keyPrefix = data.get(pos * 2 + 1);
    return reuse;
  }

  @Override
  public void swap(LongArray data, int pos0, int pos1) {
    long tempPointer = data.get(pos0 * 2);
    long tempKeyPrefix = data.get(pos0 * 2 + 1);
    data.set(pos0 * 2, data.get(pos1 * 2));
    data.set(pos0 * 2 + 1, data.get(pos1 * 2 + 1));
    data.set(pos1 * 2, tempPointer);
    data.set(pos1 * 2 + 1, tempKeyPrefix);
  }

  @Override
  public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
    dst.set(dstPos * 2, src.get(srcPos * 2));
    dst.set(dstPos * 2 + 1, src.get(srcPos * 2 + 1));
  }

  @Override
  public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
    Platform.copyMemory(
      src.getBaseObject(),
      src.getBaseOffset() + srcPos * 16,
      dst.getBaseObject(),
      dst.getBaseOffset() + dstPos * 16,
      length * 16);
  }

  @Override
  public LongArray allocate(int length) {
    assert (length < Integer.MAX_VALUE / 2) : "Length " + length + " is too large";
    // This is used as temporary buffer, it's fine to allocate from JVM heap.
    return new LongArray(MemoryBlock.fromLongArray(new long[length * 2]));
  }

}

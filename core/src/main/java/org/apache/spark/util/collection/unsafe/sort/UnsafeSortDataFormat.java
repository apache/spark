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

import org.apache.spark.util.collection.SortDataFormat;

/**
 * Supports sorting an array of (record pointer, key prefix) pairs.
 * Used in {@link UnsafeInMemorySorter}.
 * <p>
 * Within each long[] buffer, position {@code 2 * i} holds a pointer pointer to the record at
 * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
 */
final class UnsafeSortDataFormat extends SortDataFormat<RecordPointerAndKeyPrefix, long[]> {

  public static final UnsafeSortDataFormat INSTANCE = new UnsafeSortDataFormat();

  private UnsafeSortDataFormat() { }

  @Override
  public RecordPointerAndKeyPrefix getKey(long[] data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordPointerAndKeyPrefix newKey() {
    return new RecordPointerAndKeyPrefix();
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(long[] data, int pos, RecordPointerAndKeyPrefix reuse) {
    reuse.recordPointer = data[pos * 2];
    reuse.keyPrefix = data[pos * 2 + 1];
    return reuse;
  }

  @Override
  public void swap(long[] data, int pos0, int pos1) {
    long tempPointer = data[pos0 * 2];
    long tempKeyPrefix = data[pos0 * 2 + 1];
    data[pos0 * 2] = data[pos1 * 2];
    data[pos0 * 2 + 1] = data[pos1 * 2 + 1];
    data[pos1 * 2] = tempPointer;
    data[pos1 * 2 + 1] = tempKeyPrefix;
  }

  @Override
  public void copyElement(long[] src, int srcPos, long[] dst, int dstPos) {
    dst[dstPos * 2] = src[srcPos * 2];
    dst[dstPos * 2 + 1] = src[srcPos * 2 + 1];
  }

  @Override
  public void copyRange(long[] src, int srcPos, long[] dst, int dstPos, int length) {
    System.arraycopy(src, srcPos * 2, dst, dstPos * 2, length * 2);
  }

  @Override
  public long[] allocate(int length) {
    assert (length < Integer.MAX_VALUE / 2) : "Length " + length + " is too large";
    return new long[length * 2];
  }

}

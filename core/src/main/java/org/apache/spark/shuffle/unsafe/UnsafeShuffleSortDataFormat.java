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

package org.apache.spark.shuffle.unsafe;

import org.apache.spark.util.collection.SortDataFormat;

final class UnsafeShuffleSortDataFormat extends SortDataFormat<PackedRecordPointer, long[]> {

  public static final UnsafeShuffleSortDataFormat INSTANCE = new UnsafeShuffleSortDataFormat();

  private UnsafeShuffleSortDataFormat() { }

  @Override
  public PackedRecordPointer getKey(long[] data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public PackedRecordPointer newKey() {
    return new PackedRecordPointer();
  }

  @Override
  public PackedRecordPointer getKey(long[] data, int pos, PackedRecordPointer reuse) {
    reuse.set(data[pos]);
    return reuse;
  }

  @Override
  public void swap(long[] data, int pos0, int pos1) {
    final long temp = data[pos0];
    data[pos0] = data[pos1];
    data[pos1] = temp;
  }

  @Override
  public void copyElement(long[] src, int srcPos, long[] dst, int dstPos) {
    dst[dstPos] = src[srcPos];
  }

  @Override
  public void copyRange(long[] src, int srcPos, long[] dst, int dstPos, int length) {
    System.arraycopy(src, srcPos, dst, dstPos, length);
  }

  @Override
  public long[] allocate(int length) {
    return new long[length];
  }

}

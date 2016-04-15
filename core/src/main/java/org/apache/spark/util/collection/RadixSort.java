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

package org.apache.spark.util.collection;
 
import org.apache.spark.unsafe.array.LongArray;

public class RadixSort {
  public static boolean enabled = true;

  public static int sort(
      LongArray array,
      int dataLen,
      int dataOffset,
      int tmpOffset,
      int startByteIdx,
      int endByteIdx) {
    assert enabled : "Radix sort is disabled.";
    assert startByteIdx >= 0 : "startByteIdx (" + startByteIdx + ") should >= 0";
    assert endByteIdx <= 7 : "endByteIdx (" + endByteIdx + ") should <= 7";
    assert startByteIdx <= endByteIdx;
    assert dataOffset + dataLen <= array.size();
    assert tmpOffset + dataLen <= array.size();
    if (dataLen > 0) {
      // Optimization: make a pre-pass to determine which byte indices we can skip for sorting.
      // If all the byte values at a particular index are the same we don't need to sort it.
      long orMask = 0;
      long andMask = ~0L;
      for (int i = dataOffset; i < dataOffset + dataLen; i++) {
        long value = array.get(i);
        orMask |= value;
        andMask &= value;
      }
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        long bitMin = ((orMask >>> (i * 8)) & 0xff);
        long bitMax = ((andMask >>> (i * 8)) & 0xff);
        if (bitMin != bitMax) {
          sortAtByte(array, i, dataLen, dataOffset, tmpOffset);
          int tmp = dataOffset;
          dataOffset = tmpOffset;
          tmpOffset = tmp;
        }
      }
    }
    return dataOffset;
  }

  private static void sortAtByte(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset) {
    int[] tmpOffsets = getOffsets(array, byteIdx, dataLen, dataOffset, tmpOffset);
    for (int i = dataOffset; i < dataOffset + dataLen; i++) {
      long value = array.get(i);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      array.set(tmpOffsets[bucket]++, value);
    }
  }

  // TODO(ekl) it might be worth pre-computing these up-front for all bytes
  private static int[] getOffsets(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset) {
    int[] tmpOffsets = new int[256];
    for (int i = 0; i < dataLen; i++) {
      tmpOffsets[(int)((array.get(dataOffset + i) >>> (byteIdx * 8)) & 0xff)]++;
    }
    int accum = 0;
    for (int i = 0; i < 256; i++) {
      int tmp = tmpOffsets[i];
      tmpOffsets[i] = tmpOffset + accum;
      accum += tmp;
    }
    return tmpOffsets;
  }

  public static int sortKeyPrefixArray(
      LongArray array,
      int dataLen,
      int dataOffset,
      int tmpOffset,
      int startByteIdx,
      int endByteIdx) {
    assert startByteIdx >= 0 : "startByteIdx (" + startByteIdx + ") should >= 0";
    assert endByteIdx <= 7 : "endByteIdx (" + endByteIdx + ") should <= 7";
    assert startByteIdx <= endByteIdx;
    assert dataOffset + dataLen * 2 <= array.size();
    assert tmpOffset + dataLen * 2 <= array.size();
    if (dataLen > 0) {
      // Optimization: make a pre-pass to determine which byte indices we can skip for sorting.
      // If all the byte values at a particular index are the same we don't need to sort it.
      long orMask = 0;
      long andMask = ~0L;
      for (int i = dataOffset; i < dataOffset + dataLen * 2; i += 2) {
        long value = array.get(i + 1);
        orMask |= value;
        andMask &= value;
      }
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        long bitMin = ((orMask >>> (i * 8)) & 0xff);
        long bitMax = ((andMask >>> (i * 8)) & 0xff);
        if (bitMin != bitMax) {
          sortKeyPrefixArrayAtByte(array, i, dataLen, dataOffset, tmpOffset);
          int tmp = dataOffset;
          dataOffset = tmpOffset;
          tmpOffset = tmp;
        }
      }
    }
    return dataOffset;
  }

  private static void sortKeyPrefixArrayAtByte(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset) {
    int[] tmpOffsets = getKeyPrefixArrayOffsets(array, byteIdx, dataLen, dataOffset, tmpOffset);
    for (int i = dataOffset; i < dataOffset + dataLen * 2; i += 2) {
      long key = array.get(i);
      long prefix = array.get(i + 1);
      int bucket = (int)((prefix >>> (byteIdx * 8)) & 0xff);
      int offset = tmpOffsets[bucket];
      tmpOffsets[bucket] += 2;
      array.set(offset, key);
      array.set(offset + 1, prefix);
    }
  }

  // TODO(ekl) it might be worth pre-computing these up-front for all bytes
  private static int[] getKeyPrefixArrayOffsets(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset) {
    int[] tmpOffsets = new int[256];
    for (int i = 0; i < dataLen * 2; i += 2) {
      tmpOffsets[(int)((array.get(dataOffset + i + 1) >>> (byteIdx * 8)) & 0xff)] += 2;
    }
    int accum = 0;
    for (int i = 0; i < 256; i++) {
      int tmp = tmpOffsets[i];
      tmpOffsets[i] = tmpOffset + accum;
      accum += tmp;
    }
    return tmpOffsets;
  }
}

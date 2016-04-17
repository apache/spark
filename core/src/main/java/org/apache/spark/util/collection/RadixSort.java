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
      LongArray array, int dataLen, int startByteIdx, int endByteIdx,
      boolean desc, boolean signed) {
    assert startByteIdx >= 0 : "startByteIdx (" + startByteIdx + ") should >= 0";
    assert endByteIdx <= 7 : "endByteIdx (" + endByteIdx + ") should <= 7";
    assert startByteIdx <= endByteIdx;
    assert dataLen * 2 <= array.size();
    int dataOffset = 0;
    int tmpOffset = dataLen;
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
      long bitsChanged = andMask ^ orMask;
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
          sortAtByte(array, i, dataLen, dataOffset, tmpOffset, desc, signed && i == endByteIdx);
          int tmp = dataOffset;
          dataOffset = tmpOffset;
          tmpOffset = tmp;
        }
      }
    }
    return dataOffset;
  }

  private static void sortAtByte(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset,
      boolean desc, boolean signed) {
    int[] offsets = getCounts(array, byteIdx, dataLen, dataOffset);
    transformCountsToOffsets(offsets, dataLen, tmpOffset, desc, signed);
    for (int i = dataOffset; i < dataOffset + dataLen; i++) {
      long value = array.get(i);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      array.set(offsets[bucket]++, value);
    }
  }

  // TODO(ekl) it might be worth pre-computing these up-front for all bytes.
  private static int[] getCounts(LongArray array, int byteIdx, int dataLen, int dataOffset) {
    int[] counts = new int[256];
    for (int i = 0; i < dataLen; i++) {
      counts[(int)((array.get(dataOffset + i) >>> (byteIdx * 8)) & 0xff)]++;
    }
    return counts;
  }
  
  private static void transformCountsToOffsets(
      int[] counts, int dataLen, int tmpOffset, boolean desc, boolean signed) {
    int start = signed ? 128 : 0;
    if (desc) {
      int pos = dataLen;
      for (int i = start; i < start + 256; i++) {
        pos -= counts[i & 0xff];
        counts[i & 0xff] = tmpOffset + pos;
      }
    } else {
      int pos = 0;
      for (int i = start; i < start + 256; i++) {
        int tmp = counts[i & 0xff];
        counts[i & 0xff] = tmpOffset + pos;
        pos += tmp;
      }
    }
  }

  //
  // Specialization of the sort for key-prefix arrays.
  //

  public static int sortKeyPrefixArray(
      LongArray array,
      int dataLen,
      int dataOffset,
      int tmpOffset,
      int startByteIdx,
      int endByteIdx,
      boolean desc,
      boolean signed) {
    assert startByteIdx >= 0 : "startByteIdx (" + startByteIdx + ") should >= 0";
    assert endByteIdx <= 7 : "endByteIdx (" + endByteIdx + ") should <= 7";
    assert startByteIdx <= endByteIdx;
    assert dataLen * 4 <= array.size();
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
      long bitsChanged = andMask ^ orMask;
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
          sortKeyPrefixArrayAtByte(
            array, i, dataLen, dataOffset, tmpOffset, desc, signed && i == endByteIdx);
          int tmp = dataOffset;
          dataOffset = tmpOffset;
          tmpOffset = tmp;
        }
      }
    }
    return dataOffset;
  }

  private static void sortKeyPrefixArrayAtByte(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset,
      boolean desc, boolean signed) {
    int[] offsets = getKeyPrefixArrayCounts(array, byteIdx, dataLen, dataOffset);
    transformCountsToOffsets(offsets, dataLen * 2, tmpOffset, desc, signed);
    for (int i = dataOffset; i < dataOffset + dataLen * 2; i += 2) {
      long key = array.get(i);
      long prefix = array.get(i + 1);
      int bucket = (int)((prefix >>> (byteIdx * 8)) & 0xff);
      int offset = offsets[bucket];
      offsets[bucket] += 2;
      array.set(offset, key);
      array.set(offset + 1, prefix);
    }
  }

  private static int[] getKeyPrefixArrayCounts(
      LongArray array, int byteIdx, int dataLen, int dataOffset) {
    int[] counts = new int[256];
    for (int i = 0; i < dataLen * 2; i += 2) {
      counts[(int)((array.get(dataOffset + i + 1) >>> (byteIdx * 8)) & 0xff)] += 2;
    }
    return counts;
  }
}

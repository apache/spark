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
      // We only need to make passes if some of the bits change at that byte index.
      long bitsChanged = orMask ^ andMask;
      int numPasses = 0;
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
          numPasses += 1;
        }
      }
      // Precompute the bucket offsets for each pass.
      int[] offsets = new int[numPasses * 256];
      int ix = 0;
      for (ix = 0; ix < dataLen - 200000; ix += 200000) {
        int offsetsBase = 0;
        for (int j = startByteIdx; j <= endByteIdx; j++) {
          if (((bitsChanged >>> (j * 8)) & 0xff) != 0) {
            for (int k = ix; k < ix + 200000; k++) {
              long value = array.get(dataOffset + k);
              offsets[offsetsBase + (int)((value >>> (j * 8)) & 0xff)]++;
            }
            offsetsBase += 256;
          }
        }
      }
      for (int i = ix; i < dataLen; i++) {
        long value = array.get(dataOffset + i);
        int offsetsBase = 0;
        for (int j = startByteIdx; j <= endByteIdx; j++) {
          if (((bitsChanged >>> (j * 8)) & 0xff) != 0) {
            offsets[offsetsBase + (int)((value >>> (j * 8)) & 0xff)]++;
            offsetsBase += 256;
          }
        }
      }
      for (int i=0; i < numPasses; i++) {
        int accum = 0;
        for (int j = i * 256; j < (i + 1) * 256; j++) {
          int tmp = offsets[j];
          offsets[j] = accum;
          accum += tmp;
        }
      }
      // Execute each pass.
      int offsetsBase = 0;
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
          sortAtByte(array, i, dataLen, dataOffset, tmpOffset, offsets, offsetsBase);
          int tmp = dataOffset;
          dataOffset = tmpOffset;
          tmpOffset = tmp;
          offsetsBase += 256;
        }
      }
    }
    return dataOffset;
  }

  private static void sortAtByte(
      LongArray array, int byteIdx, int dataLen, int dataOffset, int tmpOffset,
      int[] offsets, int offsetsBase) {
    System.out.println("sortAtByte: " + byteIdx + " " + dataLen + " " + dataOffset + " " + tmpOffset + " " + offsetsBase);
    for (int i = dataOffset; i < dataOffset + dataLen; i++) {
      long value = array.get(i);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      array.set(tmpOffset + offsets[offsetsBase + bucket]++, value);
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
      long bitsChanged = orMask ^= andMask;
      for (int i = startByteIdx; i <= endByteIdx; i++) {
        if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
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

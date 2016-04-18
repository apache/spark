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

public class RadixSort {
  public static int sort(
      LongArray array, int numRecords, int startByteIndex, int endByteIndex,
      boolean desc, boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert startByteIndex <= endByteIndex;
    assert numRecords * 2 <= array.size();
    int dataIndex = 0;
    int tmpIndex = numRecords;
    if (numRecords > 0) {
      long[][] counts = getCounts(array, numRecords, startByteIndex, endByteIndex);
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortAtByte(
            array, counts[i], i, numRecords, dataIndex, tmpIndex,
            desc, signed && i == endByteIndex);
          int tmp = dataIndex;
          dataIndex = tmpIndex;
          tmpIndex = tmp;
        }
      }
    }
    return dataIndex;
  }

  private static void sortAtByte(
      LongArray array, long[] counts, int byteIdx, int numRecords, int dataIndex, int tmpIndex,
      boolean desc, boolean signed) {
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + tmpIndex * 8, 8, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + dataIndex * 8;
    long maxOffset = baseOffset + numRecords * 8;
    for (long offset = baseOffset; offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      Platform.putLong(baseObject, offsets[bucket], value);
      offsets[bucket] += 8;
    }
  }

  private static long[][] getCounts(
      LongArray array, int numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    // Optimization: make a pre-pass to determine which byte indices we can skip for sorting.
    // If all the byte values at a particular index are the same we don't need to sort it.
    long orMask = 0;
    long andMask = ~0L;
    long maxOffset = array.getBaseOffset() + numRecords * 8;
    Object baseObject = array.getBaseObject();
    for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      orMask |= value;
      andMask &= value;
    }
    long bitsChanged = andMask ^ orMask;
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
          counts[i][(int)((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  private static long[] transformCountsToOffsets(
      long[] counts, int numRecords, long baseOffset, int bytesPerRecord,
      boolean desc, boolean signed) {
    int start = signed ? 128 : 0;
    if (desc) {
      int pos = numRecords;
      for (int i = start; i < start + 256; i++) {
        pos -= counts[i & 0xff];
        counts[i & 0xff] = baseOffset + pos * bytesPerRecord;
      }
    } else {
      int pos = 0;
      for (int i = start; i < start + 256; i++) {
        long tmp = counts[i & 0xff];
        counts[i & 0xff] = baseOffset + pos * bytesPerRecord;
        pos += tmp;
      }
    }
    return counts;
  }

  //
  // Specialization for key-prefix arrays.
  //

  public static int sortKeyPrefixArray(
      LongArray array,
      int numRecords,
      int startByteIndex,
      int endByteIndex,
      boolean desc,
      boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert startByteIndex <= endByteIndex;
    assert numRecords * 4 <= array.size();
    int dataIndex = 0;
    int tmpIndex = numRecords * 2;
    if (numRecords > 0) {
      long[][] counts = getKeyPrefixArrayCounts(array, numRecords, startByteIndex, endByteIndex);
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortKeyPrefixArrayAtByte(
            array, counts[i], i, numRecords, dataIndex, tmpIndex,
            desc, signed && i == endByteIndex);
          int tmp = dataIndex;
          dataIndex = tmpIndex;
          tmpIndex = tmp;
        }
      }
    }
    return dataIndex;
  }

  private static long[][] getKeyPrefixArrayCounts(
      LongArray array, int numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    long orMask = 0;
    long andMask = ~0L;
    long limit = array.getBaseOffset() + numRecords * 16;
    Object baseObject = array.getBaseObject();
    for (long offset = array.getBaseOffset(); offset < limit; offset += 16) {
      long value = Platform.getLong(baseObject, offset + 8);
      orMask |= value;
      andMask &= value;
    }
    long bitsChanged = andMask ^ orMask;
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        for (long offset = array.getBaseOffset(); offset < limit; offset += 16) {
          counts[i][(int)((Platform.getLong(baseObject, offset + 8) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }
  
  private static void sortKeyPrefixArrayAtByte(
      LongArray array, long[] counts, int byteIdx, int numRecords, int dataIndex, int tmpIndex,
      boolean desc, boolean signed) {
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + tmpIndex * 8, 16, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + dataIndex * 8;
    long maxOffset = baseOffset + numRecords * 16;
    for (long offset = baseOffset; offset < maxOffset; offset += 16) {
      long key = Platform.getLong(baseObject, offset);
      long prefix = Platform.getLong(baseObject, offset + 8);
      int bucket = (int)((prefix >>> (byteIdx * 8)) & 0xff);
      long dest = offsets[bucket];
      Platform.putLong(baseObject, dest, key);
      Platform.putLong(baseObject, dest + 8, prefix);
      offsets[bucket] += 16;
    }
  }
}

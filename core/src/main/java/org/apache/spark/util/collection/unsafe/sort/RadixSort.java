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

  /**
   * Sorts a given array of longs using least-significant-digit radix sort. This routine assumes
   * you have extra space at the end of the array at least equal to the number of records. The
   * sort is destructive and may relocate the data positioned within the array.
   *
   * @param array array of long elements followed by at least that many empty slots.
   * @param numRecords number of data records in the array.
   * @param startByteIndex the first byte (in range [0, 7]) to sort each long by, counting from the
   *                       least significant byte.
   * @param endByteIndex the last byte (in range [0, 7]) to sort each long by, counting from the
   *                     least significant byte. Must be greater than startByteIndex.
   * @param desc whether this is a descending (binary-order) sort.
   * @param signed whether this is a signed (two's complement) sort.
   *
   * @return The starting index of the sorted data within the given array. We return this instead
   *         of always copying the data back to position zero for efficiency.
   */
  public static int sort(
      LongArray array, int numRecords, int startByteIndex, int endByteIndex,
      boolean desc, boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    assert numRecords * 2 <= array.size();
    int inIndex = 0;
    int outIndex = numRecords;
    if (numRecords > 0) {
      long[][] counts = getCounts(array, numRecords, startByteIndex, endByteIndex);
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortAtByte(
            array, numRecords, counts[i], i, inIndex, outIndex,
            desc, signed && i == endByteIndex);
          int tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return inIndex;
  }

  /**
   * Performs a partial sort by copying data into destination offsets for each byte value at the
   * specified byte offset.
   *
   * @param array array to partially sort.
   * @param numRecords number of data records in the array.
   * @param counts counts for each byte value. This routine destructively modifies this array.
   * @param byteIdx the byte in a long to sort at, counting from the least significant byte.
   * @param inIndex the starting index in the array where input data is located.
   * @param outIndex the starting index where sorted output data should be written.
   * @param desc whether this is a descending (binary-order) sort.
   * @param signed whether this is a signed (two's complement) sort (only applies to last byte).
   */
  private static void sortAtByte(
      LongArray array, int numRecords, long[] counts, int byteIdx, int inIndex, int outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8, 8, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8;
    long maxOffset = baseOffset + numRecords * 8;
    for (long offset = baseOffset; offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      Platform.putLong(baseObject, offsets[bucket], value);
      offsets[bucket] += 8;
    }
  }

  /**
   * Computes a value histogram for each byte in the given array.
   *
   * @param array array to count records in.
   * @param numRecords number of data records in the array.
   * @param startByteIndex the first byte to compute counts for (the prior are skipped).
   * @param endByteIndex the last byte to compute counts for.
   *
   * @return an array of eight 256-byte count arrays, one for each byte starting from the least
   *         significant byte. If the byte does not need sorting the array will be null.
   */
  private static long[][] getCounts(
      LongArray array, int numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    // Optimization: do a fast pre-pass to determine which byte indices we can skip for sorting.
    // If all the byte values at a particular index are the same we don't need to count it.
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long maxOffset = array.getBaseOffset() + numRecords * 8;
    Object baseObject = array.getBaseObject();
    for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    // Compute counts for each byte index.
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        // TODO(ekl) consider computing all the counts in one pass.
        for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
          counts[i][(int)((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * Transforms counts into the proper unsafe output offsets for the sort type.
   *
   * @param counts counts for each byte value. This routine destructively modifies this array.
   * @param numRecords number of data records in the original data array.
   * @param outputOffset output offset in bytes from the base array object.
   * @param bytesPerRecord size of each record (8 for plain sort, 16 for key-prefix sort).
   * @param desc whether this is a descending (binary-order) sort.
   * @param signed whether this is a signed (two's complement) sort.
   *
   * @return the input counts array.
   */
  private static long[] transformCountsToOffsets(
      long[] counts, int numRecords, long outputOffset, int bytesPerRecord,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    int start = signed ? 128 : 0;  // output the negative records first (values 129-255).
    if (desc) {
      int pos = numRecords;
      for (int i = start; i < start + 256; i++) {
        pos -= counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
      }
    } else {
      int pos = 0;
      for (int i = start; i < start + 256; i++) {
        long tmp = counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
        pos += tmp;
      }
    }
    return counts;
  }

  /**
   * Specialization of sort() for key-prefix arrays. In this type of array, each record consists
   * of two longs, only the second of which is sorted on.
   *
   * @param startIndex starting index in the array to sort from. This parameter is not supported
   *    in the plain sort() implementation.
   */
  public static int sortKeyPrefixArray(
      LongArray array,
      int startIndex,
      int numRecords,
      int startByteIndex,
      int endByteIndex,
      boolean desc,
      boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    assert numRecords * 4 <= array.size();
    int inIndex = startIndex;
    int outIndex = startIndex + numRecords * 2;
    if (numRecords > 0) {
      long[][] counts = getKeyPrefixArrayCounts(
        array, startIndex, numRecords, startByteIndex, endByteIndex);
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortKeyPrefixArrayAtByte(
            array, numRecords, counts[i], i, inIndex, outIndex,
            desc, signed && i == endByteIndex);
          int tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return inIndex;
  }

  /**
   * Specialization of getCounts() for key-prefix arrays. We could probably combine this with
   * getCounts with some added parameters but that seems to hurt in benchmarks.
   */
  private static long[][] getKeyPrefixArrayCounts(
      LongArray array, int startIndex, int numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long baseOffset = array.getBaseOffset() + startIndex * 8L;
    long limit = baseOffset + numRecords * 16L;
    Object baseObject = array.getBaseObject();
    for (long offset = baseOffset; offset < limit; offset += 16) {
      long value = Platform.getLong(baseObject, offset + 8);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        for (long offset = baseOffset; offset < limit; offset += 16) {
          counts[i][(int)((Platform.getLong(baseObject, offset + 8) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * Specialization of sortAtByte() for key-prefix arrays.
   */
  private static void sortKeyPrefixArrayAtByte(
      LongArray array, int numRecords, long[] counts, int byteIdx, int inIndex, int outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8, 16, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8L;
    long maxOffset = baseOffset + numRecords * 16L;
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

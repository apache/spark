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

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.array.LongArray;

public class RadixSort {

  public static int sort(
      LongArray data, int length, MemoryConsumer consumer, int startByteIndex, int endByteIndex) {
    System.out.println("radixSort: " + length);
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert startByteIndex <= endByteIndex;
    int inOffset = 0;
    int outOffset = length;
    if (length > 0) {
      long orMask = 0;
      long andMask = ~0L;
      for (int i = 0; i < length; i++) {
        long value = data.get(i);
        orMask |= value;
        andMask &= value;
      }
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        long bitMin = ((orMask >>> (i * 8)) & 0xff);
        long bitMax = ((andMask >>> (i * 8)) & 0xff);
        if (bitMin != bitMax) {
          System.out.println("sort " + i);
          sortAtByte(data, i, length, inOffset, outOffset);
          int tmp = inOffset;
          inOffset = outOffset;
          outOffset = tmp;
        } else {
          System.out.println("skip " + i);
        }
      }
    }
    return inOffset;
  }

  private static void sortAtByte(
      LongArray in, int byteIndex, int length, int inOffset, int outOffset) {
    int[] outOffsets = getOffsets(in, byteIndex, length, inOffset, outOffset);
    for (int i=inOffset; i < inOffset + length; i++) {
      long value = in.get(i);
      int bucket = (int)((value >>> (byteIndex * 8)) & 0xff);
      in.set(outOffsets[bucket]++, value);
    }
  }

  // TODO(ekl) we should probably pre-compute these up-front
  private static int[] getOffsets(
      LongArray data, int byteIndex, int length, int inOffset, int outOffset) {
    int[] counts = new int[256];
    for (int i=0; i < length; i++) {
      counts[(int)((data.get(inOffset + i) >>> (byteIndex * 8)) & 0xff)]++;
    }
    int accum = 0;
    for (int i=0; i < 256; i++) {
      int tmp = counts[i];
      counts[i] = outOffset + accum;
      accum += tmp;
    }
    return counts;
  }
}

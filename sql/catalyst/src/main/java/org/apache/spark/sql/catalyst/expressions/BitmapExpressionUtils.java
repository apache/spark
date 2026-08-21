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

/**
 * A utility class for constructing bitmap expressions.
 */
public class BitmapExpressionUtils {
  /** Number of bytes in a bitmap. */
  public static final int NUM_BYTES = 4 * 1024;

  /** Number of bits in a bitmap. */
  public static final int NUM_BITS = 8 * NUM_BYTES;

  public static long bitmapBucketNumber(long value) {
    if (value > 0) {
      return 1 + (value - 1) / NUM_BITS;
    }
    return value / NUM_BITS;
  }

  public static long bitmapBitPosition(long value) {
    if (value > 0) {
      // inputs: (1 -> NUM_BITS) map to positions (0 -> NUM_BITS - 1)
      return (value - 1) % NUM_BITS;
    }
    return (-value) % NUM_BITS;
  }

  public static long bitmapCount(byte[] bitmap) {
    long count = 0;
    for (byte b : bitmap) {
      count += Integer.bitCount(b & 0x0FF);
    }
    return count;
  }

  /** Merges both bitmaps and writes the result into bitmap1. */
  public static void bitmapMerge(byte[] bitmap1, byte[] bitmap2) {
    for (int i = 0; i < java.lang.Math.min(bitmap1.length, bitmap2.length); ++i) {
      bitmap1[i] = (byte) ((bitmap1[i] & 0x0FF) | (bitmap2[i] & 0x0FF));
    }
  }

  /** Performs bitwise XOR on both bitmaps and writes the result into bitmap1. */
  public static void bitmapXorMerge(byte[] bitmap1, byte[] bitmap2) {
    // For XOR operation, bytes beyond the shorter bitmap's length are unchanged
    // since XOR with 0 (absent bits) is identity: X ^ 0 = X.
    for (int i = 0; i < java.lang.Math.min(bitmap1.length, bitmap2.length); ++i) {
      bitmap1[i] = (byte) ((bitmap1[i] & 0x0FF) ^ (bitmap2[i] & 0x0FF));
    }
  }

  /** Performs bitwise AND on both bitmaps and writes the result into bitmap1. */
  public static void bitmapAndMerge(byte[] bitmap1, byte[] bitmap2) {
    int minLen = java.lang.Math.min(bitmap1.length, bitmap2.length);
    for (int i = 0; i < minLen; ++i) {
      bitmap1[i] = (byte) ((bitmap1[i] & 0x0FF) & (bitmap2[i] & 0x0FF));
    }
    // For AND operation, any bytes beyond the input length should be set to 0
    // since they represent bits that don't exist in the input bitmap
    for (int i = bitmap2.length; i < bitmap1.length; ++i) {
      bitmap1[i] = 0;
    }
  }

  /** Performs bitwise AND on both bitmaps and returns a new fixed-size bitmap. */
  public static byte[] bitmapAnd(byte[] bitmap1, byte[] bitmap2) {
    byte[] result = new byte[NUM_BYTES];
    int numBytes = java.lang.Math.min(
        NUM_BYTES, java.lang.Math.min(bitmap1.length, bitmap2.length));
    for (int i = 0; i < numBytes; ++i) {
      result[i] = (byte) ((bitmap1[i] & 0x0FF) & (bitmap2[i] & 0x0FF));
    }
    return result;
  }

  /** Performs bitwise OR on both bitmaps and returns a new fixed-size bitmap. */
  public static byte[] bitmapOr(byte[] bitmap1, byte[] bitmap2) {
    byte[] longer = bitmap1.length >= bitmap2.length ? bitmap1 : bitmap2;
    byte[] shorter = bitmap1.length >= bitmap2.length ? bitmap2 : bitmap1;
    byte[] result = java.util.Arrays.copyOf(longer, NUM_BYTES);
    bitmapMerge(result, shorter);
    return result;
  }

  /** Performs bitwise AND NOT on both bitmaps and returns a new fixed-size bitmap. */
  public static byte[] bitmapAndNot(byte[] bitmap1, byte[] bitmap2) {
    byte[] result = java.util.Arrays.copyOf(bitmap1, NUM_BYTES);
    int numBytes = java.lang.Math.min(
        NUM_BYTES, java.lang.Math.min(bitmap1.length, bitmap2.length));
    for (int i = 0; i < numBytes; ++i) {
      result[i] = (byte) ((result[i] & 0x0FF) & ~(bitmap2[i] & 0x0FF));
    }
    return result;
  }

  /** Performs bitwise XOR on both bitmaps and returns a new fixed-size bitmap. */
  public static byte[] bitmapXor(byte[] bitmap1, byte[] bitmap2) {
    byte[] longer = bitmap1.length >= bitmap2.length ? bitmap1 : bitmap2;
    byte[] shorter = bitmap1.length >= bitmap2.length ? bitmap2 : bitmap1;
    byte[] result = java.util.Arrays.copyOf(longer, NUM_BYTES);
    int numBytes = java.lang.Math.min(NUM_BYTES, shorter.length);
    for (int i = 0; i < numBytes; ++i) {
      result[i] = (byte) ((result[i] & 0x0FF) ^ (shorter[i] & 0x0FF));
    }
    return result;
  }
}

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

package org.apache.spark.unsafe.array;

import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  /** Returns the next number greater or equal num that is power of 2. */
  public static long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    return (int)roundNumberOfBytesToNearestWord((long)numBytes);
  }

  public static long roundNumberOfBytesToNearestWord(long numBytes) {
    long remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
    return numBytes + ((8 - remainder) & 0x7);
  }

  public static final int MAX_ROUNDED_ARRAY_LENGTH = ByteArrayUtils.MAX_ROUNDED_ARRAY_LENGTH;

  private static final boolean unaligned = Platform.unaligned();
  private static final boolean IS_LITTLE_ENDIAN =
    ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  /**
   * Optimized byte array equality check for byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBase, long leftOffset, Object rightBase, long rightOffset, final long length) {
    long i = 0;

    // check if stars align and we can get both offsets to be aligned
    if (!unaligned && ((leftOffset % 8) == (rightOffset % 8))) {
      while ((leftOffset + i) % 8 != 0 && i < length) {
        if (Platform.getByte(leftBase, leftOffset + i) !=
            Platform.getByte(rightBase, rightOffset + i)) {
              return false;
        }
        i += 1;
      }
    }
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (unaligned || (((leftOffset + i) % 8 == 0) && ((rightOffset + i) % 8 == 0))) {
      while (i <= length - 8) {
        if (Platform.getLong(leftBase, leftOffset + i) !=
            Platform.getLong(rightBase, rightOffset + i)) {
              return false;
        }
        i += 8;
      }
    }
    // this will finish off the unaligned comparisons, or do the entire aligned
    // comparison whichever is needed.
    while (i < length) {
      if (Platform.getByte(leftBase, leftOffset + i) !=
          Platform.getByte(rightBase, rightOffset + i)) {
            return false;
      }
      i += 1;
    }
    return true;
  }

  public static boolean contains(byte[] arr, byte[] sub) {
    return contains(arr, BYTE_ARRAY_OFFSET, arr.length, sub, BYTE_ARRAY_OFFSET, sub.length);
  }

  /**
   * Returns whether the `length` bytes at `base` + `offset` contain the `subLength` bytes at
   * `subBase` + `subOffset` as a contiguous subsequence.
   */
  public static boolean contains(
      Object base, long offset, int length, Object subBase, long subOffset, int subLength) {
    if (subLength == 0) {
      return true;
    }
    // The last position at which the subsequence can start.
    final int last = length - subLength;
    if (last < 0) {
      return false;
    }

    final byte firstByte = Platform.getByte(subBase, subOffset);
    final byte lastByte = Platform.getByte(subBase, subOffset + subLength - 1);
    if (unaligned && last >= 7) {
      // Test 8 candidate positions at a time. XOR the 8-byte word at `i` with the first byte of
      // the subsequence repeated, and the word at `i + subLength - 1` with its last byte
      // repeated; a zero byte in the OR of the two marks a position where both the first and the
      // last byte match, leaving only the inner bytes to compare. The final word is moved back
      // to end at `last`, overlapping the previous one, so there is no byte-at-a-time tail.
      final long firstBytes = (firstByte & 0xFFL) * 0x0101010101010101L;
      final long lastBytes = (lastByte & 0xFFL) * 0x0101010101010101L;
      int i = 0;
      while (true) {
        long candidates = zeroBytes(
          (Platform.getLong(base, offset + i) ^ firstBytes) |
          (Platform.getLong(base, offset + i + subLength - 1) ^ lastBytes));
        if (candidates != 0) {
          if (!IS_LITTLE_ENDIAN) {
            // Move the flag for the byte at the lowest address into the lowest byte.
            candidates = Long.reverseBytes(candidates);
          }
          do {
            int pos = i + (Long.numberOfTrailingZeros(candidates) >>> 3);
            if (innerBytesEqual(base, offset + pos, subBase, subOffset, subLength)) {
              return true;
            }
            candidates &= candidates - 1;
          } while (candidates != 0);
        }
        if (i == last - 7) {
          return false;
        }
        i = Math.min(i + 8, last - 7);
      }
    }

    for (int i = 0; i <= last; i++) {
      if (Platform.getByte(base, offset + i) == firstByte &&
          Platform.getByte(base, offset + i + subLength - 1) == lastByte &&
          innerBytesEqual(base, offset + i, subBase, subOffset, subLength)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a mask with the high bit set in each byte of `x` that is zero, and no other bits set.
   * Unlike the shorter `(x - 0x01..01) & ~x & 0x80..80`, no borrow propagates between bytes, so
   * a set bit never marks a non-zero byte.
   */
  private static long zeroBytes(long x) {
    final long low7 = 0x7F7F7F7F7F7F7F7FL;
    return ~(((x & low7) + low7) | x | low7);
  }

  /**
   * Returns whether the `length` bytes at the two locations are equal, given that their first
   * and last bytes are already known to be.
   */
  private static boolean innerBytesEqual(
      Object leftBase, long leftOffset, Object rightBase, long rightOffset, int length) {
    return length <= 2 ||
      arrayEquals(leftBase, leftOffset + 1, rightBase, rightOffset + 1, length - 2);
  }

  public static boolean startsWith(byte[] array, byte[] target) {
    if (target.length > array.length) {
      return false;
    }
    return arrayEquals(array, BYTE_ARRAY_OFFSET, target, BYTE_ARRAY_OFFSET, target.length);
  }

  public static boolean endsWith(byte[] array, byte[] target) {
    if (target.length > array.length) {
      return false;
    }
    return arrayEquals(array, BYTE_ARRAY_OFFSET + array.length - target.length,
      target, BYTE_ARRAY_OFFSET, target.length);
  }

  public static boolean matchAt(byte[] arr, byte[] sub, int pos) {
    if (sub.length + pos > arr.length || pos < 0) {
      return false;
    }
    return arrayEquals(arr, BYTE_ARRAY_OFFSET + pos, sub, BYTE_ARRAY_OFFSET, sub.length);
  }
}

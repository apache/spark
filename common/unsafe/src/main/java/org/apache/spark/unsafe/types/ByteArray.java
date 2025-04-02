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

package org.apache.spark.unsafe.types;

import java.nio.ByteOrder;
import java.util.Arrays;

import com.google.common.primitives.Ints;

import org.apache.spark.unsafe.Platform;

public final class ByteArray {

  public static final byte[] EMPTY_BYTE = new byte[0];
  private static final boolean IS_LITTLE_ENDIAN =
      ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  /**
   * Writes the content of a byte array into a memory address, identified by an object and an
   * offset. The target memory address must already been allocated, and have enough space to
   * hold all the bytes in this string.
   */
  public static void writeToMemory(byte[] src, Object target, long targetOffset) {
    Platform.copyMemory(src, Platform.BYTE_ARRAY_OFFSET, target, targetOffset, src.length);
  }

  /**
   * Returns a 64-bit integer that can be used as the prefix used in sorting.
   */
  public static long getPrefix(byte[] bytes) {
    if (bytes == null) {
      return 0L;
    }
    return getPrefix(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
  }

  static long getPrefix(Object base, long offset, int numBytes) {
    // Since JVMs are either 4-byte aligned or 8-byte aligned, we check the size of the bytes.
    // If size is 0, just return 0.
    // If size is between 1 and 4 (inclusive), assume data is 4-byte aligned under the hood and
    // use a getInt to fetch the prefix.
    // If size is greater than 4, assume we have at least 8 bytes of data to fetch.
    // After getting the data, we use a mask to mask out data that is not part of the bytes.
    final long p;
    final long mask;
    if (numBytes >= 8) {
      p = Platform.getLong(base, offset);
      mask = 0;
    } else if (numBytes > 4) {
      p = Platform.getLong(base, offset);
      mask = (1L << (8 - numBytes) * 8) - 1;
    } else if (numBytes > 0) {
      long pRaw = Platform.getInt(base, offset);
      p = IS_LITTLE_ENDIAN ? pRaw : (pRaw << 32);
      mask = (1L << (8 - numBytes) * 8) - 1;
    } else {
      p = 0;
      mask = 0;
    }
    return (IS_LITTLE_ENDIAN ? java.lang.Long.reverseBytes(p) : p) & ~mask;
  }

  public static int compareBinary(byte[] leftBase, byte[] rightBase) {
    return compareBinary(leftBase, Platform.BYTE_ARRAY_OFFSET, leftBase.length,
        rightBase, Platform.BYTE_ARRAY_OFFSET, rightBase.length);
  }

  static int compareBinary(
      Object leftBase,
      long leftOffset,
      int leftNumBytes,
      Object rightBase,
      long rightOffset,
      int rightNumBytes) {
    int len = Math.min(leftNumBytes, rightNumBytes);
    int wordMax = (len / 8) * 8;
    for (int i = 0; i < wordMax; i += 8) {
      long left = Platform.getLong(leftBase, leftOffset + i);
      long right = Platform.getLong(rightBase, rightOffset + i);
      if (left != right) {
        if (IS_LITTLE_ENDIAN) {
          return Long.compareUnsigned(Long.reverseBytes(left), Long.reverseBytes(right));
        } else {
          return Long.compareUnsigned(left, right);
        }
      }
    }
    for (int i = wordMax; i < len; i++) {
      // Both UTF-8 and byte array should be compared as unsigned int.
      int res = (Platform.getByte(leftBase, leftOffset + i) & 0xFF) -
          (Platform.getByte(rightBase, rightOffset + i) & 0xFF);
      if (res != 0) {
        return res;
      }
    }
    return leftNumBytes - rightNumBytes;
  }

  public static byte[] subStringSQL(byte[] bytes, int pos, int len) {
    // This pos calculation is according to UTF8String#subStringSQL
    if (pos > bytes.length) {
      return EMPTY_BYTE;
    }
    int start = 0;
    int end;
    if (pos > 0) {
      start = pos - 1;
    } else if (pos < 0) {
      start = bytes.length + pos;
    }
    if ((bytes.length - start) < len) {
      end = bytes.length;
    } else {
      end = start + len;
    }
    start = Math.max(start, 0); // underflow
    if (start >= end) {
      return EMPTY_BYTE;
    }
    return Arrays.copyOfRange(bytes, start, end);
  }

  /**
   * Concatenate multiple byte arrays into one.
   * If one of the inputs is null then null will be returned.
   *
   * @param inputs byte arrays to concatenate
   * @return the concatenated byte array or null if one of the arguments is null
   */
  public static byte[] concat(byte[]... inputs) {
    return concatWS(EMPTY_BYTE, inputs);
  }

  /**
   * Concatenate multiple byte arrays with a given delimiter.
   * If the delimiter or one of the inputs is null then null will be returned.
   *
   * @param delimiter byte array to be placed between each input
   * @param inputs    byte arrays to concatenate
   * @return the concatenated byte array or null if one of the arguments is null
   */
  public static byte[] concatWS(byte[] delimiter, byte[]... inputs) {
    if (delimiter == null) {
      return null;
    }
    // Compute the total length of the result
    long totalLength = 0;
    for (byte[] input : inputs) {
      if (input != null) {
        totalLength += input.length + delimiter.length;
      } else {
        return null;
      }
    }
    if (totalLength > 0) totalLength -= delimiter.length;
    // Allocate a new byte array, and copy the inputs one by one into it
    final byte[] result = new byte[Ints.checkedCast(totalLength)];
    int offset = 0;
    for (int i = 0; i < inputs.length; i++) {
      byte[] input = inputs[i];
      int len = input.length;
      Platform.copyMemory(
        input, Platform.BYTE_ARRAY_OFFSET,
        result, Platform.BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
      if (delimiter.length > 0 && i < inputs.length - 1) {
        Platform.copyMemory(
          delimiter, Platform.BYTE_ARRAY_OFFSET,
          result, Platform.BYTE_ARRAY_OFFSET + offset,
          delimiter.length);
        offset += delimiter.length;
      }
    }
    return result;
  }

  // Helper method for implementing `lpad` and `rpad`.
  // If the padding pattern's length is 0, return the first `len` bytes of the input byte
  // sequence if it is longer than `len` bytes, or a copy of the byte sequence, otherwise.
  private static byte[] padWithEmptyPattern(byte[] bytes, int len) {
    len = Math.min(bytes.length, len);
    final byte[] result = new byte[len];
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, result, Platform.BYTE_ARRAY_OFFSET, len);
    return result;
  }

  // Helper method for implementing `lpad` and `rpad`.
  // Fills the resulting byte sequence with the pattern. The resulting byte sequence is
  // passed as the first argument and it is filled from position `firstPos` (inclusive)
  // to position `beyondPos` (not inclusive).
  private static void fillWithPattern(byte[] result, int firstPos, int beyondPos, byte[] pad) {
    for (int pos = firstPos; pos < beyondPos; pos += pad.length) {
      final int jMax = Math.min(pad.length, beyondPos - pos);
      for (int j = 0; j < jMax; ++j) {
        result[pos + j] = pad[j];
      }
    }
  }

  // Left-pads the input byte sequence using the provided padding pattern.
  // In the special case that the padding pattern is empty, the resulting byte sequence
  // contains the first `len` bytes of the input if they exist, or is a copy of the input
  // byte sequence otherwise.
  // For padding patterns with positive byte length, the resulting byte sequence's byte length is
  // equal to `len`. If the input byte sequence is not less than `len` bytes, its first `len` bytes
  // are returned. Otherwise, the remaining missing bytes are filled in with the provided pattern.
  public static byte[] lpad(byte[] bytes, int len, byte[] pad) {
    if (bytes == null || pad == null) return null;
    // If the input length is 0, return the empty byte sequence.
    if (len == 0) return EMPTY_BYTE;
    // The padding pattern is empty.
    if (pad.length == 0) return padWithEmptyPattern(bytes, len);
    // The general case.
    // 1. Copy the first `len` bytes of the input byte sequence into the output if they exist.
    final byte[] result = new byte[len];
    final int minLen = Math.min(len, bytes.length);
    Platform.copyMemory(
      bytes, Platform.BYTE_ARRAY_OFFSET,
      result, Platform.BYTE_ARRAY_OFFSET + len - minLen,
      minLen);
    // 2. If the input has less than `len` bytes, fill in the rest using the provided pattern.
    if (bytes.length < len) {
      fillWithPattern(result, 0, len - bytes.length, pad);
    }
    return result;
  }

  // Right-pads the input byte sequence using the provided padding pattern.
  // In the special case that the padding pattern is empty, the resulting byte sequence
  // contains the first `len` bytes of the input if they exist, or is a copy of the input
  // byte sequence otherwise.
  // For padding patterns with positive byte length, the resulting byte sequence's byte length is
  // equal to `len`. If the input byte sequence is not less than `len` bytes, its first `len` bytes
  // are returned. Otherwise, the remaining missing bytes are filled in with the provided pattern.
  public static byte[] rpad(byte[] bytes, int len, byte[] pad) {
    if (bytes == null || pad == null) return null;
    // If the input length is 0, return the empty byte sequence.
    if (len == 0) return EMPTY_BYTE;
    // The padding pattern is empty.
    if (pad.length == 0) return padWithEmptyPattern(bytes, len);
    // The general case.
    // 1. Copy the first `len` bytes of the input sequence into the output if they exist.
    final byte[] result = new byte[len];
    Platform.copyMemory(
      bytes, Platform.BYTE_ARRAY_OFFSET,
      result, Platform.BYTE_ARRAY_OFFSET,
      Math.min(len, bytes.length));
    // 2. If the input has less than `len` bytes, fill in the rest using the provided pattern.
    if (bytes.length < len) {
      fillWithPattern(result, bytes.length, len, pad);
    }
    return result;
  }
}

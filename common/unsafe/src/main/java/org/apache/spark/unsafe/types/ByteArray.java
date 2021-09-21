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

import java.util.Arrays;

import com.google.common.primitives.Ints;

import org.apache.spark.unsafe.Platform;

public final class ByteArray {

  public static final byte[] EMPTY_BYTE = new byte[0];

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
    } else {
      final int minLen = Math.min(bytes.length, 8);
      long p = 0;
      for (int i = 0; i < minLen; ++i) {
        p |= ((long) Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET + i) & 0xff)
            << (56 - 8 * i);
      }
      return p;
    }
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

  public static byte[] concat(byte[]... inputs) {
    // Compute the total length of the result
    long totalLength = 0;
    for (byte[] input : inputs) {
      if (input != null) {
        totalLength += input.length;
      } else {
        return null;
      }
    }

    // Allocate a new byte array, and copy the inputs one by one into it
    final byte[] result = new byte[Ints.checkedCast(totalLength)];
    int offset = 0;
    for (byte[] input : inputs) {
      int len = input.length;
      Platform.copyMemory(
        input, Platform.BYTE_ARRAY_OFFSET,
        result, Platform.BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
    }
    return result;
  }

  // Return the bitwise AND of two byte arrays. The byte length of the result is equal to the
  // maximum byte length of the two inputs. The two input byte arrays are aligned with respect
  // to their least significant (right-most) bytes.
  public static byte[] bitwiseAnd(byte[] bytes1, byte[] bytes2) {
    if (bytes1 == null || bytes2 == null) return null;
    // Compute the length of the result (maximum of the lengths of the inputs).
    final int len1 = bytes1.length;
    final int len2 = bytes2.length;
    final int maxLen = Math.max(len1, len2);
    if (maxLen == 0) {
      return EMPTY_BYTE;
    }
    final byte[] result = new byte[maxLen];
    final int minLen = Math.min(len1, len2);
    // Initialize the first `maxLen - minLen` bytes to 0.
    Platform.setMemory(result, Platform.BYTE_ARRAY_OFFSET, maxLen - minLen, (byte)0);
    // Compute the right-most minLen bytes of the result.
    for (int j = 0; j < minLen; ++j) {
      result[maxLen - 1 - j] = (byte)(bytes1[len1 - 1 - j] & bytes2[len2 - 1 - j]);
    }
    return result;
  }

  // Return the bitwise OR of two byte arrays. The byte length of the result is equal to the
  // maximum byte length of the two inputs. The two input byte arrays are aligned with respect
  // to their least significant (right-most) bytes.
  public static byte[] bitwiseOr(byte[] bytes1, byte[] bytes2) {
    if (bytes1 == null || bytes2 == null) return null;
    // Compute the length of the result (maximum of the lengths of the inputs).
    final int len1 = bytes1.length;
    final int len2 = bytes2.length;
    final int maxLen = Math.max(len1, len2);
    if (maxLen == 0) {
      return EMPTY_BYTE;
    }
    final byte[] result = new byte[maxLen];
    final int minLen = Math.min(len1, len2);
    // Copy the first `maxLen - minLen` bytes of the longer byte array into the result buffer.
    final byte[] maxLenBytes = (len1 == maxLen) ? bytes1 : bytes2;
    Platform.copyMemory(
            maxLenBytes, Platform.BYTE_ARRAY_OFFSET,
            result, Platform.BYTE_ARRAY_OFFSET,
            maxLen - minLen);
    // Compute the right-most minLen bytes of the result.
    for (int j = 0; j < minLen; ++j) {
      result[maxLen - 1 - j] = (byte)(bytes1[len1 - 1 - j] | bytes2[len2 - 1 - j]);
    }
    return result;
  }

  // Return the bitwise XOR of two byte arrays. The byte length of the result is equal to the
  // maximum byte length of the two inputs. The two input byte arrays are aligned with respect
  // to their least significant (right-most) bytes.
  public static byte[] bitwiseXor(byte[] bytes1, byte[] bytes2) {
    if (bytes1 == null || bytes2 == null) return null;
    // Compute the length of the result (maximum of the lengths of the inputs).
    final int len1 = bytes1.length;
    final int len2 = bytes2.length;
    final int maxLen = Math.max(len1, len2);
    if (maxLen == 0) {
      return EMPTY_BYTE;
    }
    final byte[] result = new byte[maxLen];
    final int minLen = Math.min(len1, len2);
    // Copy the first `maxLen - minLen` bytes of the longer byte array into the result buffer.
    final byte[] maxLenBytes = (len1 == maxLen) ? bytes1 : bytes2;
    Platform.copyMemory(
            maxLenBytes, Platform.BYTE_ARRAY_OFFSET,
            result, Platform.BYTE_ARRAY_OFFSET,
            maxLen - minLen);
    // Compute the right-most minLen bytes of the result.
    for (int j = 0; j < minLen; ++j) {
      result[maxLen - 1 - j] = (byte)(bytes1[len1 - 1 - j] ^ bytes2[len2 - 1 - j]);
    }
    return result;
  }

  // Return the bitwise NOT of a byte array.
  public static byte[] bitwiseNot(byte[] bytes) {
    if (bytes == null) return null;
    final int len = bytes.length;
    if (bytes.length == 0) {
      return EMPTY_BYTE;
    }
    final byte[] result = new byte[len];
    for (int i = 0; i < len; ++i) {
      result[i] = (byte)(~bytes[i]);
    }
    return result;
  }
}

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
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        totalLength += (long)inputs[i].length;
      } else {
        return null;
      }
    }

    // Allocate a new byte array, and copy the inputs one by one into it
    final byte[] result = new byte[Ints.checkedCast(totalLength)];
    int offset = 0;
    for (int i = 0; i < inputs.length; i++) {
      int len = inputs[i].length;
      Platform.copyMemory(
        inputs[i], Platform.BYTE_ARRAY_OFFSET,
        result, Platform.BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
    }
    return result;
  }

  /**
   * Trims bytes representing spaces from both ends of this byte array.
   *
   * @param srcArr the source byte array
   * @return this byte array with no bytes representing spaces at the start or end
   */
  public static byte[] trim(byte[] srcArr) {
    return trim(srcArr, new byte[]{(byte) 32});
  }

  /**
   * Trims instances of the given trim byte array from both ends of this byte array.
   *
   * @param srcArr the source byte array
   * @param trimArr the trim byte array
   * @return this byte array with no occurrences of the trim byte array at the start or end,
   * or `null` if `trimArr` is `null`
   */
  public static byte[] trim(byte[] srcArr, byte[] trimArr) {
    return trimLeft(trimRight(srcArr, trimArr), trimArr);
  }

  /**
   * Trims instances of the given trim byte array from the start of this byte array.
   *
   * @param srcArr the source byte array
   * @param trimArr the trim byte array
   * @return this byte array with no occurrences of the trim byte array at the start,
   * or `null` if `trimArr` is `null`
   */
  public static byte[] trimLeft(byte[] srcArr, byte[] trimArr) {
    if (trimArr == null) {
      return null;
    } else {
      // the searching byte position in the source byte array
      int searchIdx = 0;

      while (searchIdx < srcArr.length) {
        boolean matched = false;
        for (byte b: trimArr) {
          if (b == srcArr[searchIdx]) {
            matched = true;
            break;
          }
        }
        if (matched) {
          searchIdx += 1;
        } else {
          break;
        }
      }
      return subStringSQL(srcArr, searchIdx + 1, srcArr.length - searchIdx);
    }
  }

  /**
   * Trims instances of the given trim byte array from the end of this byte array.
   *
   * @param srcArr the source byte array
   * @param trimArr the trim byte array
   * @return this byte array with no occurrences of the trim byte array at the end,
   * or `null` if `trimArr` is `null`
   */
  public static byte[] trimRight(byte[] srcArr, byte[] trimArr) {
    if (trimArr == null) {
      return null;
    } else {
      // the searching byte position in the source byte array
      int searchIdx = srcArr.length - 1;

      while (searchIdx > -1) {
        boolean matched = false;
        for (byte b: trimArr) {
          if (b == srcArr[searchIdx]) {
            matched = true;
            break;
          }
        }
        if (matched) {
          searchIdx -= 1;
        } else {
          break;
        }
      }

      return subStringSQL(srcArr, 0, searchIdx + 1);
    }
  }
}

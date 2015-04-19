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

package org.apache.spark.unsafe.string;

import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.array.ByteArrayMethods;

import java.io.UnsupportedEncodingException;import java.lang.Object;import java.lang.String;

/**
 * A String encoded in UTF-8 as long representing the string's length, followed by a
 * contiguous region of bytes; see http://en.wikipedia.org/wiki/UTF-8 for details.
 */
public final class UTF8StringMethods {

  private UTF8StringMethods() {
    // Make the default constructor private, since this only holds static methods.
    // See UTF8StringPointer for a more object-oriented interface to UTF8String data.
  }

  /**
   * Return the length of the string, in bytes (NOT characters), not including
   * the space to store the length itself.
   */
  static long getLengthInBytes(Object baseObject, long baseOffset) {
    return PlatformDependent.UNSAFE.getLong(baseObject, baseOffset);
  }

  public static int compare(
      Object leftBaseObject,
      long leftBaseOffset,
      int leftLengthInBytes,
      Object rightBaseObject,
      long rightBaseOffset,
      int rightLengthInBytes) {
    int i = 0;
    while (i < leftLengthInBytes && i < rightLengthInBytes) {
      final byte leftByte = PlatformDependent.UNSAFE.getByte(leftBaseObject, leftBaseOffset + i);
      final byte rightByte = PlatformDependent.UNSAFE.getByte(rightBaseObject, rightBaseOffset + i);
      final int res = leftByte - rightByte;
      if (res != 0) return res;
      i += 1;
    }
    return leftLengthInBytes - rightLengthInBytes;
  }

  public static boolean startsWith(
      Object strBaseObject,
      long strBaseOffset,
      int strLengthInBytes,
      Object prefixBaseObject,
      long prefixBaseOffset,
      int prefixLengthInBytes) {
    if (prefixLengthInBytes > strLengthInBytes) {
      return false;
    } {
      return ByteArrayMethods.arrayEquals(
        strBaseObject,
        strBaseOffset,
        prefixBaseObject,
        prefixBaseOffset,
        prefixLengthInBytes);
    }
  }

  public static boolean endsWith(
      Object strBaseObject,
      long strBaseOffset,
      int strLengthInBytes,
      Object suffixBaseObject,
      long suffixBaseOffset,
      int suffixLengthInBytes) {
    if (suffixLengthInBytes > strLengthInBytes) {
      return false;
    } {
      return ByteArrayMethods.arrayEquals(
        strBaseObject,
        strBaseOffset + strLengthInBytes - suffixLengthInBytes,
        suffixBaseObject,
        suffixBaseOffset,
        suffixLengthInBytes);
    }
  }

  /**
   * Return the number of code points in a string.
   *
   * This is only used by Substring() when `start` is negative.
   */
  public static int getLengthInCodePoints(Object baseObject, long baseOffset, int lengthInBytes) {
    int len = 0;
    int i = 0;
    while (i < lengthInBytes) {
      i += numOfBytes(PlatformDependent.UNSAFE.getByte(baseObject, baseOffset + i));
      len += 1;
    }
    return len;
  }

  public static String toJavaString(Object baseObject, long baseOffset, int lengthInBytes) {
    final byte[] bytes = new byte[(int) lengthInBytes];
    PlatformDependent.UNSAFE.copyMemory(
      baseObject,
      baseOffset,
      bytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      lengthInBytes
    );
    String str = null;
    try {
      str = new String(bytes, "utf-8");
    } catch (UnsupportedEncodingException e) {
      PlatformDependent.throwException(e);
    }
    return str;
  }

  /**
   * Write a Java string in UTF8String format to the specified memory location.
   *
   * @return the number of bytes written, including the space for tracking the string's length.
   */
  public static int createFromJavaString(Object baseObject, long baseOffset, String str) {
    final byte[] strBytes = str.getBytes();
    final int strLengthInBytes = strBytes.length;
    PlatformDependent.copyMemory(
      strBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      baseObject,
      baseOffset,
      strLengthInBytes
    );
    return strLengthInBytes;
  }

  /**
   * Return the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  public static int numOfBytes(byte b) {
    final int offset = (b & 0xFF) - 192;
    if (offset >= 0) {
      return bytesOfCodePointInUTF8[offset];
    } else {
      return 1;
    }
  }

  /**
   * number of tailing bytes in a UTF8 sequence for a code point
   * see http://en.wikipedia.org/wiki/UTF-8, 192-256 of Byte 1
   */
  private static int[] bytesOfCodePointInUTF8 = new int[] {
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6, 6, 6
  };

}

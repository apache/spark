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

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Locale;

import org.apache.spark.unsafe.array.ByteArrayMethods;

import static org.apache.spark.unsafe.PlatformDependent.*;

/**
 * A UTF-8 String for internal Spark use.
 * <p>
 * A String encoded in UTF-8 as an Array[Byte], which can be used for comparison,
 * search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 * <p>
 * Note: This is not designed for general use cases, should not be used outside SQL.
 */
public final class UTF8String implements Comparable<UTF8String>, Serializable {

  @Nonnull
  private final Object base;
  private final long offset;
  private final int numBytes;

  private static int[] bytesOfCodePointInUTF8 = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6, 6, 6};

  /**
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length);
    } else {
      return null;
    }
  }

  public static UTF8String fromString(String str) {
    if (str == null) return null;
    try {
      return fromBytes(str.getBytes("utf-8"));
    } catch (UnsupportedEncodingException e) {
      // Turn the exception into unchecked so we can find out about it at runtime, but
      // don't need to add lots of boilerplate code everywhere.
      throwException(e);
      return null;
    }
  }

  public UTF8String(Object base, long offset, int size) {
    this.base = base;
    this.offset = offset;
    this.numBytes = size;
  }

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  public static int numBytesForFirstByte(final byte b) {
    final int offset = (b & 0xFF) - 192;
    return (offset >= 0) ? bytesOfCodePointInUTF8[offset] : 1;
  }

  /**
   * Returns the code point that starts at `start`
   */
  private int codePointAt(int start, int num) {
    byte first = getByte(start);
    switch (num) {
      case 1:
        return (int) first;
      case 2:
        return ((first & 0x1F) << 6) | (getByte(start + 1) & 0x3F);
      default:
        int code = first & (1 << (7 - num) - 1);
        for (int i = 1; i < num; i ++) {
          code <<= 6;
          code += getByte(start + i);
        }
        return code;
    }
  }

  /**
   * Update code point using UTF-8 encoding at (base, offset).
   */
  private static void updateCodePoint(Object base, long offset, int code, int num) {
    switch (num) {
      case 1:
        UNSAFE.putByte(base, offset, (byte) code);
        break;
      case 2:
        UNSAFE.putByte(base, offset, (byte) ((code >> 6) & 0x1F | 0xC0));
        UNSAFE.putByte(base, offset + 1, (byte) (code & 0x3F | 0x80));
        break;
      default:
        for (int i = 1; i < num; i ++) {
          UNSAFE.putByte(base, offset + num - i, (byte) (code & 0x3F | 0x80));
          code >>>= 6;
        }
        int first = (code & ((1 << (7 - num)) - 1)) + ~((1 << (8 - num)) - 1);
        UNSAFE.putByte(base, offset, (byte) first);
    }
  }

  /**
   * Returns the number of bytes
   */
  public int numBytes() {
    return numBytes;
  }

  /**
   * Returns the number of code points in it.
   *
   * This is only used by Substring() when `start` is negative.
   */
  public int numChars() {
    int len = 0;
    for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
      len += 1;
    }
    return len;
  }

  /**
   * Returns the underline bytes, will be a copy of it if it's part of another array.
   */
  public byte[] getBytes() {
    // avoid copy if `base` is `byte[]`
    if (offset == BYTE_ARRAY_OFFSET && base instanceof byte[]
      && ((byte[]) base).length == numBytes) {
      return (byte[]) base;
    } else {
      byte[] bytes = new byte[numBytes];
      copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, numBytes);
      return bytes;
    }
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  public UTF8String substring(final int start, final int until) {
    if (until <= start || start >= numBytes) {
      return UTF8String.fromBytes(new byte[0]);
    }

    int i = 0;
    int c = 0;
    for (; i < numBytes && c < start; i += numBytesForFirstByte(getByte(i))) {
      c += 1;
    }

    int j = i;
    for (; j < numBytes && c < until; j += numBytesForFirstByte(getByte(i))) {
      c += 1;
    }

    byte[] bytes = new byte[j - i];
    copyMemory(base, offset + i, bytes, BYTE_ARRAY_OFFSET, j - i);
    return UTF8String.fromBytes(bytes);
  }

  public boolean contains(final UTF8String substring) {
    if (substring.numBytes == 0) {
      return true;
    }

    byte first = substring.getByte(0);
    for (int i = 0; i <= numBytes - substring.numBytes; i++) {
      if (getByte(i) == first && matchAt(substring, i)) {
        return true;
      }
    }
    return false;
  }

  private byte getByte(int i) {
    return UNSAFE.getByte(base, offset + i);
  }

  private boolean matchAt(final UTF8String s, int pos) {
    if (s.numBytes + pos > numBytes || pos < 0) {
      return false;
    }
    return ByteArrayMethods.arrayEquals(base, offset + pos, s.base, s.offset, s.numBytes);
  }

  public boolean startsWith(final UTF8String prefix) {
    return matchAt(prefix, 0);
  }

  public boolean endsWith(final UTF8String suffix) {
    return matchAt(suffix, numBytes - suffix.numBytes);
  }

  private static String lang = Locale.getDefault().getLanguage();
  private static boolean localeDependent = lang == "tr" || lang == "az" || lang == "lt";
  private static int ERROR = 0xFFFFFFFF;  // Character.ERROR

  /**
   * Returns the upper case of this string
   */
  public UTF8String toUpperCase() {
    byte[] buf = null;
    for (int i = 0; i < numBytes; ){
      int n = numBytesForFirstByte(getByte(i));
      int code = codePointAt(i, n);
      int upper = Character.toUpperCase(code);
      if (upper != code) {
        if (upper == ERROR || localeDependent) {
          // fallback to String.toUpperCase() to handle locale
          return fromString(toString().toUpperCase());
        }
        if (buf == null) {
          // It's always have the same number of bytes for upper case
          buf = new byte[numBytes];
          copyMemory(base, offset, buf, BYTE_ARRAY_OFFSET, numBytes);
        }
        updateCodePoint(buf, BYTE_ARRAY_OFFSET + i, upper, n);
      }
      i += n;
    }
    return buf != null ? fromBytes(buf) : this;
  }

  /**
   * Returns the lower case of this string
   */
  public UTF8String toLowerCase() {
    byte[] buf = null;
    for (int i = 0; i < numBytes; ){
      int n = numBytesForFirstByte(getByte(i));
      int code = codePointAt(i, n);
      int lower = Character.toLowerCase(code);
      if (lower != code) {
        if (lower == ERROR || localeDependent) {
          // fallback to String.toUpperCase() to handle locale
          return fromString(toString().toLowerCase());
        }
        if (buf == null) {
          // It's always have the same number of bytes for upper case
          buf = new byte[numBytes];
          copyMemory(base, offset, buf, BYTE_ARRAY_OFFSET, numBytes);
        }
        updateCodePoint(buf, BYTE_ARRAY_OFFSET + i, lower, n);
      }
      i += n;
    }
    return buf != null ? fromBytes(buf) : this;
  }

  @Override
  public String toString() {
    try {
      // this is slow
      return new String(getBytes(), "utf-8");
    } catch (UnsupportedEncodingException e) {
      // Turn the exception into unchecked so we can find out about it at runtime, but
      // don't need to add lots of boilerplate code everywhere.
      throwException(e);
      return "unknown";  // we will never reach here.
    }
  }

  @Override
  public UTF8String clone() {
    return fromBytes(getBytes());
  }

  @Override
  public int compareTo(final UTF8String other) {
    int len = numBytes < other.numBytes ? numBytes : other.numBytes;
    int i = 0;
    // TODO: compare 8 bytes as unsigned long
    while (i < len) {
      int res = (getByte(i) & 0xFF) - (other.getByte(i) & 0xFF);
      if (res != 0) {
        return res;
      }
      i += 1;
    }
    return numBytes - other.numBytes;
  }

  public int compare(final UTF8String other) {
    return compareTo(other);
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof UTF8String) {
      UTF8String o = (UTF8String) other;
      if (numBytes != o.numBytes){
        return false;
      }
      return ByteArrayMethods.arrayEquals(base, offset, o.base, o.offset, numBytes);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int result = 1;
    for (int i = 0; i < numBytes; i ++) {
      result = 31 * result + getByte(i);
    }
    return result;
  }
}

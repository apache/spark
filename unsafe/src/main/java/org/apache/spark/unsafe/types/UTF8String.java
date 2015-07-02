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
  private final int size;

  private static int[] bytesOfCodePointInUTF8 = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6, 6, 6};

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
    this.size = size;
  }

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  public int numBytes(final byte b) {
    final int offset = (b & 0xFF) - 192;
    return (offset >= 0) ? bytesOfCodePointInUTF8[offset] : 1;
  }

  /**
   * Returns the number of code points in it.
   *
   * This is only used by Substring() when `start` is negative.
   */
  public int length() {
    int len = 0;
    for (int i = 0; i < size; i += numBytes(getByte(i))) {
      len += 1;
    }
    return len;
  }

  public byte[] getBytes() {
    byte[] bytes = new byte[size];
    copyMemory(base, offset, bytes, BYTE_ARRAY_OFFSET, size);
    return bytes;
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  public UTF8String substring(final int start, final int until) {
    if (until <= start || start >= size) {
      return UTF8String.fromBytes(new byte[0]);
    }

    int i = 0;
    int c = 0;
    for (; i < size && c < start; i += numBytes(getByte(i))) {
      c += 1;
    }

    int j = i;
    for (; j < size && c < until; j += numBytes(getByte(i))) {
      c += 1;
    }

    byte[] bytes = new byte[j - i];
    copyMemory(base, offset + i, bytes, BYTE_ARRAY_OFFSET, j - i);
    return UTF8String.fromBytes(bytes);
  }

  public boolean contains(final UTF8String substring) {
    if (substring.size == 0) {
      return true;
    }

    byte first = substring.getByte(0);
    for (int i = 0; i <= size - substring.size; i++) {
      if (getByte(i) == first && matchAt(substring, i)) {
        return true;
      }
    }
    return false;
  }

  private long getLong(int i) {
    return UNSAFE.getLong(base, offset + i);
  }

  private byte getByte(int i) {
    return UNSAFE.getByte(base, offset + i);
  }

  private boolean matchAt(final UTF8String s, int pos) {
    if (s.size + pos > size || pos < 0) {
      return false;
    }

    int i = 0;
    while (i <= s.size - 8) {
      if (getLong(pos + i) != s.getLong(i)) {
        return false;
      }
      i += 8;
    }
    while (i < s.size) {
      if (getByte(pos + i) != s.getByte(i)) {
        return false;
      }
      i += 1;
    }
    return true;
  }

  public boolean startsWith(final UTF8String prefix) {
    return matchAt(prefix, 0);
  }

  public boolean endsWith(final UTF8String suffix) {
    return matchAt(suffix, size - suffix.size);
  }

  public UTF8String toUpperCase() {
    // this is locale aware
    return UTF8String.fromString(toString().toUpperCase());
  }

  public UTF8String toLowerCase() {
    // this is locale aware
    return UTF8String.fromString(toString().toLowerCase());
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
    return UTF8String.fromBytes(getBytes());
  }

  @Override
  public int compareTo(final UTF8String other) {
    int len = size < other.size ? size : other.size;
    int i = 0;
    while (i <= len - 8) {
      long a = getLong(i);
      long b = other.getLong(i);
      // a - b will overflow
      if (a != b) {
        return a < b ? -1 : 1;
      }
      i += 8;
    }
    while (i < len) {
      int res = getByte(i) - other.getByte(i);
      if (res != 0) {
        return res;
      }
      i += 1;
    }
    return size - other.size;
  }

  public int compare(final UTF8String other) {
    return compareTo(other);
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof UTF8String) {
      return compareTo((UTF8String) other) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    long result = 1;
    int i = 0;
    while (i <= size - 8) {
      result = result * 31 + getLong(i);
      i += 8;
    }
    while (i < size) {
      result = 31 * result + getByte(i);
      i += 1;
    }
    return (int) ((result >> 32) ^ result);
  }
}

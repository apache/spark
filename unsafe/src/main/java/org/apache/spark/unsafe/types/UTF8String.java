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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import javax.annotation.Nullable;

import org.apache.spark.unsafe.PlatformDependent;

/**
 * A UTF-8 String for internal Spark use.
 * <p>
 * A String encoded in UTF-8 as an Array[Byte], which can be used for comparison,
 * search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 * <p>
 * Note: This is not designed for general use cases, should not be used outside SQL.
 */
public final class UTF8String implements Comparable<UTF8String>, Serializable {

  @Nullable
  private byte[] bytes;

  private static int[] bytesOfCodePointInUTF8 = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    5, 5, 5, 5,
    6, 6, 6, 6};

  public static UTF8String fromBytes(byte[] bytes) {
    return (bytes != null) ? new UTF8String().set(bytes) : null;
  }

  public static UTF8String fromString(String str) {
    return (str != null) ? new UTF8String().set(str) : null;
  }

  /**
   * Updates the UTF8String with String.
   */
  public UTF8String set(final String str) {
    try {
      bytes = str.getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      // Turn the exception into unchecked so we can find out about it at runtime, but
      // don't need to add lots of boilerplate code everywhere.
      PlatformDependent.throwException(e);
    }
    return this;
  }

  /**
   * Updates the UTF8String with byte[], which should be encoded in UTF-8.
   */
  public UTF8String set(final byte[] bytes) {
    this.bytes = bytes;
    return this;
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
    for (int i = 0; i < bytes.length; i+= numBytes(bytes[i])) {
      len += 1;
    }
    return len;
  }

  public byte[] getBytes() {
    return bytes;
  }

  /**
   * Returns a substring of this.
   * @param start the position of first code point
   * @param until the position after last code point, exclusive.
   */
  public UTF8String substring(final int start, final int until) {
    if (until <= start || start >= bytes.length) {
      return UTF8String.fromBytes(new byte[0]);
    }

    int i = 0;
    int c = 0;
    for (; i < bytes.length && c < start; i += numBytes(bytes[i])) {
      c += 1;
    }

    int j = i;
    for (; j < bytes.length && c < until; j += numBytes(bytes[i])) {
      c += 1;
    }

    return UTF8String.fromBytes(Arrays.copyOfRange(bytes, i, j));
  }

  public boolean contains(final UTF8String substring) {
    if (substring == null) return false;
    final byte[] b = substring.getBytes();
    if (b.length == 0) {
      return true;
    }

    for (int i = 0; i <= bytes.length - b.length; i++) {
      if (bytes[i] == b[0] && startsWith(b, i)) {
        return true;
      }
    }
    return false;
  }

  private boolean startsWith(final byte[] prefix, int offset) {
    if (prefix.length + offset > bytes.length || offset < 0) {
      return false;
    }
    int i = 0;
    while (i < prefix.length && prefix[i] == bytes[i + offset]) {
      i++;
    }
    return i == prefix.length;
  }

  public boolean startsWith(final UTF8String prefix) {
    return prefix != null && startsWith(prefix.getBytes(), 0);
  }

  public boolean endsWith(final UTF8String suffix) {
    return suffix != null && startsWith(suffix.getBytes(), bytes.length - suffix.getBytes().length);
  }

  public UTF8String toUpperCase() {
    return UTF8String.fromString(toString().toUpperCase());
  }

  public UTF8String toLowerCase() {
    return UTF8String.fromString(toString().toLowerCase());
  }

  @Override
  public String toString() {
    try {
      return new String(bytes, "utf-8");
    } catch (UnsupportedEncodingException e) {
      // Turn the exception into unchecked so we can find out about it at runtime, but
      // don't need to add lots of boilerplate code everywhere.
      PlatformDependent.throwException(e);
      return "unknown";  // we will never reach here.
    }
  }

  @Override
  public UTF8String clone() {
    return new UTF8String().set(bytes);
  }

  @Override
  public int compareTo(final UTF8String other) {
    final byte[] b = other.getBytes();
    for (int i = 0; i < bytes.length && i < b.length; i++) {
      int res = bytes[i] - b[i];
      if (res != 0) {
        return res;
      }
    }
    return bytes.length - b.length;
  }

  public int compare(final UTF8String other) {
    return compareTo(other);
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof UTF8String) {
      return Arrays.equals(bytes, ((UTF8String) other).getBytes());
    } else if (other instanceof String) {
      // Used only in unit tests.
      String s = (String) other;
      return bytes.length >= s.length() && length() == s.length() && toString().equals(s);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }
}

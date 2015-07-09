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
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET, bytes.length);
    } else {
      return null;
    }
  }

  /**
   * Creates an UTF8String from String.
   */
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

  protected UTF8String(Object base, long offset, int size) {
    this.base = base;
    this.offset = offset;
    this.numBytes = size;
  }

  /**
   * Returns the number of bytes for a code point with the first byte as `b`
   * @param b The first byte of a code point
   */
  private static int numBytesForFirstByte(final byte b) {
    final int offset = (b & 0xFF) - 192;
    return (offset >= 0) ? bytesOfCodePointInUTF8[offset] : 1;
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
      return fromBytes(new byte[0]);
    }

    int i = 0;
    int c = 0;
    while (i < numBytes && c < start) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    int j = i;
    while (i < numBytes && c < until) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    byte[] bytes = new byte[i - j];
    copyMemory(base, offset + j, bytes, BYTE_ARRAY_OFFSET, i - j);
    return fromBytes(bytes);
  }

  /**
   * Returns whether this contains `substring` or not.
   */
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

  /**
   * Returns the byte at position `i`.
   */
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

  /**
   * Returns the upper case of this string
   */
  public UTF8String toUpperCase() {
    return fromString(toString().toUpperCase());
  }

  /**
   * Returns the lower case of this string
   */
  public UTF8String toLowerCase() {
    return fromString(toString().toLowerCase());
  }

  @Override
  public String toString() {
    try {
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
    int len = Math.min(numBytes, other.numBytes);
    // TODO: compare 8 bytes as unsigned long
    for (int i = 0; i < len; i ++) {
      // In UTF-8, the byte should be unsigned, so we should compare them as unsigned int.
      int res = (getByte(i) & 0xFF) - (other.getByte(i) & 0xFF);
      if (res != 0) {
        return res;
      }
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

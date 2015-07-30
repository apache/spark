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
import java.util.Arrays;

import org.apache.spark.unsafe.PlatformDependent;
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
    6, 6};

  public static final UTF8String EMPTY_UTF8 = UTF8String.fromString("");

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
   * Creates an UTF8String from byte array, which should be encoded in UTF-8.
   *
   * Note: `bytes` will be hold by returned UTF8String.
   */
  public static UTF8String fromBytes(byte[] bytes, int offset, int numBytes) {
    if (bytes != null) {
      return new UTF8String(bytes, BYTE_ARRAY_OFFSET + offset, numBytes);
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

  /**
   * Creates an UTF8String that contains `length` spaces.
   */
  public static UTF8String blankString(int length) {
    byte[] spaces = new byte[length];
    Arrays.fill(spaces, (byte) ' ');
    return fromBytes(spaces);
  }

  protected UTF8String(Object base, long offset, int numBytes) {
    this.base = base;
    this.offset = offset;
    this.numBytes = numBytes;
  }

  /**
   * Writes the content of this string into a memory address, identified by an object and an offset.
   * The target memory address must already been allocated, and have enough space to hold all the
   * bytes in this string.
   */
  public void writeToMemory(Object target, long targetOffset) {
    PlatformDependent.copyMemory(
      base,
      offset,
      target,
      targetOffset,
      numBytes
    );
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
   */
  public int numChars() {
    int len = 0;
    for (int i = 0; i < numBytes; i += numBytesForFirstByte(getByte(i))) {
      len += 1;
    }
    return len;
  }

  /**
   * Returns a 64-bit integer that can be used as the prefix used in sorting.
   */
  public long getPrefix() {
    // Since JVMs are either 4-byte aligned or 8-byte aligned, we check the size of the string.
    // If size is 0, just return 0.
    // If size is between 0 and 4 (inclusive), assume data is 4-byte aligned under the hood and
    // use a getInt to fetch the prefix.
    // If size is greater than 4, assume we have at least 8 bytes of data to fetch.
    // After getting the data, we use a mask to mask out data that is not part of the string.
    long p;
    if (numBytes >= 8) {
      p = PlatformDependent.UNSAFE.getLong(base, offset);
    } else  if (numBytes > 4) {
      p = PlatformDependent.UNSAFE.getLong(base, offset);
      p = p & ((1L << numBytes * 8) - 1);
    } else if (numBytes > 0) {
      p = (long) PlatformDependent.UNSAFE.getInt(base, offset);
      p = p & ((1L << numBytes * 8) - 1);
    } else {
      p = 0;
    }
    p = java.lang.Long.reverseBytes(p);
    return p;
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

  public UTF8String substringSQL(int pos, int length) {
    // Information regarding the pos calculation:
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.
    int start = (pos > 0) ? pos -1 : ((pos < 0) ? numChars() + pos : 0);
    int end = (length == Integer.MAX_VALUE) ? Integer.MAX_VALUE : start + length;
    return substring(start, end);
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

  /**
   * Copy the bytes from the current UTF8String, and make a new UTF8String.
   * @param start the start position of the current UTF8String in bytes.
   * @param end the end position of the current UTF8String in bytes.
   * @return a new UTF8String in the position of [start, end] of current UTF8String bytes.
   */
  private UTF8String copyUTF8String(int start, int end) {
    int len = end - start + 1;
    byte[] newBytes = new byte[len];
    copyMemory(base, offset + start, newBytes, BYTE_ARRAY_OFFSET, len);
    return UTF8String.fromBytes(newBytes);
  }

  public UTF8String trim() {
    int s = 0;
    int e = this.numBytes - 1;
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) == 0x20) s++;
    // skip all of the space (0x20) in the right side
    while (e >= 0 && getByte(e) == 0x20) e--;

    if (s > e) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(s, e);
    }
  }

  public UTF8String trimLeft() {
    int s = 0;
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) == 0x20) s++;
    if (s == this.numBytes) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(s, this.numBytes - 1);
    }
  }

  public UTF8String trimRight() {
    int e = numBytes - 1;
    // skip all of the space (0x20) in the right side
    while (e >= 0 && getByte(e) == 0x20) e--;

    if (e < 0) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(0, e);
    }
  }

  public UTF8String reverse() {
    byte[] result = new byte[this.numBytes];

    int i = 0; // position in byte
    while (i < numBytes) {
      int len = numBytesForFirstByte(getByte(i));
      copyMemory(this.base, this.offset + i, result,
              BYTE_ARRAY_OFFSET + result.length - i - len, len);

      i += len;
    }

    return UTF8String.fromBytes(result);
  }

  public UTF8String repeat(int times) {
    if (times <=0) {
      return EMPTY_UTF8;
    }

    byte[] newBytes = new byte[numBytes * times];
    copyMemory(this.base, this.offset, newBytes, BYTE_ARRAY_OFFSET, numBytes);

    int copied = 1;
    while (copied < times) {
      int toCopy = Math.min(copied, times - copied);
      System.arraycopy(newBytes, 0, newBytes, copied * numBytes, numBytes * toCopy);
      copied += toCopy;
    }

    return UTF8String.fromBytes(newBytes);
  }

  /**
   * Returns the position of the first occurrence of substr in
   * current string from the specified position (0-based index).
   *
   * @param v the string to be searched
   * @param start the start position of the current string for searching
   * @return the position of the first occurrence of substr, if not found, -1 returned.
   */
  public int indexOf(UTF8String v, int start) {
    if (v.numBytes() == 0) {
      return 0;
    }

    // locate to the start position.
    int i = 0; // position in byte
    int c = 0; // position in character
    while (i < numBytes && c < start) {
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    }

    do {
      if (i + v.numBytes > numBytes) {
        return -1;
      }
      if (ByteArrayMethods.arrayEquals(base, offset + i, v.base, v.offset, v.numBytes)) {
        return c;
      }
      i += numBytesForFirstByte(getByte(i));
      c += 1;
    } while (i < numBytes);

    return -1;
  }

  /**
   * Returns str, right-padded with pad to a length of len
   * For example:
   *   ('hi', 5, '??') =&gt; 'hi???'
   *   ('hi', 1, '??') =&gt; 'h'
   */
  public UTF8String rpad(int len, UTF8String pad) {
    int spaces = len - this.numChars(); // number of char need to pad
    if (spaces <= 0) {
      // no padding at all, return the substring of the current string
      return substring(0, len);
    } else {
      int padChars = pad.numChars();
      int count = spaces / padChars; // how many padding string needed
      // the partial string of the padding
      UTF8String remain = pad.substring(0, spaces - padChars * count);

      byte[] data = new byte[this.numBytes + pad.numBytes * count + remain.numBytes];
      copyMemory(this.base, this.offset, data, BYTE_ARRAY_OFFSET, this.numBytes);
      int offset = this.numBytes;
      int idx = 0;
      while (idx < count) {
        copyMemory(pad.base, pad.offset, data, BYTE_ARRAY_OFFSET + offset, pad.numBytes);
        ++idx;
        offset += pad.numBytes;
      }
      copyMemory(remain.base, remain.offset, data, BYTE_ARRAY_OFFSET + offset, remain.numBytes);

      return UTF8String.fromBytes(data);
    }
  }

  /**
   * Returns str, left-padded with pad to a length of len.
   * For example:
   *   ('hi', 5, '??') =&gt; '???hi'
   *   ('hi', 1, '??') =&gt; 'h'
   */
  public UTF8String lpad(int len, UTF8String pad) {
    int spaces = len - this.numChars(); // number of char need to pad
    if (spaces <= 0) {
      // no padding at all, return the substring of the current string
      return substring(0, len);
    } else {
      int padChars = pad.numChars();
      int count = spaces / padChars; // how many padding string needed
      // the partial string of the padding
      UTF8String remain = pad.substring(0, spaces - padChars * count);

      byte[] data = new byte[this.numBytes + pad.numBytes * count + remain.numBytes];

      int offset = 0;
      int idx = 0;
      while (idx < count) {
        copyMemory(pad.base, pad.offset, data, BYTE_ARRAY_OFFSET + offset, pad.numBytes);
        ++idx;
        offset += pad.numBytes;
      }
      copyMemory(remain.base, remain.offset, data, BYTE_ARRAY_OFFSET + offset, remain.numBytes);
      offset += remain.numBytes;
      copyMemory(this.base, this.offset, data, BYTE_ARRAY_OFFSET + offset, numBytes());

      return UTF8String.fromBytes(data);
    }
  }

  /**
   * Concatenates input strings together into a single string. Returns null if any input is null.
   */
  public static UTF8String concat(UTF8String... inputs) {
    // Compute the total length of the result.
    int totalLength = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        totalLength += inputs[i].numBytes;
      } else {
        return null;
      }
    }

    // Allocate a new byte array, and copy the inputs one by one into it.
    final byte[] result = new byte[totalLength];
    int offset = 0;
    for (int i = 0; i < inputs.length; i++) {
      int len = inputs[i].numBytes;
      copyMemory(
        inputs[i].base, inputs[i].offset,
        result, BYTE_ARRAY_OFFSET + offset,
        len);
      offset += len;
    }
    return fromBytes(result);
  }

  /**
   * Concatenates input strings together into a single string using the separator.
   * A null input is skipped. For example, concat(",", "a", null, "c") would yield "a,c".
   */
  public static UTF8String concatWs(UTF8String separator, UTF8String... inputs) {
    if (separator == null) {
      return null;
    }

    int numInputBytes = 0;  // total number of bytes from the inputs
    int numInputs = 0;      // number of non-null inputs
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        numInputBytes += inputs[i].numBytes;
        numInputs++;
      }
    }

    if (numInputs == 0) {
      // Return an empty string if there is no input, or all the inputs are null.
      return fromBytes(new byte[0]);
    }

    // Allocate a new byte array, and copy the inputs one by one into it.
    // The size of the new array is the size of all inputs, plus the separators.
    final byte[] result = new byte[numInputBytes + (numInputs - 1) * separator.numBytes];
    int offset = 0;

    for (int i = 0, j = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        int len = inputs[i].numBytes;
        copyMemory(
          inputs[i].base, inputs[i].offset,
          result, PlatformDependent.BYTE_ARRAY_OFFSET + offset,
          len);
        offset += len;

        j++;
        // Add separator if this is not the last input.
        if (j < numInputs) {
          copyMemory(
            separator.base, separator.offset,
            result, PlatformDependent.BYTE_ARRAY_OFFSET + offset,
            separator.numBytes);
          offset += separator.numBytes;
        }
      }
    }
    return fromBytes(result);
  }

  public UTF8String[] split(UTF8String pattern, int limit) {
    String[] splits = toString().split(pattern.toString(), limit);
    UTF8String[] res = new UTF8String[splits.length];
    for (int i = 0; i < res.length; i++) {
      res[i] = fromString(splits[i]);
    }
    return res;
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
  public int compareTo(@Nonnull final UTF8String other) {
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
      if (numBytes != o.numBytes) {
        return false;
      }
      return ByteArrayMethods.arrayEquals(base, offset, o.base, o.offset, numBytes);
    } else {
      return false;
    }
  }

  /**
   * Levenshtein distance is a metric for measuring the distance of two strings. The distance is
   * defined by the minimum number of single-character edits (i.e. insertions, deletions or
   * substitutions) that are required to change one of the strings into the other.
   */
  public int levenshteinDistance(UTF8String other) {
    // Implementation adopted from org.apache.common.lang3.StringUtils.getLevenshteinDistance

    int n = numChars();
    int m = other.numChars();

    if (n == 0) {
      return m;
    } else if (m == 0) {
      return n;
    }

    UTF8String s, t;

    if (n <= m) {
      s = this;
      t = other;
    } else {
      s = other;
      t = this;
      int swap;
      swap = n;
      n = m;
      m = swap;
    }

    int p[] = new int[n + 1];
    int d[] = new int[n + 1];
    int swap[];

    int i, i_bytes, j, j_bytes, num_bytes_j, cost;

    for (i = 0; i <= n; i++) {
      p[i] = i;
    }

    for (j = 0, j_bytes = 0; j < m; j_bytes += num_bytes_j, j++) {
      num_bytes_j = numBytesForFirstByte(t.getByte(j_bytes));
      d[0] = j + 1;

      for (i = 0, i_bytes = 0; i < n; i_bytes += numBytesForFirstByte(s.getByte(i_bytes)), i++) {
        if (s.getByte(i_bytes) != t.getByte(j_bytes) ||
              num_bytes_j != numBytesForFirstByte(s.getByte(i_bytes))) {
          cost = 1;
        } else {
          cost = (ByteArrayMethods.arrayEquals(t.base, t.offset + j_bytes, s.base,
              s.offset + i_bytes, num_bytes_j)) ? 0 : 1;
        }
        d[i + 1] = Math.min(Math.min(d[i] + 1, p[i + 1] + 1), p[i] + cost);
      }

      swap = p;
      p = d;
      d = swap;
    }

    return p[n];
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

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

package org.apache.spark.unsafe;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write {@link UTF8String}s to an internal buffer and build the concatenated
 * {@link UTF8String} at the end.
 */
public class UTF8StringBuilder {

  private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  private byte[] buffer;
  private int cursor = Platform.BYTE_ARRAY_OFFSET;

  public UTF8StringBuilder() {
    // Since initial buffer size is 16 in `StringBuilder`, we set the same size here
    this(16);
  }

  public UTF8StringBuilder(int initialSize) {
    if (initialSize < 0) {
      throw new IllegalArgumentException("Size must be non-negative");
    }
    if (initialSize > ARRAY_MAX) {
      throw new IllegalArgumentException(
        "Size " + initialSize + " exceeded maximum size of " + ARRAY_MAX);
    }
    this.buffer = new byte[initialSize];
  }

  // Grows the buffer by at least `neededSize`
  private void grow(int neededSize) {
    if (neededSize > ARRAY_MAX - totalSize()) {
      throw new UnsupportedOperationException(
        "Cannot grow internal buffer by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX);
    }
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      final byte[] tmp = new byte[newLength];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
    }
  }

  private int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }

  public void append(UTF8String value) {
    grow(value.numBytes());
    value.writeToMemory(buffer, cursor);
    cursor += value.numBytes();
  }

  public void append(String value) {
    append(UTF8String.fromString(value));
  }

  public void appendBytes(Object base, long offset, int length) {
    grow(length);
    Platform.copyMemory(
      base,
      offset,
      buffer,
      cursor,
      length);
    cursor += length;
  }

  public void appendByte(byte singleByte) {
    grow(1);
    buffer[cursor - Platform.BYTE_ARRAY_OFFSET] = singleByte;
    cursor++;
  }

  public UTF8String build() {
    return UTF8String.fromBytes(buffer, 0, totalSize());
  }

  public void appendCodePoint(int codePoint) {
    if (codePoint <= 0x7F) {
      appendByte((byte) codePoint);
    }
    else if (codePoint <= 0x7FF) {
      appendByte((byte) (0xC0 | (codePoint >> 6)));
      appendByte((byte) (0x80 | (codePoint & 0x3F)));
    }
    else if (codePoint <= 0xFFFF) {
      appendByte((byte) (0xE0 | (codePoint >> 12)));
      appendByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
      appendByte((byte) (0x80 | (codePoint & 0x3F)));
    }
    else if (codePoint <= 0x10FFFF) {
      appendByte((byte) (0xF0 | (codePoint >> 18)));
      appendByte((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
      appendByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
      appendByte((byte) (0x80 | (codePoint & 0x3F)));
    }
    else {
      throw new IllegalArgumentException("Invalid Unicode codePoint: " + codePoint);
    }
  }

}

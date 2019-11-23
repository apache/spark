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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * A helper class to manage the data buffer for an unsafe row.  The data buffer can grow and
 * automatically re-point the unsafe row to it.
 *
 * This class can be used to build a one-pass unsafe row writing program, i.e. data will be written
 * to the data buffer directly and no extra copy is needed.  There should be only one instance of
 * this class per writing program, so that the memory segment/data buffer can be reused.  Note that
 * for each incoming record, we should call `reset` of BufferHolder instance before write the record
 * and reuse the data buffer.
 */
final class BufferHolder {

  private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  // buffer is guarantee to be word-aligned since UnsafeRow assumes each field is word-aligned.
  private byte[] buffer;
  private int cursor = Platform.BYTE_ARRAY_OFFSET;
  private final UnsafeRow row;
  private final int fixedSize;

  BufferHolder(UnsafeRow row) {
    this(row, 64);
  }

  BufferHolder(UnsafeRow row, int initialSize) {
    int bitsetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields());
    if (row.numFields() > (ARRAY_MAX - initialSize - bitsetWidthInBytes) / 8) {
      throw new UnsupportedOperationException(
        "Cannot create BufferHolder for input UnsafeRow because there are " +
          "too many fields (number of fields: " + row.numFields() + ")");
    }
    this.fixedSize = bitsetWidthInBytes + 8 * row.numFields();
    int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(fixedSize + initialSize);
    this.buffer = new byte[roundedSize];
    this.row = row;
    this.row.pointTo(buffer, buffer.length);
  }

  /**
   * Grows the buffer by at least neededSize and points the row to the buffer.
   */
  void grow(int neededSize) {
    if (neededSize < 0) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + neededSize + " because the size is negative");
    }
    if (neededSize > ARRAY_MAX - totalSize()) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX);
    }
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      // This will not happen frequently, because the buffer is re-used.
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(newLength);
      final byte[] tmp = new byte[roundedSize];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
      row.pointTo(buffer, buffer.length);
    }
  }

  byte[] getBuffer() {
    return buffer;
  }

  int getCursor() {
    return cursor;
  }

  void increaseCursor(int val) {
    cursor += val;
  }

  void reset() {
    cursor = Platform.BYTE_ARRAY_OFFSET + fixedSize;
  }

  int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}

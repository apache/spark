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

/**
 * A helper class to manage the data buffer for an unsafe row.  The data buffer can grow and
 * automatically re-point the unsafe row to it.
 *
 * This class can be used to build a one-pass unsafe row writing program, i.e. data will be written
 * to the data buffer directly and no extra copy is needed.  There should be only one instance of
 * this class per writing program, so that the memory segment/data buffer can be reused.  Note that
 * for each incoming record, we should call `reset` of BufferHolder instance before write the record
 * and reuse the data buffer.
 *
 * Generally we should call `UnsafeRow.setTotalSize` and pass in `BufferHolder.totalSize` to update
 * the size of the result row, after writing a record to the buffer. However, we can skip this step
 * if the fields of row are all fixed-length, as the size of result row is also fixed.
 */
public class BufferHolder {
  public byte[] buffer;
  public int cursor = Platform.BYTE_ARRAY_OFFSET;
  private final UnsafeRow row;
  private final int fixedSize;

  public BufferHolder(UnsafeRow row) {
    this(row, 64);
  }

  public BufferHolder(UnsafeRow row, int initialSize) {
    int bitsetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields());
    if (row.numFields() > (Integer.MAX_VALUE - initialSize - bitsetWidthInBytes) / 8) {
      throw new UnsupportedOperationException(
        "Cannot create BufferHolder for input UnsafeRow because there are " +
          "too many fields (number of fields: " + row.numFields() + ")");
    }
    this.fixedSize = bitsetWidthInBytes + 8 * row.numFields();
    this.buffer = new byte[fixedSize + initialSize];
    this.row = row;
    this.row.pointTo(buffer, buffer.length);
  }

  /**
   * Grows the buffer by at least neededSize and points the row to the buffer.
   */
  public void grow(int neededSize) {
    if (neededSize > Integer.MAX_VALUE - totalSize()) {
      throw new UnsupportedOperationException(
        "Cannot grow BufferHolder by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + Integer.MAX_VALUE);
    }
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      // This will not happen frequently, because the buffer is re-used.
      int newLength = length < Integer.MAX_VALUE / 2 ? length * 2 : Integer.MAX_VALUE;
      final byte[] tmp = new byte[newLength];
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

  public void reset() {
    cursor = Platform.BYTE_ARRAY_OFFSET + fixedSize;
  }

  public int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}

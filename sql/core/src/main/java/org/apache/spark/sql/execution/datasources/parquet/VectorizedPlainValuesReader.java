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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;

import org.apache.commons.lang.NotImplementedException;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private byte[] buffer;
  private int offset;
  private int bitOffset; // Only used for booleans.

  public VectorizedPlainValuesReader() {
  }

  @Override
  public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
    this.buffer = bytes;
    this.offset = offset + Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final void readBooleans(int total, ColumnVector c, int rowId) {
    // TODO: properly vectorize this
    for (int i = 0; i < total; i++) {
      c.putBoolean(rowId + i, readBoolean());
    }
  }

  @Override
  public final void readIntegers(int total, ColumnVector c, int rowId) {
    c.putIntsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public final void readLongs(int total, ColumnVector c, int rowId) {
    c.putLongsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 8 * total;
  }

  @Override
  public final void readFloats(int total, ColumnVector c, int rowId) {
    c.putFloats(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public final void readDoubles(int total, ColumnVector c, int rowId) {
    c.putDoubles(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 8 * total;
  }

  @Override
  public final void readBytes(int total, ColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      // Bytes are stored as a 4-byte little endian int. Just read the first byte.
      // TODO: consider pushing this in ColumnVector by adding a readBytes with a stride.
      c.putInt(rowId + i, buffer[offset]);
      offset += 4;
    }
  }

  @Override
  public final boolean readBoolean() {
    byte b = Platform.getByte(buffer, offset);
    boolean v = (b & (1 << bitOffset)) != 0;
    bitOffset += 1;
    if (bitOffset == 8) {
      bitOffset = 0;
      offset++;
    }
    return v;
  }

  @Override
  public final int readInteger() {
    int v = Platform.getInt(buffer, offset);
    offset += 4;
    return v;
  }

  @Override
  public final long readLong() {
    long v = Platform.getLong(buffer, offset);
    offset += 8;
    return v;
  }

  @Override
  public final byte readByte() {
    return (byte)readInteger();
  }

  @Override
  public final float readFloat() {
    float v = Platform.getFloat(buffer, offset);
    offset += 4;
    return v;
  }

  @Override
  public final double readDouble() {
    double v = Platform.getDouble(buffer, offset);
    offset += 8;
    return v;
  }

  @Override
  public final void readBinary(int total, ColumnVector v, int rowId) {
    for (int i = 0; i < total; i++) {
      int len = readInteger();
      int start = offset;
      offset += len;
      v.putByteArray(rowId + i, buffer, start - Platform.BYTE_ARRAY_OFFSET, len);
    }
  }

  @Override
  public final Binary readBinary(int len) {
    Binary result = Binary.fromByteArray(buffer, offset - Platform.BYTE_ARRAY_OFFSET, len);
    offset += len;
    return result;
  }
}

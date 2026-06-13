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
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * Vectorized reader for the Parquet BYTE_STREAM_SPLIT encoding.
 *
 * <p>BYTE_STREAM_SPLIT de-interleaves the bytes of N fixed-width values into W
 * separate "streams", one per byte position. For example, N FLOAT values (W=4)
 * are stored as:
 * <pre>
 *   stream 0: byte 0 of value 0, byte 0 of value 1, ..., byte 0 of value N-1
 *   stream 1: byte 1 of value 0, byte 1 of value 1, ..., byte 1 of value N-1
 *   stream 2: byte 2 of value 0, byte 2 of value 1, ..., byte 2 of value N-1
 *   stream 3: byte 3 of value 0, byte 3 of value 1, ..., byte 3 of value N-1
 * </pre>
 *
 * <p>This makes each stream highly compressible for time-series and scientific
 * data (adjacent values share high-order bytes). Decoding gathers the original
 * bytes back: {@code value[i] = {stream[0][i], stream[1][i], ..., stream[W-1][i]}}.
 *
 * <p>Supports FLOAT (W=4), DOUBLE (W=8), INT32 (W=4), INT64 (W=8), and
 * FIXED_LEN_BYTE_ARRAY (W=type length).
 */
public class VectorizedByteStreamSplitValuesReader
    extends VectorizedReaderBase {

  /** Width of each value in bytes (4 for FLOAT/INT32, 8 for DOUBLE/INT64). */
  private final int typeWidth;

  /** Total number of values in the current page. */
  private int valueCount;

  /** Raw encoded page data: W streams of valueCount bytes each. */
  private byte[] pageData;

  /** Current read position (number of values consumed so far). */
  private int offset;

  public VectorizedByteStreamSplitValuesReader(int typeWidth) {
    this.typeWidth = typeWidth;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    // For nullable columns the page data section contains only non-null values,
    // but valueCount includes nulls. Use the actual bytes available in the stream
    // to derive the real number of encoded values.
    int totalBytes = in.available();
    this.valueCount = totalBytes / typeWidth;
    this.offset = 0;
    this.pageData = new byte[totalBytes];
    // Read the entire page into pageData. ByteBufferInputStream.slice() returns a
    // single ByteBuffer of exactly the requested length, copying if the data spans
    // multiple internal buffers.
    ByteBuffer buf = in.slice(totalBytes);
    buf.get(pageData, 0, totalBytes);
  }

  // --------------- helpers ---------------

  /** Assembles a single 4-byte little-endian int from 4 streams at the given index. */
  private int assembleInt(int idx) {
    return (pageData[idx] & 0xFF)
         | ((pageData[valueCount + idx] & 0xFF) << 8)
         | ((pageData[2 * valueCount + idx] & 0xFF) << 16)
         | ((pageData[3 * valueCount + idx] & 0xFF) << 24);
  }

  /** Assembles a single 8-byte little-endian long from 8 streams at the given index. */
  private long assembleLong(int idx) {
    return (pageData[idx] & 0xFFL)
         | ((pageData[valueCount + idx] & 0xFFL) << 8)
         | ((pageData[2 * valueCount + idx] & 0xFFL) << 16)
         | ((pageData[3 * valueCount + idx] & 0xFFL) << 24)
         | ((pageData[4 * valueCount + idx] & 0xFFL) << 32)
         | ((pageData[5 * valueCount + idx] & 0xFFL) << 40)
         | ((pageData[6 * valueCount + idx] & 0xFFL) << 48)
         | ((pageData[7 * valueCount + idx] & 0xFFL) << 56);
  }

  // --------------- single-value reads ---------------

  @Override
  public byte readByte() {
    return (byte) readInteger();
  }

  @Override
  public short readShort() {
    return (short) readInteger();
  }

  @Override
  public int readInteger() {
    return assembleInt(offset++);
  }

  @Override
  public long readLong() {
    return assembleLong(offset++);
  }

  @Override
  public float readFloat() {
    return Float.intBitsToFloat(assembleInt(offset++));
  }

  @Override
  public double readDouble() {
    return Double.longBitsToDouble(assembleLong(offset++));
  }

  @Override
  public Binary readBinary(int len) {
    byte[] result = new byte[len];
    for (int b = 0; b < len; b++) {
      result[b] = pageData[b * valueCount + offset];
    }
    offset++;
    return Binary.fromConstantByteArray(result);
  }

  // --------------- batch reads ---------------

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putByte(rowId + i, (byte) assembleInt(offset + i));
    }
    offset += total;
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putShort(rowId + i, (short) assembleInt(offset + i));
    }
    offset += total;
  }

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putInt(rowId + i, assembleInt(offset + i));
    }
    offset += total;
  }

  @Override
  public void readIntegersAsLongs(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putLong(rowId + i, assembleInt(offset + i));
    }
    offset += total;
  }

  @Override
  public void readIntegersAsDoubles(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putDouble(rowId + i, (double) assembleInt(offset + i));
    }
    offset += total;
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putLong(rowId + i, assembleLong(offset + i));
    }
    offset += total;
  }

  @Override
  public void readLongsAsInts(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putInt(rowId + i, (int) assembleLong(offset + i));
    }
    offset += total;
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putFloat(rowId + i, Float.intBitsToFloat(assembleInt(offset + i)));
    }
    offset += total;
  }

  @Override
  public void readFloatsAsDoubles(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putDouble(rowId + i, (double) Float.intBitsToFloat(assembleInt(offset + i)));
    }
    offset += total;
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      c.putDouble(rowId + i, Double.longBitsToDouble(assembleLong(offset + i)));
    }
    offset += total;
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    // Reuse a single scratch buffer to avoid per-value byte[] + Binary allocation.
    byte[] scratch = new byte[typeWidth];
    for (int i = 0; i < total; i++) {
      for (int b = 0; b < typeWidth; b++) {
        scratch[b] = pageData[b * valueCount + offset];
      }
      c.putByteArray(rowId + i, scratch, 0, typeWidth);
      offset++;
    }
  }

  // --------------- skip methods ---------------
  // All types share the same page layout, so skipping is just advancing the offset.
  // skipBooleans is not overridden: BSS never encodes booleans; the base class throws.

  @Override
  public void skipBytes(int total) { offset += total; }

  @Override
  public void skipShorts(int total) { offset += total; }

  @Override
  public void skipIntegers(int total) { offset += total; }

  @Override
  public void skipLongs(int total) { offset += total; }

  @Override
  public void skipFloats(int total) { offset += total; }

  @Override
  public void skipDoubles(int total) { offset += total; }

  @Override
  public void skipBinary(int total) { offset += total; }

  @Override
  public void skipFixedLenByteArray(int total, int len) { offset += total; }
}

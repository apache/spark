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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * Base class for implementations of VectorizedValuesReader. Mainly to avoid duplication
 * of methods that are not supported by concrete implementations
 */
public class VectorizedReaderBase extends ValuesReader implements VectorizedValuesReader {

  /**
   * Encodes an unsigned long as a minimal big-endian two's-complement byte array
   * compatible with {@link java.math.BigInteger} encoding. The result is written into
   * the backing array of {@code buf} (which must have capacity >= 9). Returns the
   * start offset; the valid bytes are {@code buf.array()[start .. 8]} (length = 9 - start).
   *
   * <p>This avoids the per-value overhead of
   * {@code new BigInteger(Long.toUnsignedString(v)).toByteArray()} which allocates a
   * String, a BigInteger, and a byte[] on every call.
   */
  static int encodeUnsignedLongBigEndian(long v, ByteBuffer buf) {
    byte[] scratch = buf.array();
    // ByteBuffer is big-endian by default; writes 8 bytes MSB-first at offset 1.
    // Always write before the zero-check so that the buffer is current even when reused.
    buf.putLong(1, v);
    if (v == 0L) {
      return 8;  // scratch[8] is already 0x00; caller writes 9 - 8 = 1 byte: [0x00]
    }
    // Find the first non-zero byte (minimal encoding).
    int start = 1;
    while (scratch[start] == 0) start++;
    // Prepend 0x00 sign byte if MSB of the first significant byte is set.
    if ((scratch[start] & 0x80) != 0) scratch[--start] = 0;
    return start;
  }

  /**
   * Convenience: encodes an unsigned long and returns a new byte[] with the minimal
   * BigInteger-compatible encoding. Use {@link #encodeUnsignedLongBigEndian(long, ByteBuffer)}
   * with a reusable buffer in hot paths to avoid allocation.
   */
  static byte[] unsignedLongToBytesBigEndian(long v) {
    ByteBuffer buf = ByteBuffer.allocate(9);
    int start = encodeUnsignedLongBigEndian(v, buf);
    return Arrays.copyOfRange(buf.array(), start, 9);
  }

  @Override
  public void skip() {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public byte readByte() {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public short readShort() {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public Binary readBinary(int len) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readIntegersWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readLongsWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase, String timeZone) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readGeometry(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void readGeography(int total, WritableColumnVector c, int rowId) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipBooleans(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipBytes(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipShorts(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipIntegers(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipLongs(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipFloats(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipDoubles(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipBinary(int total) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw SparkUnsupportedOperationException.apply();
  }

}

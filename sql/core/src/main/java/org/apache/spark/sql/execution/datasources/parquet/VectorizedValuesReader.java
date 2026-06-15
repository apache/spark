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

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import org.apache.parquet.io.api.Binary;

/**
 * Interface for value decoding that supports vectorized (aka batched) decoding.
 * TODO: merge this into parquet-mr.
 */
public interface VectorizedValuesReader {
  boolean readBoolean();
  byte readByte();
  short readShort();
  int readInteger();
  long readLong();
  float readFloat();
  double readDouble();
  Binary readBinary(int len);

  /*
   * Reads `total` values into `c` start at `c[rowId]`
   */
  void readBooleans(int total, WritableColumnVector c, int rowId);
  void readBytes(int total, WritableColumnVector c, int rowId);
  void readShorts(int total, WritableColumnVector c, int rowId);
  void readIntegers(int total, WritableColumnVector c, int rowId);
  void readIntegersWithRebase(int total, WritableColumnVector c, int rowId, boolean failIfRebase);
  void readUnsignedIntegers(int total, WritableColumnVector c, int rowId);
  void readUnsignedLongs(int total, WritableColumnVector c, int rowId);
  void readLongs(int total, WritableColumnVector c, int rowId);
  void readLongsWithRebase(
      int total,
      WritableColumnVector c,
      int rowId,
      boolean failIfRebase,
      String timeZone);
  void readFloats(int total, WritableColumnVector c, int rowId);
  void readDoubles(int total, WritableColumnVector c, int rowId);

  /**
   * Reads {@code total} INT32 values, sign-extends each to a long, and writes them into
   * {@code c} starting at {@code c[rowId]}. Used by type-converting updaters that read
   * parquet INT32 columns into Spark {@code LongType} (or wider decimal) targets.
   *
   * <p>The default implementation falls back to a per-row read+widen+write loop and is
   * therefore equivalent in cost to the legacy per-row Updater path. Subclasses backed
   * by contiguous bulk storage (e.g. PLAIN encoding via {@link VectorizedPlainValuesReader})
   * should override to read source bytes once and run a tight in-method conversion loop,
   * avoiding {@code total} virtual dispatches on {@link #readInteger()}. Readers without
   * an override preserve correctness but gain no speedup.
   */
  default void readIntegersAsLongs(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i += 1) {
      c.putLong(rowId + i, readInteger());
    }
  }

  /**
   * Reads {@code total} INT32 values, widens each to a double, and writes them into
   * {@code c} starting at {@code c[rowId]}. The widening is lossless because every
   * INT32 fits exactly in a double's 53-bit mantissa. Used by the type-converting
   * updater that reads parquet INT32 columns into Spark {@code DoubleType} targets.
   *
   * <p>The default implementation falls back to a per-row read+widen+write loop and is
   * therefore equivalent in cost to the legacy per-row Updater path. Subclasses backed
   * by contiguous bulk storage (e.g. PLAIN encoding via {@link VectorizedPlainValuesReader})
   * should override to read source bytes once and run a tight in-method conversion loop,
   * avoiding {@code total} virtual dispatches on {@link #readInteger()}. Readers without
   * an override preserve correctness but gain no speedup.
   */
  default void readIntegersAsDoubles(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i += 1) {
      c.putDouble(rowId + i, readInteger());
    }
  }

  /**
   * Reads {@code total} FLOAT values, widens each to a double, and writes them into
   * {@code c} starting at {@code c[rowId]}. The widening is Java's primitive
   * float-to-double conversion: exact for every finite and infinite float; a NaN
   * float widens to a double NaN (the payload may be canonicalized by the JVM).
   * Used by the type-converting updater that reads parquet FLOAT columns into
   * Spark {@code DoubleType} targets.
   *
   * <p>The default implementation falls back to a per-row read+widen+write loop and is
   * therefore equivalent in cost to the legacy per-row Updater path. Subclasses backed
   * by contiguous bulk storage (e.g. PLAIN encoding via {@link VectorizedPlainValuesReader})
   * should override to read source bytes once and run a tight in-method conversion loop,
   * avoiding {@code total} virtual dispatches on {@link #readFloat()}. Readers without
   * an override preserve correctness but gain no speedup.
   */
  default void readFloatsAsDoubles(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i += 1) {
      c.putDouble(rowId + i, readFloat());
    }
  }

  /**
   * Reads {@code total} INT64 values, narrows each to an int via Java's primitive
   * long-to-int cast (the high 32 bits are discarded), and writes them into {@code c}
   * starting at {@code c[rowId]}. Used by the type-converting updater that reads parquet
   * INT64 DECIMAL columns whose Spark target is a 32-bit decimal (precision <= 9); such
   * values are guaranteed by Parquet's decimal encoding to fit in int32, so the
   * narrowing is non-lossy in practice.
   *
   * <p>The default implementation falls back to a per-row read+narrow+write loop and is
   * therefore equivalent in cost to the legacy per-row Updater path. Subclasses backed
   * by contiguous bulk storage (e.g. PLAIN encoding via {@link VectorizedPlainValuesReader})
   * should override to read source bytes once and run a tight in-method conversion loop,
   * avoiding {@code total} virtual dispatches on {@link #readLong()}. Readers without
   * an override preserve correctness but gain no speedup.
   */
  default void readLongsAsInts(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i += 1) {
      c.putInt(rowId + i, (int) readLong());
    }
  }

  void readBinary(int total, WritableColumnVector c, int rowId);
  void readGeometry(int total, WritableColumnVector c, int rowId);
  void readGeography(int total, WritableColumnVector c, int rowId);

   /*
    * Skips `total` values
    */
   void skipBooleans(int total);
   void skipBytes(int total);
   void skipShorts(int total);
   void skipIntegers(int total);
   void skipLongs(int total);
   void skipFloats(int total);
   void skipDoubles(int total);
   void skipBinary(int total);
   void skipFixedLenByteArray(int total, int len);

  /**
   * A functional interface to write integer values to columnar output
   */
  @FunctionalInterface
  interface IntegerOutputWriter {

    /**
     * A functional interface that writes a long value to a specified row in an output column
     * vector
     *
     * @param outputColumnVector the vector to write to
     * @param rowId the row to write to
     * @param val value to write
     */
    void write(WritableColumnVector outputColumnVector, int rowId, long val);
  }

  @FunctionalInterface
  interface ByteBufferOutputWriter {
    void write(WritableColumnVector c, int rowId, ByteBuffer val, int length);

    static void writeArrayByteBuffer(WritableColumnVector c, int rowId, ByteBuffer val,
        int length) {
      c.putByteArray(rowId,
          val.array(),
          val.arrayOffset() + val.position(),
          length);
    }

    static void skipWrite(WritableColumnVector c, int rowId, ByteBuffer val, int length) { }
  }
}

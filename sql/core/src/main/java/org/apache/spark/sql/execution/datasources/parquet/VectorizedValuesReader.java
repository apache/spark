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
  void readBinary(int total, WritableColumnVector c, int rowId);

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

}

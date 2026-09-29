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

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * An implementation of the Parquet DELTA_LENGTH_BYTE_ARRAY decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaLengthByteArrayReader extends VectorizedReaderBase implements
    VectorizedValuesReader {

  private final VectorizedDeltaBinaryPackedReader lengthReader;
  private ByteBufferInputStream in;
  private WritableColumnVector lengthsVector;
  private int currentRow = 0;

  VectorizedDeltaLengthByteArrayReader() {
    lengthReader = new VectorizedDeltaBinaryPackedReader();
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    lengthsVector = new OnHeapColumnVector(valueCount, IntegerType);
    lengthReader.initFromPage(valueCount, in);
    int lengthCount = getTotalValueCount();
    if (lengthCount > valueCount) {
      throw new ParquetDecodingException("Corrupted DELTA_LENGTH_BYTE_ARRAY data: " +
          lengthCount + " value lengths in a page of " + valueCount + " values");
    }
    lengthReader.readIntegers(lengthCount, lengthsVector, 0);
    this.in = in.remainingStream();
    validateLengths();
  }

  /**
   * The value lengths are read from the file, so validate all of them once, before any of them
   * is used: a corrupt page must fail the read instead of producing malformed values. Values are
   * consumed in order, so once every length is non-negative and they add up to at most the bytes
   * left in the page, no read or skip of a value can run past the end of the page.
   */
  private void validateLengths() {
    int lengthCount = getTotalValueCount();
    long totalLength = 0;
    for (int i = 0; i < lengthCount; i++) {
      int length = lengthsVector.getInt(i);
      if (length < 0) {
        throw new ParquetDecodingException(
            "Corrupted DELTA_LENGTH_BYTE_ARRAY data: negative value length: " + length);
      }
      totalLength += length;
    }
    int available = in.available();
    if (totalLength > available) {
      throw new ParquetDecodingException("Corrupted DELTA_LENGTH_BYTE_ARRAY data: value " +
          "lengths add up to " + totalLength + " bytes, but only " + available + " are left");
    }
  }

  /**
   * True value count, i.e. the number of decoded value lengths. May be less than the
   * `valueCount` passed to `initFromPage` because of nulls.
   */
  int getTotalValueCount() {
    return lengthReader.getTotalValueCount();
  }

  /** Checks that the rows [startRow, startRow + total) have a decoded value length. */
  private void checkRows(int startRow, int total) {
    int lengthCount = getTotalValueCount();
    if (startRow < 0 || (long) startRow + total > lengthCount) {
      throw new ParquetDecodingException("Corrupted DELTA_LENGTH_BYTE_ARRAY data: reading " +
          total + " values from row " + startRow + ", but only " + lengthCount +
          " value lengths were decoded");
    }
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    ByteBuffer buffer;
    ByteBufferOutputWriter outputWriter = ByteBufferOutputWriter::writeArrayByteBuffer;
    int length;
    checkRows(currentRow, total);
    for (int i = 0; i < total; i++) {
      length = lengthsVector.getInt(currentRow + i);
      try {
        buffer = in.slice(length);
      } catch (EOFException e) {
        throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
      }
      outputWriter.write(c, rowId + i, buffer, length);
    }
    currentRow += total;
  }

  public ByteBuffer getBytes(int rowId) {
    checkRows(rowId, 1);
    int length = lengthsVector.getInt(rowId);
    try {
      return in.slice(length);
    } catch (EOFException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
    }
  }

  @Override
  public void skipBinary(int total) {
    checkRows(currentRow, total);
    long totalLength = 0;
    for (int i = 0; i < total; i++) {
      totalLength += lengthsVector.getInt(currentRow + i);
    }
    skipFully(in, totalLength);
    currentRow += total;
  }
}

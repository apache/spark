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

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An implementation of the Parquet DELTA_BYTE_ARRAY decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaByteArrayReader extends VectorizedReaderBase
    implements VectorizedValuesReader, RequiresPreviousReader {

  private final VectorizedDeltaBinaryPackedReader prefixLengthReader =
      new VectorizedDeltaBinaryPackedReader();
  private final VectorizedDeltaLengthByteArrayReader suffixReader;
  private WritableColumnVector prefixLengthVector;
  private WritableColumnVector suffixVector;
  private byte[] previous = new byte[0];
  private int currentRow = 0;

  //temporary variable used by getBinary
  private final WritableColumnVector binaryValVector;

  VectorizedDeltaByteArrayReader() {
    this.suffixReader = new VectorizedDeltaLengthByteArrayReader();
    binaryValVector = new OnHeapColumnVector(1, BinaryType);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    prefixLengthVector = new OnHeapColumnVector(valueCount, IntegerType);
    suffixVector = new OnHeapColumnVector(valueCount, BinaryType);
    prefixLengthReader.initFromPage(valueCount, in);
    prefixLengthReader.readIntegers(prefixLengthReader.getTotalValueCount(),
        prefixLengthVector, 0);
    suffixReader.initFromPage(valueCount, in);
    suffixReader.readBinary(valueCount, suffixVector, 0);
  }

  @Override
  public Binary readBinary(int len) {
    readValues(1, binaryValVector, 0, ByteBufferOutputWriter::writeArrayByteBuffer);
    return Binary.fromConstantByteArray(binaryValVector.getBinary(0));
  }

  private void readValues(int total, WritableColumnVector c, int rowId,
      ByteBufferOutputWriter outputWriter) {
    if (total == 0) {
      return;
    }

    for (int i = 0; i < total; i++) {
      int prefixLength = prefixLengthVector.getInt(currentRow);
      byte[] suffix = suffixVector.getBinary(currentRow);
      // This does not copy bytes
      int length = prefixLength + suffix.length;

      // NOTE: due to PARQUET-246, it is important that we
      // respect prefixLength which was read from prefixLengthReader,
      // even for the *first* value of a page. Even though the first
      // value of the page should have an empty prefix, it may not
      // because of PARQUET-246.

      // We have to do this to materialize the output
      if (prefixLength != 0) {
        // We could do
        //  c.putByteArray(rowId + i, previous, 0, prefixLength);
        //  c.putByteArray(rowId+i, suffix, prefixLength, suffix.length);
        //  previous =  c.getBinary(rowId+1);
        // but it incurs the same cost of copying the values twice _and_ c.getBinary
        // is a _slow_ byte by byte copy
        // The following always uses the faster system arraycopy method
        byte[] out = new byte[length];
        System.arraycopy(previous, 0, out, 0, prefixLength);
        System.arraycopy(suffix, 0, out, prefixLength, suffix.length);
        previous = out;
      } else {
        previous = suffix;
      }
      outputWriter.write(c, rowId + i, ByteBuffer.wrap(previous), previous.length);
      currentRow++;
    }
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId, ByteBufferOutputWriter::writeArrayByteBuffer);
  }

  /**
   * There was a bug (PARQUET-246) in which DeltaByteArrayWriter's reset() method did not clear the
   * previous value state that it tracks internally. This resulted in the first value of all pages
   * (except for the first page) to be a delta from the last value of the previous page. In order to
   * read corrupted files written with this bug, when reading a new page we need to recover the
   * previous page's last value to use it (if needed) to read the first value.
   */
  public void setPreviousReader(ValuesReader reader) {
    if (reader != null) {
      this.previous = ((VectorizedDeltaByteArrayReader) reader).previous;
    }
  }

  @Override
  public void skipBinary(int total) {
    // we have to read all the values so that we always have the correct 'previous'
    // we just don't write it to the output vector
    readValues(total, null, currentRow, ByteBufferOutputWriter::skipWrite);
  }

}

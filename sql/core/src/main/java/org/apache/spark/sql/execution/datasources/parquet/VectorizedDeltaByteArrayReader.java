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

  private final VectorizedDeltaBinaryPackedReader prefixLengthReader;
  private final VectorizedDeltaLengthByteArrayReader suffixReader;
  private WritableColumnVector prefixLengthVector;
  private ByteBuffer previous;
  private int currentRow = 0;

  // Temporary variable used by readBinary
  private final WritableColumnVector binaryValVector;
  // Temporary variable used by skipBinary
  private final WritableColumnVector tempBinaryValVector;

  VectorizedDeltaByteArrayReader() {
    this.prefixLengthReader = new VectorizedDeltaBinaryPackedReader();
    this.suffixReader = new VectorizedDeltaLengthByteArrayReader();
    binaryValVector = new OnHeapColumnVector(1, BinaryType);
    tempBinaryValVector = new OnHeapColumnVector(1, BinaryType);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    prefixLengthVector = new OnHeapColumnVector(valueCount, IntegerType);
    prefixLengthReader.initFromPage(valueCount, in);
    prefixLengthReader.readIntegers(prefixLengthReader.getTotalValueCount(),
        prefixLengthVector, 0);
    suffixReader.initFromPage(valueCount, in);
  }

  @Override
  public Binary readBinary(int len) {
    readValues(1, binaryValVector, 0);
    return Binary.fromConstantByteArray(binaryValVector.getBinary(0));
  }

  private void readValues(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      // NOTE: due to PARQUET-246, it is important that we
      // respect prefixLength which was read from prefixLengthReader,
      // even for the *first* value of a page. Even though the first
      // value of the page should have an empty prefix, it may not
      // because of PARQUET-246.
      int prefixLength = prefixLengthVector.getInt(currentRow);
      ByteBuffer suffix = suffixReader.getBytes(currentRow);
      byte[] suffixArray = suffix.array();
      int suffixLength = suffix.limit() - suffix.position();
      int length = prefixLength + suffixLength;

      // We have to do this to materialize the output
      WritableColumnVector arrayData = c.arrayData();
      int offset = arrayData.getElementsAppended();
      if (prefixLength != 0) {
        arrayData.appendBytes(prefixLength, previous.array(), previous.position());
      }
      arrayData.appendBytes(suffixLength, suffixArray, suffix.position());
      c.putArray(rowId + i, offset, length);
      previous = arrayData.getByteBuffer(offset, length);
      currentRow++;
    }
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId);
  }

  /**
   * There was a bug (PARQUET-246) in which DeltaByteArrayWriter's reset() method did not clear the
   * previous value state that it tracks internally. This resulted in the first value of all pages
   * (except for the first page) to be a delta from the last value of the previous page. In order to
   * read corrupted files written with this bug, when reading a new page we need to recover the
   * previous page's last value to use it (if needed) to read the first value.
   */
  @Override
  public void setPreviousReader(ValuesReader reader) {
    if (reader != null) {
      this.previous = ((VectorizedDeltaByteArrayReader) reader).previous;
    }
  }

  @Override
  public void skipBinary(int total) {
    WritableColumnVector c1 = tempBinaryValVector;
    WritableColumnVector c2 = binaryValVector;

    for (int i = 0; i < total; i++) {
      int prefixLength = prefixLengthVector.getInt(currentRow);
      ByteBuffer suffix = suffixReader.getBytes(currentRow);
      byte[] suffixArray = suffix.array();
      int suffixLength = suffix.limit() - suffix.position();
      int length = prefixLength + suffixLength;

      WritableColumnVector arrayData = c1.arrayData();
      c1.reset();
      if (prefixLength != 0) {
        arrayData.appendBytes(prefixLength, previous.array(), previous.position());
      }
      arrayData.appendBytes(suffixLength, suffixArray, suffix.position());
      previous = arrayData.getByteBuffer(0, length);
      currentRow++;

      WritableColumnVector tmp = c1;
      c1 = c2;
      c2 = tmp;
    }
  }

}

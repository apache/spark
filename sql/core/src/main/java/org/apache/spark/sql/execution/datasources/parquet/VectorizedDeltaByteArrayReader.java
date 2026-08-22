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
import org.apache.spark.sql.types.GeographyType;
import org.apache.spark.sql.types.GeometryType;

import java.io.IOException;

/**
 * An implementation of the Parquet DELTA_BYTE_ARRAY decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaByteArrayReader extends VectorizedReaderBase
    implements VectorizedValuesReader, RequiresPreviousReader {

  private final VectorizedDeltaBinaryPackedReader prefixLengthReader;
  private final VectorizedDeltaLengthByteArrayReader suffixReader;
  private WritableColumnVector prefixLengthVector;

  /**
   * Reusable buffer holding the most recently decoded value. On each iteration the
   * prefix bytes (shared with the previous value) are already in place at the start
   * of this buffer, so only the suffix needs to be copied in. This eliminates the
   * per-value allocation that the old {@code ByteBuffer}-based approach required.
   */
  private byte[] prevBuf = new byte[64];
  private int prevLen = 0;
  private int currentRow = 0;

  // Temporary variable used by readBinary(int)
  private final WritableColumnVector binaryValVector;

  VectorizedDeltaByteArrayReader() {
    this.prefixLengthReader = new VectorizedDeltaBinaryPackedReader();
    this.suffixReader = new VectorizedDeltaLengthByteArrayReader();
    binaryValVector = new OnHeapColumnVector(1, BinaryType);
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
      int suffixLength = suffixReader.getSuffixLength(currentRow);
      int length = prefixLength + suffixLength;

      // Grow prevBuf if needed, preserving the prefix bytes already in place.
      if (length > prevBuf.length) {
        byte[] newBuf = new byte[Math.max(length, prevBuf.length * 2)];
        System.arraycopy(prevBuf, 0, newBuf, 0, prefixLength);
        prevBuf = newBuf;
      }
      // The prefix bytes (prevBuf[0..prefixLength-1]) are already in place from
      // the previous iteration. Read the suffix directly after them.
      suffixReader.getSuffixInto(currentRow, prevBuf, prefixLength);
      prevLen = length;

      // Write the full assembled value to the output column vector.
      WritableColumnVector arrayData = c.arrayData();
      int offset = arrayData.getElementsAppended();
      arrayData.appendBytes(length, prevBuf, 0);
      c.putArray(rowId + i, offset, length);
      currentRow++;
    }
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    readValues(total, c, rowId);
  }

  @Override
  public void readGeometry(int total, WritableColumnVector c, int rowId) {
    assert(c.dataType() instanceof GeometryType);
    int srid = ((GeometryType) c.dataType()).srid();
    readGeoData(total, c, rowId, srid, WKBToGeometryConverter.INSTANCE);
  }

  @Override
  public void readGeography(int total, WritableColumnVector c, int rowId) {
    assert(c.dataType() instanceof GeographyType);
    int srid = ((GeographyType) c.dataType()).srid();
    readGeoData(total, c, rowId, srid, WKBToGeographyConverter.INSTANCE);
  }

  private void readGeoData(int total, WritableColumnVector c, int rowId, int srid,
     WKBConverterStrategy converter) {
    for (int i = 0; i < total; i++) {
      int prefixLength = prefixLengthVector.getInt(currentRow);
      int suffixLength = suffixReader.getSuffixLength(currentRow);
      int length = prefixLength + suffixLength;

      byte[] wkb = new byte[length];
      if (prefixLength > 0) {
        System.arraycopy(prevBuf, 0, wkb, 0, prefixLength);
      }
      suffixReader.getSuffixInto(currentRow, wkb, prefixLength);

      // Converts WKB into a physical representation of geometry/geography.
      byte[] physicalValue = converter.convert(wkb, srid);

      WritableColumnVector arrayData = c.arrayData();
      int offset = arrayData.getElementsAppended();
      arrayData.appendBytes(physicalValue.length, physicalValue, 0);

      c.putArray(rowId + i, offset, physicalValue.length);

      // Take ownership of wkb as prevBuf -- it is freshly allocated and
      // exactly the right size for the next value's prefix.
      prevBuf = wkb;
      prevLen = length;

      currentRow++;
    }
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
      VectorizedDeltaByteArrayReader prev = (VectorizedDeltaByteArrayReader) reader;
      if (prev.prevLen > prevBuf.length) {
        prevBuf = new byte[prev.prevLen];
      }
      System.arraycopy(prev.prevBuf, 0, prevBuf, 0, prev.prevLen);
      prevLen = prev.prevLen;
    }
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      int prefixLength = prefixLengthVector.getInt(currentRow);
      int suffixLength = suffixReader.getSuffixLength(currentRow);
      int length = prefixLength + suffixLength;

      // Grow prevBuf if needed, preserving the prefix bytes already in place.
      if (length > prevBuf.length) {
        byte[] newBuf = new byte[Math.max(length, prevBuf.length * 2)];
        System.arraycopy(prevBuf, 0, newBuf, 0, prefixLength);
        prevBuf = newBuf;
      }
      // Read the suffix directly after the prefix -- keeps prevBuf up-to-date
      // for the next value (or the next page via setPreviousReader).
      suffixReader.getSuffixInto(currentRow, prevBuf, prefixLength);
      prevLen = length;
      currentRow++;
    }
  }

}

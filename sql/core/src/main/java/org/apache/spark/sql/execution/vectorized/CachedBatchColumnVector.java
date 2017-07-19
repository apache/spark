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
package org.apache.spark.sql.execution.vectorized;

import java.nio.ByteBuffer;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.execution.columnar.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column backed by data compressed thru ColumnAccessor
 * this is a wrapper to read compressed data for table cache
 */
public final class CachedBatchColumnVector extends ColumnVector {

  // accessor for a column
  private ColumnAccessor columnAccessor;

  // a row where the compressed data is extracted
  private transient UnsafeRow unsafeRow;
  private transient BufferHolder bufferHolder;
  private transient UnsafeRowWriter rowWriter;
  private transient MutableUnsafeRow mutableRow;

  // an accessor uses only column 0
  private final int ORDINAL = 0;

  // Keep row id that was previously accessed
  private int previousRowId = -1;


  public CachedBatchColumnVector(byte[] buffer, int numRows, DataType type) {
    super(numRows, DataTypes.NullType, MemoryMode.ON_HEAP);
    initialize(buffer, type);
    reserveInternal(numRows);
    reset();
  }

  @Override
  public long valuesNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }
  @Override
  public long nullsNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public void close() {
  }

  // call extractTo() for rowId only once before getting actual data
  private void prepareAccess(int rowId) {
    if (previousRowId == rowId) {
      // do nothing
    } else if (previousRowId < rowId) {
      for (; previousRowId < rowId; previousRowId++) {
        assert (columnAccessor.hasNext());
        bufferHolder.reset();
        rowWriter.zeroOutNullBytes();
        columnAccessor.extractTo(mutableRow, ORDINAL);
      }
    } else {
      throw new UnsupportedOperationException("Row access order must be equal or ascending." +
        " Internal row " + rowId + "is accessed after internal row "+ previousRowId + "was accessed.");
    }
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNull(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.isNullAt(ORDINAL);
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getBoolean(ORDINAL);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getByte(ORDINAL);
  }

  @Override
  public byte[] getBytes(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getShort(ORDINAL);
  }

  @Override
  public short[] getShorts(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getInt(ORDINAL);
  }

  @Override
  public int[] getInts(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  public int getDictId(int rowId) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getLong(ORDINAL);
  }

  @Override
  public long[] getLongs(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getFloat(ORDINAL);
  }

  @Override
  public float[] getFloats(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getDouble(ORDINAL);
  }

  @Override
  public double[] getDoubles(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    throw new UnsupportedOperationException();
  }
  @Override
  public int getArrayOffset(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadBytes(ColumnVector.Array array) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  public final UTF8String getUTF8String(int rowId) {
    prepareAccess(rowId);
    return unsafeRow.getUTF8String(ORDINAL);
  }

  @Override
  protected void reserveInternal(int newCapacity) {
    capacity = newCapacity;
  }

  private void initialize(byte[] buffer, DataType type) {
    this.type = type;

    if (columnAccessor == null) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
      columnAccessor = ColumnAccessor$.MODULE$.apply(type, byteBuffer);
    }
    if (mutableRow == null) {
      unsafeRow = new UnsafeRow(1);
      bufferHolder = new BufferHolder(unsafeRow);
      rowWriter = new UnsafeRowWriter(bufferHolder, 1);
      mutableRow = new MutableUnsafeRow(rowWriter);
    }

    if (type instanceof ArrayType) {
      throw new UnsupportedOperationException();
    } else if (type instanceof BinaryType) {
      throw new UnsupportedOperationException();
    } else if (type instanceof StructType) {
      throw new UnsupportedOperationException();
    } else if (type instanceof MapType) {
      throw new UnsupportedOperationException();
    } else if (type instanceof DecimalType && ((DecimalType) type).precision() > Decimal.MAX_LONG_DIGITS()) {
      throw new UnsupportedOperationException();
    }
  }
}

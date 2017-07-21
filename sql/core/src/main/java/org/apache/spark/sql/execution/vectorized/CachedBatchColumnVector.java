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
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;

/**
 * A column vector backed by data compressed thru ColumnAccessor
 * this is a wrapper to read compressed data for table cache
 */
public final class CachedBatchColumnVector extends ReadOnlyColumnVector {

  // accessor for a column
  private ColumnAccessor columnAccessor;

  // Array for decompressed null information in a column
  private byte[] nulls;

  // Array for decompressed data information in a column
  private byte[] data;
  private long offset;


  public CachedBatchColumnVector(byte[] buffer, int numRows, DataType type) {
    super(numRows, type, MemoryMode.ON_HEAP);
    initialize(buffer, type);
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

  //
  // APIs dealing with nulls
  //

  @Override
  public boolean isNullAt(int rowId) {
    return nulls[rowId] == 1;
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getBoolean(data, offset + rowId);
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
  public byte getByte(int rowId) {
    return Platform.getByte(data, offset + rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public short getShort(int rowId) {
    return Platform.getShort(data, offset + rowId * 2);
  }

  @Override
  public short[] getShorts(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(data, offset + rowId * 4);
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
  public long getLong(int rowId) {
    return Platform.getLong(data, offset + rowId * 8);
  }

  @Override
  public long[] getLongs(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with floats
  //

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(data, offset + rowId * 4);
  }

  @Override
  public float[] getFloats(int rowId, int count)  {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(data, offset + rowId * 8);
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
  public void loadBytes(ColumnVector.Array array) {
    throw new UnsupportedOperationException();
  }

  //
  // APIs dealing with Byte Arrays
  //

  public final UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  private void initialize(byte[] buffer, DataType type) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    columnAccessor = ColumnAccessor$.MODULE$.apply(type, byteBuffer);

    if (type instanceof StringType) {
      throw new UnsupportedOperationException();
    } else if (type instanceof ArrayType) {
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

    Tuple2 buffers = ColumnAccessor$.MODULE$.decompress(columnAccessor, capacity);
    ByteBuffer dataBuffer = (ByteBuffer)(buffers._1());
    ByteBuffer nullsBuffer = (ByteBuffer)(buffers._2());

    int numNulls = ByteBufferHelper.getInt(nullsBuffer);
    if (numNulls > 0) {
      nulls = new byte[capacity];
      anyNullsSet = true;
    }
    for (int i = 0; i < numNulls; i++) {
      int cordinal = ByteBufferHelper.getInt(nullsBuffer);
      nulls[cordinal] = (byte)1;
    }
    data = dataBuffer.array();
    offset = Platform.BYTE_ARRAY_OFFSET + dataBuffer.position();
  }
}

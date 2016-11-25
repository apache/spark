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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector class wrapping Hive's ColumnVector. Because Spark ColumnarBatch only accepts
 * Spark's vectorized.ColumnVector, this column vector is used to adapt Hive ColumnVector with
 * Spark ColumnarBatch. This class inherits Spark's vectorized.ColumnVector class, but all data
 * setter methods (e.g., putInt) in Spark vectorized.ColumnVector are not implemented.
 */
public class OrcColumnVector extends org.apache.spark.sql.execution.vectorized.ColumnVector {
  private ColumnVector col;

  public OrcColumnVector(ColumnVector col, DataType type) {
    super(type);
    this.col = col;
  }

  /* A helper method to get the row index in a column. */
  private int getRowIndex(int rowId) {
    return this.col.isRepeating ? 0 : rowId;
  }

  @Override
  public long valuesNativeAddress() {
    throw new NotImplementedException();
  }

  @Override
  public long nullsNativeAddress() {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    throw new NotImplementedException();
  }

  @Override
  public void putNull(int rowId) {
    throw new NotImplementedException();
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new NotImplementedException();
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    throw new NotImplementedException();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return col.isNull[getRowIndex(rowId)];
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    throw new NotImplementedException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    LongColumnVector col = (LongColumnVector) this.col;
    return col.vector[getRowIndex(rowId)] > 0;
  }

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    throw new NotImplementedException();
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public byte getByte(int rowId) {
    LongColumnVector col = (LongColumnVector) this.col;
    return (byte) col.vector[getRowIndex(rowId)];
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    throw new NotImplementedException();
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new NotImplementedException();
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public short getShort(int rowId) {
    LongColumnVector col = (LongColumnVector) this.col;
    return (short) col.vector[getRowIndex(rowId)];
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    throw new NotImplementedException();
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new NotImplementedException();
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public int getInt(int rowId) {
    LongColumnVector col = (LongColumnVector) this.col;
    return (int) col.vector[getRowIndex(rowId)];
  }

  /**
   * Returns the dictionary Id for rowId.
   */
  @Override
  public int getDictId(int rowId) {
    throw new NotImplementedException();
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public long getLong(int rowId) {
    LongColumnVector col = (LongColumnVector) this.col;
    return (long) col.vector[getRowIndex(rowId)];
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public float getFloat(int rowId) {
    DoubleColumnVector col = (DoubleColumnVector) this.col;
    return (float) col.vector[getRowIndex(rowId)];
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    throw new NotImplementedException();
  }

  @Override
  public double getDouble(int rowId) {
    DoubleColumnVector col = (DoubleColumnVector) this.col;
    return (double) col.vector[getRowIndex(rowId)];
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    throw new NotImplementedException();
  }

  @Override
  public int getArrayOffset(int rowId) {
    throw new NotImplementedException();
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  public void loadBytes(org.apache.spark.sql.execution.vectorized.ColumnVector.Array array) {
    throw new NotImplementedException();
  }

  /**
   * Returns the decimal for rowId.
   */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    DecimalColumnVector col = (DecimalColumnVector) this.col;
    int index = getRowIndex(rowId);
    return Decimal.apply(col.vector[index].getHiveDecimal().bigDecimalValue(), precision, scale);
  }

  /**
   * Returns the UTF8String for rowId.
   */
  @Override
  public UTF8String getUTF8String(int rowId) {
    BytesColumnVector col = (BytesColumnVector) this.col;
    int index = getRowIndex(rowId);
    return UTF8String.fromBytes(col.vector[index], col.start[index], col.length[index]);
  }

  /**
   * Returns the byte array for rowId.
   */
  @Override
  public byte[] getBinary(int rowId) {
    BytesColumnVector col = (BytesColumnVector) this.col;
    int index = getRowIndex(rowId);
    byte[] binary = new byte[col.length[index]];
    System.arraycopy(col.vector[index], col.start[index], binary, 0, binary.length);
    return binary;
  }

  //
  // APIs dealing with Byte Arrays
  //
  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    throw new NotImplementedException();
  }

  @Override
  protected void reserveInternal(int newCapacity) {
    throw new NotImplementedException();
  }
}

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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class adds the constant support to ColumnVector.
 * It supports all the types and contains `set` APIs,
 * which will set the exact same value to all rows.
 *
 * Capacity: The vector only stores one copy of the data, and acts as an unbounded vector
 * (get from any row will return the same value)
 */
public class ConstantColumnVector extends ColumnVector {

  private byte nullData;
  private byte byteData;
  private short shortData;
  private int intData;
  private long longData;
  private float floatData;
  private double doubleData;
  private byte[] byteArrayData;
  private ConstantColumnVector[] childData;
  private ColumnarArray arrayData;
  private ColumnarMap mapData;

  private final int numRows;

  public ConstantColumnVector(int numRows, DataType type) {
    super(type);
    this.numRows = numRows;

    if (type instanceof StructType) {
      this.childData = new ConstantColumnVector[((StructType) type).fields().length];
    } else if (type instanceof CalendarIntervalType) {
      // Three columns. Months as int. Days as Int. Microseconds as Long.
      this.childData = new ConstantColumnVector[3];
    } else {
      this.childData = null;
    }
  }

  @Override
  public void close() {
    byteArrayData = null;
    for (int i = 0; i < childData.length; i++) {
      childData[i].close();
      childData[i] = null;
    }
    childData = null;
    arrayData = null;
    mapData = null;
  }

  @Override
  public boolean hasNull() {
    return nullData == 1;
  }

  @Override
  public int numNulls() {
    return hasNull() ? numRows : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullData == 1;
  }

  public void setNull() {
    nullData = (byte) 1;
  }

  public void setNotNull() {
    nullData = (byte) 0;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return byteData == 1;
  }

  public void setBoolean(boolean value) {
    byteData = (byte) ((value) ? 1 : 0);
  }

  @Override
  public byte getByte(int rowId) {
    return byteData;
  }

  public void setByte(byte value) {
    byteData = value;
  }

  @Override
  public short getShort(int rowId) {
    return shortData;
  }

  public void setShort(short value) {
    shortData = value;
  }

  @Override
  public int getInt(int rowId) {
    return intData;
  }

  public void setInt(int value) {
    intData = value;
  }

  @Override
  public long getLong(int rowId) {
    return longData;
  }

  public void setLong(long value) {
    longData = value;
  }

  @Override
  public float getFloat(int rowId) {
    return floatData;
  }

  public void setFloat(float value) {
    floatData = value;
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData;
  }

  public void setDouble(double value) {
    doubleData = value;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return arrayData;
  }

  public void setArray(ColumnarArray value) {
    arrayData = value;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return mapData;
  }

  public void setMap(ColumnarMap value) {
    mapData = value;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  public void setDecimal(Decimal value, int precision) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      setInt((int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      setLong(value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      setByteArray(bigInteger.toByteArray());
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromBytes(byteArrayData);
  }

  public void setUtf8String(UTF8String value) {
    setByteArray(value.getBytes());
  }

  private void setByteArray(byte[] value) {
    byteArrayData =  value;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return byteArrayData;
  }

  public void setBinary(byte[] value) {
    setByteArray(value);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return childData[ordinal];
  }

  public void setChild(int ordinal, ConstantColumnVector value) {
    childData[ordinal] = value;
  }
}

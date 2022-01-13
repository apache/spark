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

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This class adds the constant support to ColumnVector.
 * It supports all the types and contains put APIs,
 * which will put the exact same value to all rows.
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

  private int numRows;

  public ConstantColumnVector(int numRows, DataType type) {
    super(type);
    this.numRows = numRows;

    // copy and modify from WritableColumnVector
    // could also putChild by users
    if (isArray()) {
      DataType childType;
      if (type instanceof ArrayType) {
        childType = ((ArrayType) type).elementType();
      } else {
        childType = DataTypes.ByteType;
      }
      this.childData = new ConstantColumnVector[1];
      this.childData[0] = new ConstantColumnVector(numRows, childType);
    } else if (type instanceof StructType) {
      StructType st = (StructType) type;
      this.childData = new ConstantColumnVector[st.fields().length];
      for (int i = 0; i < childData.length; ++i) {
        this.childData[i] = new ConstantColumnVector(numRows, st.fields()[i].dataType());
      }
    } else if (type instanceof MapType) {
      // 0: key, 1: value
      MapType mapType = (MapType) type;
      this.childData = new ConstantColumnVector[2];
      this.childData[0] = new ConstantColumnVector(numRows, mapType.keyType());
      this.childData[1] = new ConstantColumnVector(numRows, mapType.valueType());
    } else if (type instanceof CalendarIntervalType) {
      // 0: Months as Int, 1: Days as Int, 2: Microseconds as Long.
      this.childData = new ConstantColumnVector[3];
      this.childData[0] = new ConstantColumnVector(numRows, DataTypes.IntegerType);
      this.childData[1] = new ConstantColumnVector(numRows, DataTypes.IntegerType);
      this.childData[2] = new ConstantColumnVector(numRows, DataTypes.LongType);
    } else {
      this.childData = null;
    }
  }

  protected boolean isArray() {
    return type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType ||
        DecimalType.isByteArrayDecimalType(type);
  }

  @Override
  public void close() {
    byteArrayData = null;
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

  public void putNull() {
    nullData = (byte) 1;
  }

  public void putNotNull() {
    nullData = (byte) 0;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return byteData == 1;
  }

  public void putBoolean(boolean value) {
    byteData = (byte) ((value) ? 1 : 0);
  }

  @Override
  public byte getByte(int rowId) {
    return byteData;
  }

  public void putByte(byte value) {
    byteData = value;
  }

  @Override
  public short getShort(int rowId) {
    return shortData;
  }

  public void putShort(short value) {
    shortData = value;
  }

  @Override
  public int getInt(int rowId) {
    return intData;
  }

  public void putInt(int value) {
    intData = value;
  }

  @Override
  public long getLong(int rowId) {
    return longData;
  }

  public void putLong(long value) {
    longData = value;
  }

  @Override
  public float getFloat(int rowId) {
    return floatData;
  }

  public void putFloat(float value) {
    floatData = value;
  }

  @Override
  public double getDouble(int rowId) {
    return doubleData;
  }

  public void putDouble(double value) {
    doubleData = value;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return arrayData;
  }

  public void putArray(ColumnarArray value) {
    arrayData = value;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return mapData;
  }

  public void putMap(ColumnarMap value) {
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

  public void putDecimal(Decimal value, int precision) {
    // copy and modify from WritableColumnVector
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt((int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(bigInteger.toByteArray());
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromBytes(byteArrayData);
  }

  public void putUtf8String(UTF8String value) {
    putByteArray(value.getBytes());
  }

  private void putByteArray(byte[] value) {
    byteArrayData =  value;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return byteArrayData;
  }

  public void putBinary(byte[] value) {
    putByteArray(value);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return childData[ordinal];
  }

  public void putChild(int ordinal, ConstantColumnVector value) {
    childData[ordinal] = value;
  }
}

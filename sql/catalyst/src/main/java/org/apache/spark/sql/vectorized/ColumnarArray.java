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
package org.apache.spark.sql.vectorized;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Array abstraction in {@link ColumnVector}.
 */
@Evolving
public final class ColumnarArray extends ArrayData {
  // The data for this array. This array contains elements from
  // data[offset] to data[offset + length).
  private final ColumnVector data;
  private final int offset;
  private final int length;

  public ColumnarArray(ColumnVector data, int offset, int length) {
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int numElements() {
    return length;
  }

  /**
   * Sets all the appropriate null bits in the input UnsafeArrayData.
   *
   * @param arrayData The UnsafeArrayData to set the null bits for
   * @return The UnsafeArrayData with the null bits set
   */
  private UnsafeArrayData setNullBits(UnsafeArrayData arrayData) {
    if (data.hasNull()) {
      for (int i = 0; i < length; i++) {
        if (data.isNullAt(offset + i)) {
          arrayData.setNullAt(i);
        }
      }
    }
    return arrayData;
  }

  @Override
  public ArrayData copy() {
    DataType dt = data.dataType();

    if (dt instanceof BooleanType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toBooleanArray()));
    } else if (dt instanceof ByteType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toByteArray()));
    } else if (dt instanceof ShortType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toShortArray()));
    } else if (dt instanceof IntegerType || dt instanceof DateType
            || dt instanceof YearMonthIntervalType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toIntArray()));
    } else if (dt instanceof LongType || dt instanceof TimestampType
            || dt instanceof DayTimeIntervalType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toLongArray()));
    } else if (dt instanceof FloatType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toFloatArray()));
    } else if (dt instanceof DoubleType) {
      return setNullBits(UnsafeArrayData.fromPrimitiveArray(toDoubleArray()));
    } else {
      return new GenericArrayData(toObjectArray(dt)).copy(); // ensure the elements are copied.
    }
  }

  @Override
  public boolean[] toBooleanArray() { return data.getBooleans(offset, length); }

  @Override
  public byte[] toByteArray() { return data.getBytes(offset, length); }

  @Override
  public short[] toShortArray() { return data.getShorts(offset, length); }

  @Override
  public int[] toIntArray() { return data.getInts(offset, length); }

  @Override
  public long[] toLongArray() { return data.getLongs(offset, length); }

  @Override
  public float[] toFloatArray() { return data.getFloats(offset, length); }

  @Override
  public double[] toDoubleArray() { return data.getDoubles(offset, length); }

  // TODO: this is extremely expensive.
  @Override
  public Object[] array() {
    DataType dt = data.dataType();
    Object[] list = new Object[length];
    try {
      for (int i = 0; i < length; i++) {
        if (!data.isNullAt(offset + i)) {
          list[i] = get(i, dt);
        }
      }
      return list;
    } catch(Exception e) {
      throw new RuntimeException("Could not get the array", e);
    }
  }

  @Override
  public boolean isNullAt(int ordinal) { return data.isNullAt(offset + ordinal); }

  @Override
  public boolean getBoolean(int ordinal) {
    return data.getBoolean(offset + ordinal);
  }

  @Override
  public byte getByte(int ordinal) { return data.getByte(offset + ordinal); }

  @Override
  public short getShort(int ordinal) {
    return data.getShort(offset + ordinal);
  }

  @Override
  public int getInt(int ordinal) { return data.getInt(offset + ordinal); }

  @Override
  public long getLong(int ordinal) { return data.getLong(offset + ordinal); }

  @Override
  public float getFloat(int ordinal) {
    return data.getFloat(offset + ordinal);
  }

  @Override
  public double getDouble(int ordinal) { return data.getDouble(offset + ordinal); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getDecimal(offset + ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getUTF8String(offset + ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getBinary(offset + ordinal);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getInterval(offset + ordinal);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return data.getStruct(offset + ordinal);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return data.getArray(offset + ordinal);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return data.getMap(offset + ordinal);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return SpecializedGettersReader.read(this, ordinal, dataType, false, false);
  }

  @Override
  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

  @Override
  public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
}

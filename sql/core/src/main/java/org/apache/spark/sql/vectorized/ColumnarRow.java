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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Row abstraction in {@link ColumnVector}.
 */
public final class ColumnarRow extends InternalRow {
  // The data for this row.
  // E.g. the value of 3rd int field is `data.getChildColumn(3).getInt(rowId)`.
  private final ColumnVector data;
  private final int rowId;
  private final int numFields;

  public ColumnarRow(ColumnVector data, int rowId) {
    assert (data.dataType() instanceof StructType);
    this.data = data;
    this.rowId = rowId;
    this.numFields = ((StructType) data.dataType()).size();
  }

  @Override
  public int numFields() { return numFields; }

  /**
   * Revisit this. This is expensive. This is currently only used in test paths.
   */
  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(numFields);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = data.getChildColumn(i).dataType();
        if (dt instanceof BooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (dt instanceof ByteType) {
          row.setByte(i, getByte(i));
        } else if (dt instanceof ShortType) {
          row.setShort(i, getShort(i));
        } else if (dt instanceof IntegerType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof LongType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof FloatType) {
          row.setFloat(i, getFloat(i));
        } else if (dt instanceof DoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (dt instanceof StringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (dt instanceof BinaryType) {
          row.update(i, getBinary(i));
        } else if (dt instanceof DecimalType) {
          DecimalType t = (DecimalType)dt;
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
          row.setLong(i, getLong(i));
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) { return data.getChildColumn(ordinal).isNullAt(rowId); }

  @Override
  public boolean getBoolean(int ordinal) { return data.getChildColumn(ordinal).getBoolean(rowId); }

  @Override
  public byte getByte(int ordinal) { return data.getChildColumn(ordinal).getByte(rowId); }

  @Override
  public short getShort(int ordinal) { return data.getChildColumn(ordinal).getShort(rowId); }

  @Override
  public int getInt(int ordinal) { return data.getChildColumn(ordinal).getInt(rowId); }

  @Override
  public long getLong(int ordinal) { return data.getChildColumn(ordinal).getLong(rowId); }

  @Override
  public float getFloat(int ordinal) { return data.getChildColumn(ordinal).getFloat(rowId); }

  @Override
  public double getDouble(int ordinal) { return data.getChildColumn(ordinal).getDouble(rowId); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    return data.getChildColumn(ordinal).getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    return data.getChildColumn(ordinal).getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    return data.getChildColumn(ordinal).getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    final int months = data.getChildColumn(ordinal).getChildColumn(0).getInt(rowId);
    final long microseconds = data.getChildColumn(ordinal).getChildColumn(1).getLong(rowId);
    return new CalendarInterval(months, microseconds);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    return data.getChildColumn(ordinal).getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    if (data.getChildColumn(ordinal).isNullAt(rowId)) return null;
    return data.getChildColumn(ordinal).getArray(rowId);
  }

  @Override
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType t = (DecimalType) dataType;
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType)dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }

  @Override
  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

  @Override
  public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
}

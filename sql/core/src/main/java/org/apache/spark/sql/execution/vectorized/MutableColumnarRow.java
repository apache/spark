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
import java.util.Map;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * A mutable version of {@link ColumnarRow}, which is used in the vectorized hash map for hash
 * aggregate, and {@link ColumnarBatch} to save object creation.
 *
 * Note that this class intentionally has a lot of duplicated code with {@link ColumnarRow}, to
 * avoid java polymorphism overhead by keeping {@link ColumnarRow} and this class final classes.
 */
public final class MutableColumnarRow extends InternalRow {
  public int rowId;
  private final WritableColumnVector[] columns;

  public MutableColumnarRow(WritableColumnVector[] writableColumns) {
    this.columns = writableColumns;
  }

  @Override
  public int numFields() { return columns.length; }

  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(columns.length);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = columns[i].dataType();
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
        } else if (dt instanceof DecimalType t) {
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof StructType) {
          row.update(i, getStruct(i, ((StructType) dt).fields().length).copy());
        } else if (dt instanceof ArrayType) {
          row.update(i, getArray(i).copy());
        } else if (dt instanceof MapType) {
          row.update(i, getMap(i).copy());
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public boolean isNullAt(int ordinal) { return columns[ordinal].isNullAt(rowId); }

  @Override
  public boolean getBoolean(int ordinal) { return columns[ordinal].getBoolean(rowId); }

  @Override
  public byte getByte(int ordinal) { return columns[ordinal].getByte(rowId); }

  @Override
  public short getShort(int ordinal) { return columns[ordinal].getShort(rowId); }

  @Override
  public int getInt(int ordinal) { return columns[ordinal].getInt(rowId); }

  @Override
  public long getLong(int ordinal) { return columns[ordinal].getLong(rowId); }

  @Override
  public float getFloat(int ordinal) { return columns[ordinal].getFloat(rowId); }

  @Override
  public double getDouble(int ordinal) { return columns[ordinal].getDouble(rowId); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[ordinal].getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return columns[ordinal].getInterval(rowId);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return columns[ordinal].getVariant(rowId);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return columns[ordinal].getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return columns[ordinal].getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return columns[ordinal].getMap(rowId);
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
    } else if (dataType instanceof DecimalType t) {
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType structType) {
      return getStruct(ordinal, structType.fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new SparkUnsupportedOperationException(
        "_LEGACY_ERROR_TEMP_3192", Map.of("dt", dataType.toString()));
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    if (value == null) {
      setNullAt(ordinal);
    } else {
      DataType dt = columns[ordinal].dataType();
      if (dt instanceof BooleanType) {
        setBoolean(ordinal, (boolean) value);
      } else if (dt instanceof IntegerType) {
        setInt(ordinal, (int) value);
      } else if (dt instanceof ShortType) {
        setShort(ordinal, (short) value);
      } else if (dt instanceof LongType) {
        setLong(ordinal, (long) value);
      } else if (dt instanceof FloatType) {
        setFloat(ordinal, (float) value);
      } else if (dt instanceof DoubleType) {
        setDouble(ordinal, (double) value);
      } else if (dt instanceof DecimalType t) {
        Decimal d = Decimal.apply((BigDecimal) value, t.precision(), t.scale());
        setDecimal(ordinal, d, t.precision());
      } else if (dt instanceof CalendarIntervalType) {
        setInterval(ordinal, (CalendarInterval) value);
      } else {
        throw new SparkUnsupportedOperationException(
          "_LEGACY_ERROR_TEMP_3192", Map.of("dt", dt.toString()));
      }
    }
  }

  @Override
  public void setNullAt(int ordinal) {
    columns[ordinal].putNull(rowId);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putBoolean(rowId, value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putByte(rowId, value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putShort(rowId, value);
  }

  @Override
  public void setInt(int ordinal, int value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putInt(rowId, value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putLong(rowId, value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putFloat(rowId, value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putDouble(rowId, value);
  }

  @Override
  public void setDecimal(int ordinal, Decimal value, int precision) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putDecimal(rowId, value, precision);
  }

  @Override
  public void setInterval(int ordinal, CalendarInterval value) {
    columns[ordinal].putNotNull(rowId);
    columns[ordinal].putInterval(rowId, value);
  }
}

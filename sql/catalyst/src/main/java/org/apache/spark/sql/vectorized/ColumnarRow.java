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

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.types.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

/**
 * Row abstraction in {@link ColumnVector}.
 */
@Evolving
public final class ColumnarRow extends InternalRow {
  // The data for this row.
  // E.g. the value of 3rd int field is `data.getChild(3).getInt(rowId)`.
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
        DataType dt = data.getChild(i).dataType();
        PhysicalDataType pdt = PhysicalDataType.apply(dt);
        if (pdt instanceof PhysicalBooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (pdt instanceof PhysicalByteType) {
          row.setByte(i, getByte(i));
        } else if (pdt instanceof PhysicalShortType) {
          row.setShort(i, getShort(i));
        } else if (pdt instanceof PhysicalIntegerType) {
          row.setInt(i, getInt(i));
        } else if (pdt instanceof PhysicalLongType) {
          row.setLong(i, getLong(i));
        } else if (pdt instanceof PhysicalFloatType) {
          row.setFloat(i, getFloat(i));
        } else if (pdt instanceof PhysicalDoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (pdt instanceof PhysicalStringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (pdt instanceof PhysicalBinaryType) {
          row.update(i, getBinary(i));
        } else if (pdt instanceof PhysicalDecimalType t) {
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (pdt instanceof PhysicalStructType t) {
          row.update(i, getStruct(i, t.fields().length).copy());
        } else if (pdt instanceof PhysicalArrayType) {
          row.update(i, getArray(i).copy());
        } else if (pdt instanceof PhysicalMapType) {
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
  public boolean isNullAt(int ordinal) { return data.getChild(ordinal).isNullAt(rowId); }

  @Override
  public boolean getBoolean(int ordinal) { return data.getChild(ordinal).getBoolean(rowId); }

  @Override
  public byte getByte(int ordinal) { return data.getChild(ordinal).getByte(rowId); }

  @Override
  public short getShort(int ordinal) { return data.getChild(ordinal).getShort(rowId); }

  @Override
  public int getInt(int ordinal) { return data.getChild(ordinal).getInt(rowId); }

  @Override
  public long getLong(int ordinal) { return data.getChild(ordinal).getLong(rowId); }

  @Override
  public float getFloat(int ordinal) { return data.getChild(ordinal).getFloat(rowId); }

  @Override
  public double getDouble(int ordinal) { return data.getChild(ordinal).getDouble(rowId); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return data.getChild(ordinal).getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return data.getChild(ordinal).getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return data.getChild(ordinal).getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return data.getChild(ordinal).getInterval(rowId);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return data.getChild(ordinal).getVariant(rowId);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return data.getChild(ordinal).getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return data.getChild(ordinal).getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return data.getChild(ordinal).getMap(rowId);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType || dataType instanceof YearMonthIntervalType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType || dataType instanceof DayTimeIntervalType) {
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
    } else if (dataType instanceof TimestampNTZType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType)dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else if (dataType instanceof VariantType) {
      return getVariant(ordinal);
    } else {
      throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3155");
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    throw SparkUnsupportedOperationException.apply();
  }

  @Override
  public void setNullAt(int ordinal) {
    throw SparkUnsupportedOperationException.apply();
  }
}

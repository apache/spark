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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class represents a column of values and provides the main APIs to access the data
 * values. It supports all the types and contains get APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * To handle nested schemas, ColumnVector has two types: Arrays and Structs. In both cases these
 * columns have child columns. All of the data is stored in the child columns and the parent column
 * contains nullability, and in the case of Arrays, the lengths and offsets into the child column.
 * Lengths and offsets are encoded identically to INTs.
 * Maps are just a special case of a two field struct.
 *
 * Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for values
 * in the current RowBatch.
 *
 * A ColumnVector should be considered immutable once originally created.
 *
 * ColumnVectors are intended to be reused.
 */
public abstract class ColumnVector implements AutoCloseable {

  /**
   * Holder object to return an array. This object is intended to be reused. Callers should
   * copy the data out if it needs to be stored.
   */
  public static final class Array extends ArrayData {
    // The data for this array. This array contains elements from
    // data[offset] to data[offset + length).
    public final ColumnVector data;
    public int length;
    public int offset;

    // Populate if binary data is required for the Array. This is stored here as an optimization
    // for string data.
    public byte[] byteArray;
    public int byteArrayOffset;

    // Reused staging buffer, used for loading from offheap.
    protected byte[] tmpByteArray = new byte[1];

    protected Array(ColumnVector data) {
      this.data = data;
    }

    @Override
    public int numElements() { return length; }

    @Override
    public ArrayData copy() {
      throw new UnsupportedOperationException();
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
      int month = data.getChildColumn(0).getInt(offset + ordinal);
      long microseconds = data.getChildColumn(1).getLong(offset + ordinal);
      return new CalendarInterval(month, microseconds);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return data.getStruct(offset + ordinal);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return data.getArray(offset + ordinal);
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
      } else if (dataType instanceof CalendarIntervalType) {
        return getInterval(ordinal);
      } else {
        throw new UnsupportedOperationException("Datatype not supported " + dataType);
      }
    }

    @Override
    public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

    @Override
    public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
  }

  /**
   * Returns the data type of this column.
   */
  public final DataType dataType() { return type; }

  /**
   * Cleans up memory for this column. The column is not usable after this.
   * TODO: this should probably have ref-counted semantics.
   */
  public abstract void close();

  /**
   * Returns the number of nulls in this column.
   */
  public abstract int numNulls();

  /**
   * Returns true if any of the nulls indicator are set for this column. This can be used
   * as an optimization to prevent setting nulls.
   */
  public abstract boolean anyNullsSet();

  /**
   * Returns the off heap ptr for the arrays backing the NULLs and values buffer. Only valid
   * to call for off heap columns.
   */
  public abstract long nullsNativeAddress();
  public abstract long valuesNativeAddress();

  /**
   * Returns whether the value at rowId is NULL.
   */
  public abstract boolean isNullAt(int rowId);

  /**
   * Returns the value for rowId.
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract boolean[] getBooleans(int rowId, int count);

  /**
   * Returns the value for rowId.
   */
  public abstract byte getByte(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract byte[] getBytes(int rowId, int count);

  /**
   * Returns the value for rowId.
   */
  public abstract short getShort(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract short[] getShorts(int rowId, int count);

  /**
   * Returns the value for rowId.
   */
  public abstract int getInt(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract int[] getInts(int rowId, int count);

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public abstract int getDictId(int rowId);

  /**
   * Returns the value for rowId.
   */
  public abstract long getLong(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract long[] getLongs(int rowId, int count);

  /**
   * Returns the value for rowId.
   */
  public abstract float getFloat(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract float[] getFloats(int rowId, int count);

  /**
   * Returns the value for rowId.
   */
  public abstract double getDouble(int rowId);

  /**
   * Gets values from [rowId, rowId + count)
   */
  public abstract double[] getDoubles(int rowId, int count);

  /**
   * Returns the length of the array at rowid.
   */
  public abstract int getArrayLength(int rowId);

  /**
   * Returns the offset of the array at rowid.
   */
  public abstract int getArrayOffset(int rowId);

  /**
   * Returns a utility object to get structs.
   */
  public ColumnarBatch.Row getStruct(int rowId) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns a utility object to get structs.
   * provided to keep API compatibility with InternalRow for code generation
   */
  public ColumnarBatch.Row getStruct(int rowId, int size) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns the array at rowid.
   */
  public final ColumnVector.Array getArray(int rowId) {
    resultArray.length = getArrayLength(rowId);
    resultArray.offset = getArrayOffset(rowId);
    return resultArray;
  }

  /**
   * Loads the data into array.byteArray.
   */
  public abstract void loadBytes(ColumnVector.Array array);

  /**
   * Returns the value for rowId.
   */
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the decimal for rowId.
   */
  public abstract Decimal getDecimal(int rowId, int precision, int scale);

  /**
   * Returns the UTF8String for rowId.
   */
  public abstract UTF8String getUTF8String(int rowId);

  /**
   * Returns the byte array for rowId.
   */
  public abstract byte[] getBinary(int rowId);

  /**
   * Returns the data for the underlying array.
   */
  public abstract ColumnVector arrayData();

  /**
   * Returns the ordinal's child data column.
   */
  public abstract ColumnVector getChildColumn(int ordinal);

  /**
   * Returns true if this column is an array.
   */
  public final boolean isArray() { return resultArray != null; }

  /**
   * Data type for this column.
   */
  protected DataType type;

  /**
   * Reusable Array holder for getArray().
   */
  protected ColumnVector.Array resultArray;

  /**
   * Reusable Struct holder for getStruct().
   */
  protected ColumnarBatch.Row resultStruct;

  /**
   * The Dictionary for this column.
   *
   * If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /**
   * Reusable column for ids of dictionary.
   */
  protected ColumnVector dictionaryIds;

  /**
   * Returns true if this column has a dictionary.
   */
  public boolean hasDictionary() { return this.dictionary != null; }

  /**
   * Returns the underlying integer column for ids of dictionary.
   */
  public ColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected ColumnVector(DataType type) {
    this.type = type;
  }
}

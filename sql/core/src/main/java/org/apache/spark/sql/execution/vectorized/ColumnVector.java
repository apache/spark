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

import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class represents in-memory values of a column and provides the main APIs to access the data.
 * It supports all the types and contains get APIs as well as their batched versions. The batched
 * versions are considered to be faster and preferable whenever possible.
 *
 * To handle nested schemas, ColumnVector has two types: Arrays and Structs. In both cases these
 * columns have child columns. All of the data are stored in the child columns and the parent column
 * only contains nullability. In the case of Arrays, the lengths and offsets are saved in the child
 * column and are encoded identically to INTs.
 *
 * Maps are just a special case of a two field struct.
 *
 * Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for values
 * in the current batch.
 */
public abstract class ColumnVector implements AutoCloseable {

  /**
   * Returns the data type of this column.
   */
  public final DataType dataType() { return type; }

  /**
   * Cleans up memory for this column. The column is not usable after this.
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
   * Returns the length of the array for rowId.
   */
  public abstract int getArrayLength(int rowId);

  /**
   * Returns the offset of the array for rowId.
   */
  public abstract int getArrayOffset(int rowId);

  /**
   * Returns the struct for rowId.
   */
  public final ColumnarRow getStruct(int rowId) {
    return new ColumnarRow(this, rowId);
  }

  /**
   * A special version of {@link #getShort(int)}, which is only used as an adapter for Spark codegen
   * framework, the second parameter is totally ignored.
   */
  public final ColumnarRow getStruct(int rowId, int size) {
    return getStruct(rowId);
  }

  /**
   * Returns the array for rowId.
   */
  public final ColumnarArray getArray(int rowId) {
    return new ColumnarArray(arrayData(), getArrayOffset(rowId), getArrayLength(rowId));
  }

  /**
   * Returns the map for rowId.
   */
  public MapData getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the decimal for rowId.
   */
  public abstract Decimal getDecimal(int rowId, int precision, int scale);

  /**
   * Returns the UTF8String for rowId. Note that the returned UTF8String may point to the data of
   * this column vector, please copy it if you want to keep it after this column vector is freed.
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
   * Data type for this column.
   */
  protected DataType type;

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected ColumnVector(DataType type) {
    this.type = type;
  }
}

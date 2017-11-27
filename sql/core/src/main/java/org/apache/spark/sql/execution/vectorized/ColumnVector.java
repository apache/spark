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
  public ColumnarRow getStruct(int rowId) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns a utility object to get structs.
   * provided to keep API compatibility with InternalRow for code generation
   */
  public ColumnarRow getStruct(int rowId, int size) {
    resultStruct.rowId = rowId;
    return resultStruct;
  }

  /**
   * Returns the array at rowid.
   */
  public final ColumnarArray getArray(int rowId) {
    resultArray.length = getArrayLength(rowId);
    resultArray.offset = getArrayOffset(rowId);
    return resultArray;
  }

  /**
   * Loads the data into array.byteArray.
   */
  public abstract void loadBytes(ColumnarArray array);

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
  protected ColumnarArray resultArray;

  /**
   * Reusable Struct holder for getStruct().
   */
  protected ColumnarRow resultStruct;

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

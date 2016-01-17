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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.types.DataType;

/**
 * This class represents a column of values and provides the main APIs to access the data
 * values. It supports all the types and contains get/put APIs as well as their batched versions.
 * The batched versions are preferable whenever possible.
 *
 * Most of the APIs take the rowId as a parameter. This is the local 0-based row id for values
 * in the current RowBatch.
 *
 * A ColumnVector should be considered immutable once originally created. In other words, it is not
 * valid to call put APIs after reads until reset() is called.
 */
public abstract class ColumnVector {
  /**
   * Allocates a column with each element of size `width` either on or off heap.
   */
  public static ColumnVector allocate(int capacity, DataType type, MemoryMode mode) {
    if (mode == MemoryMode.OFF_HEAP) {
      return new OffHeapColumnVector(capacity, type);
    } else {
      return new OnHeapColumnVector(capacity, type);
    }
  }

  public final DataType dataType() { return type; }

  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  public void reset() {
    numNulls = 0;
    if (anyNullsSet) {
      putNotNulls(0, capacity);
      anyNullsSet = false;
    }
  }

  /**
   * Cleans up memory for this column. The column is not usable after this.
   * TODO: this should probably have ref-counted semantics.
   */
  public abstract void close();

  /**
   * Returns the number of nulls in this column.
   */
  public final int numNulls() { return numNulls; }

  /**
   * Returns true if any of the nulls indicator are set for this column. This can be used
   * as an optimization to prevent setting nulls.
   */
  public final boolean anyNullsSet() { return anyNullsSet; }

  /**
   * Returns the off heap ptr for the arrays backing the NULLs and values buffer. Only valid
   * to call for off heap columns.
   */
  public abstract long nullsNativeAddress();
  public abstract long valuesNativeAddress();

  /**
   * Sets the value at rowId to null/not null.
   */
  public abstract void putNotNull(int rowId);
  public abstract void putNull(int rowId);

  /**
   * Sets the values from [rowId, rowId + count) to null/not null.
   */
  public abstract void putNulls(int rowId, int count);
  public abstract void putNotNulls(int rowId, int count);

  /**
   * Returns whether the value at rowId is NULL.
   */
  public abstract boolean getIsNull(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putInts(int rowId, int count, int value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the integer for rowId.
   */
  public abstract int getInt(int rowId);

  /**
   * Sets the value at rowId to `value`.
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Sets values from [rowId, rowId + count) to value.
   */
  public abstract void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [rowId, rowId + count) to [src + srcIndex, src + srcIndex + count)
   * src should contain `count` doubles written as ieee format.
   */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [rowId, rowId + count) to [src[srcIndex], src[srcIndex + count])
   * The data in src must be ieee formated doubles.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Returns the double for rowId.
   */
  public abstract double getDouble(int rowId);

  /**
   * Maximum number of rows that can be stored in this column.
   */
  protected final int capacity;

  /**
   * Number of nulls in this column. This is an optimization for the reader, to skip NULL checks.
   */
  protected int numNulls;

  /**
   * True if there is at least one NULL byte set. This is an optimization for the writer, to skip
   * having to clear NULL bits.
   */
  protected boolean anyNullsSet;

  /**
   * Data type for this column.
   */
  protected final DataType type;

  protected ColumnVector(int capacity, DataType type) {
    this.capacity = capacity;
    this.type = type;
  }
}

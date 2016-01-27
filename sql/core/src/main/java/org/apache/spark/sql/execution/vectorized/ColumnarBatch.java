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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.commons.lang.NotImplementedException;

/**
 * This class is the in memory representation of rows as they are streamed through operators. It
 * is designed to maximize CPU efficiency and not storage footprint. Since it is expected that
 * each operator allocates one of thee objects, the storage footprint on the task is negligible.
 *
 * The layout is a columnar with values encoded in their native format. Each RowBatch contains
 * a horizontal partitioning of the data, split into columns.
 *
 * The ColumnarBatch supports either on heap or offheap modes with (mostly) the identical API.
 *
 * TODO:
 *  - There are many TODOs for the existing APIs. They should throw a not implemented exception.
 *  - Compaction: The batch and columns should be able to compact based on a selection vector.
 */
public final class ColumnarBatch {
  private static final int DEFAULT_BATCH_SIZE = 4 * 1024;
  private static MemoryMode DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP;

  private final StructType schema;
  private final int capacity;
  private int numRows;
  private final ColumnVector[] columns;

  // True if the row is filtered.
  private final boolean[] filteredRows;

  // Total number of rows that have been filtered.
  private int numRowsFiltered = 0;

  public static ColumnarBatch allocate(StructType schema, MemoryMode memMode) {
    return new ColumnarBatch(schema, DEFAULT_BATCH_SIZE, memMode);
  }

  public static ColumnarBatch allocate(StructType type) {
    return new ColumnarBatch(type, DEFAULT_BATCH_SIZE, DEFAULT_MEMORY_MODE);
  }

  public static ColumnarBatch allocate(StructType schema, MemoryMode memMode, int maxRows) {
    return new ColumnarBatch(schema, maxRows, memMode);
  }

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allcoations.
   */
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  /**
   * Adapter class to interop with existing components that expect internal row. A lot of
   * performance is lost with this translation.
   */
  public static final class Row extends InternalRow {
    private int rowId;
    private final ColumnarBatch parent;
    private final int fixedLenRowSize;

    private Row(ColumnarBatch parent) {
      this.parent = parent;
      this.fixedLenRowSize = UnsafeRow.calculateFixedPortionByteSize(parent.numCols());
    }

    /**
     * Marks this row as being filtered out. This means a subsequent iteration over the rows
     * in this batch will not include this row.
     */
    public final void markFiltered() {
      parent.markFiltered(rowId);
    }

    @Override
    public final int numFields() {
      return parent.numCols();
    }

    @Override
    /**
     * Revisit this. This is expensive.
     */
    public final InternalRow copy() {
      UnsafeRow row = new UnsafeRow(parent.numCols());
      row.pointTo(new byte[fixedLenRowSize], fixedLenRowSize);
      for (int i = 0; i < parent.numCols(); i++) {
        if (isNullAt(i)) {
          row.setNullAt(i);
        } else {
          DataType dt = parent.schema.fields()[i].dataType();
          if (dt instanceof IntegerType) {
            row.setInt(i, getInt(i));
          } else if (dt instanceof LongType) {
            row.setLong(i, getLong(i));
          } else if (dt instanceof DoubleType) {
            row.setDouble(i, getDouble(i));
          } else {
            throw new RuntimeException("Not implemented.");
          }
        }
      }
      return row;
    }

    @Override
    public final boolean anyNull() {
      throw new NotImplementedException();
    }

    @Override
    public final boolean isNullAt(int ordinal) {
      return parent.column(ordinal).getIsNull(rowId);
    }

    @Override
    public final boolean getBoolean(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final byte getByte(int ordinal) { return parent.column(ordinal).getByte(rowId); }

    @Override
    public final short getShort(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final int getInt(int ordinal) {
      return parent.column(ordinal).getInt(rowId);
    }

    @Override
    public final long getLong(int ordinal) { return parent.column(ordinal).getLong(rowId); }

    @Override
    public final float getFloat(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final double getDouble(int ordinal) {
      return parent.column(ordinal).getDouble(rowId);
    }

    @Override
    public final Decimal getDecimal(int ordinal, int precision, int scale) {
      throw new NotImplementedException();
    }

    @Override
    public final UTF8String getUTF8String(int ordinal) {
      ColumnVector.Array a = parent.column(ordinal).getByteArray(rowId);
      return UTF8String.fromBytes(a.byteArray, a.byteArrayOffset, a.length);
    }

    @Override
    public final byte[] getBinary(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final CalendarInterval getInterval(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final InternalRow getStruct(int ordinal, int numFields) {
      return parent.column(ordinal).getStruct(rowId);
    }

    @Override
    public final ArrayData getArray(int ordinal) {
      return parent.column(ordinal).getArray(rowId);
    }

    @Override
    public final MapData getMap(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public final Object get(int ordinal, DataType dataType) {
      throw new NotImplementedException();
    }
  }

  /**
   * Returns an iterator over the rows in this batch. This skips rows that are filtered out.
   */
  public Iterator<Row> rowIterator() {
    final int maxRows = ColumnarBatch.this.numRows();
    final Row row = new Row(this);
    return new Iterator<Row>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        return rowId < maxRows;
      }

      @Override
      public Row next() {
        assert(hasNext());
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Resets the batch for writing.
   */
  public void reset() {
    for (int i = 0; i < numCols(); ++i) {
      columns[i].reset();
    }
    if (this.numRowsFiltered > 0) {
      Arrays.fill(filteredRows, false);
    }
    this.numRows = 0;
    this.numRowsFiltered = 0;
  }

  /**
   * Sets the number of rows that are valid.
   */
  public void setNumRows(int numRows) {
    assert(numRows <= this.capacity);
    this.numRows = numRows;
  }

  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() { return columns.length; }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() { return numRows; }

  /**
   * Returns the number of valid rowss.
   */
  public int numValidRows() {
    assert(numRowsFiltered <= numRows);
    return numRows - numRowsFiltered;
  }

  /**
   * Returns the max capacity (in number of rows) for this batch.
   */
  public int capacity() { return capacity; }

  /**
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * Marks this row as being filtered out. This means a subsequent iteration over the rows
   * in this batch will not include this row.
   */
  public final void markFiltered(int rowId) {
    assert(filteredRows[rowId] == false);
    filteredRows[rowId] = true;
    ++numRowsFiltered;
  }

  private ColumnarBatch(StructType schema, int maxRows, MemoryMode memMode) {
    this.schema = schema;
    this.capacity = maxRows;
    this.columns = new ColumnVector[schema.size()];
    this.filteredRows = new boolean[maxRows];

    for (int i = 0; i < schema.fields().length; ++i) {
      StructField field = schema.fields()[i];
      columns[i] = ColumnVector.allocate(maxRows, field.dataType(), memMode);
    }
  }
}

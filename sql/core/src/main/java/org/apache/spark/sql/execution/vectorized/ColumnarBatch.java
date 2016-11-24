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
import java.util.*;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * This class is the in memory representation of rows as they are streamed through operators. It
 * is designed to maximize CPU efficiency and not storage footprint. Since it is expected that
 * each operator allocates one of these objects, the storage footprint on the task is negligible.
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

  // Column indices that cannot have null values.
  private final Set<Integer> nullFilteredColumns;

  // Total number of rows that have been filtered.
  private int numRowsFiltered = 0;

  // Staging row returned from getRow.
  final Row row;

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
   * calling this. This must be called at the end to clean up memory allocations.
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
  public static final class Row extends MutableRow {
    protected int rowId;
    private final ColumnarBatch parent;
    private final int fixedLenRowSize;
    private final ColumnVector[] columns;

    // Ctor used if this is a top level row.
    private Row(ColumnarBatch parent) {
      this.parent = parent;
      this.fixedLenRowSize = UnsafeRow.calculateFixedPortionByteSize(parent.numCols());
      this.columns = parent.columns;
    }

    // Ctor used if this is a struct.
    protected Row(ColumnVector[] columns) {
      this.parent = null;
      this.fixedLenRowSize = UnsafeRow.calculateFixedPortionByteSize(columns.length);
      this.columns = columns;
    }

    /**
     * Marks this row as being filtered out. This means a subsequent iteration over the rows
     * in this batch will not include this row.
     */
    public void markFiltered() {
      parent.markFiltered(rowId);
    }

    public ColumnVector[] columns() { return columns; }

    @Override
    public int numFields() { return columns.length; }

    @Override
    /**
     * Revisit this. This is expensive. This is currently only used in test paths.
     */
    public InternalRow copy() {
      GenericMutableRow row = new GenericMutableRow(columns.length);
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
            row.update(i, getUTF8String(i));
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
      final int months = columns[ordinal].getChildColumn(0).getInt(rowId);
      final long microseconds = columns[ordinal].getChildColumn(1).getLong(rowId);
      return new CalendarInterval(months, microseconds);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return columns[ordinal].getStruct(rowId);
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return columns[ordinal].getArray(rowId);
    }

    @Override
    public MapData getMap(int ordinal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new UnsupportedOperationException();
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
        } else if (dt instanceof DecimalType) {
          DecimalType t = (DecimalType) dt;
          setDecimal(ordinal, Decimal.apply((BigDecimal) value, t.precision(), t.scale()),
              t.precision());
        } else {
          throw new UnsupportedOperationException("Datatype not supported " + dt);
        }
      }
    }

    @Override
    public void setNullAt(int ordinal) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNull(rowId);
    }

    @Override
    public void setBoolean(int ordinal, boolean value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putBoolean(rowId, value);
    }

    @Override
    public void setByte(int ordinal, byte value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putByte(rowId, value);
    }

    @Override
    public void setShort(int ordinal, short value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putShort(rowId, value);
    }

    @Override
    public void setInt(int ordinal, int value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putInt(rowId, value);
    }

    @Override
    public void setLong(int ordinal, long value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putLong(rowId, value);
    }

    @Override
    public void setFloat(int ordinal, float value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putFloat(rowId, value);
    }

    @Override
    public void setDouble(int ordinal, double value) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putDouble(rowId, value);
    }

    @Override
    public void setDecimal(int ordinal, Decimal value, int precision) {
      assert (!columns[ordinal].isConstant);
      columns[ordinal].putNotNull(rowId);
      columns[ordinal].putDecimal(rowId, value, precision);
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
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
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
   * Sets the number of rows that are valid. Additionally, marks all rows as "filtered" if one or
   * more of their attributes are part of a non-nullable column.
   */
  public void setNumRows(int numRows) {
    assert(numRows <= this.capacity);
    this.numRows = numRows;

    for (int ordinal : nullFilteredColumns) {
      if (columns[ordinal].numNulls != 0) {
        for (int rowId = 0; rowId < numRows; rowId++) {
          if (!filteredRows[rowId] && columns[ordinal].isNullAt(rowId)) {
            filteredRows[rowId] = true;
            ++numRowsFiltered;
          }
        }
      }
    }
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
   * Returns the number of valid rows.
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
   * Sets (replaces) the column at `ordinal` with column. This can be used to do very efficient
   * projections.
   */
  public void setColumn(int ordinal, ColumnVector column) {
    if (column instanceof OffHeapColumnVector) {
      throw new UnsupportedOperationException("Need to ref count columns.");
    }
    columns[ordinal] = column;
  }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public ColumnarBatch.Row getRow(int rowId) {
    assert(rowId >= 0);
    assert(rowId < numRows);
    row.rowId = rowId;
    return row;
  }

  /**
   * Marks this row as being filtered out. This means a subsequent iteration over the rows
   * in this batch will not include this row.
   */
  public void markFiltered(int rowId) {
    assert(!filteredRows[rowId]);
    filteredRows[rowId] = true;
    ++numRowsFiltered;
  }

  /**
   * Marks a given column as non-nullable. Any row that has a NULL value for the corresponding
   * attribute is filtered out.
   */
  public void filterNullsInColumn(int ordinal) {
    nullFilteredColumns.add(ordinal);
  }

  private ColumnarBatch(StructType schema, int maxRows, MemoryMode memMode) {
    this.schema = schema;
    this.capacity = maxRows;
    this.columns = new ColumnVector[schema.size()];
    this.nullFilteredColumns = new HashSet<>();
    this.filteredRows = new boolean[maxRows];

    for (int i = 0; i < schema.fields().length; ++i) {
      StructField field = schema.fields()[i];
      columns[i] = ColumnVector.allocate(maxRows, field.dataType(), memMode);
    }

    this.row = new Row(this);
  }
}

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

import java.util.*;

import org.apache.spark.sql.types.StructType;

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
  public static final int DEFAULT_BATCH_SIZE = 4 * 1024;

  private final StructType schema;
  private final int capacity;
  private int numRows;
  final ColumnVector[] columns;

  // True if the row is filtered.
  private final boolean[] filteredRows;

  // Column indices that cannot have null values.
  private final Set<Integer> nullFilteredColumns;

  // Total number of rows that have been filtered.
  private int numRowsFiltered = 0;

  // Staging row returned from getRow.
  final VectorBasedRow row;

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
   * Returns an iterator over the rows in this batch. This skips rows that are filtered out.
   */
  public Iterator<VectorBasedRow> rowIterator() {
    final int maxRows = ColumnarBatch.this.numRows();
    final VectorBasedRow row = new VectorBasedRow(this);
    return new Iterator<VectorBasedRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        while (rowId < maxRows && ColumnarBatch.this.filteredRows[rowId]) {
          ++rowId;
        }
        return rowId < maxRows;
      }

      @Override
      public VectorBasedRow next() {
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
      if (columns[i] instanceof WritableColumnVector) {
        ((WritableColumnVector) columns[i]).reset();
      }
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
      if (columns[ordinal].numNulls() != 0) {
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
   * Returns the schema that makes up this batch.
   */
  public StructType schema() { return schema; }

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
  public VectorBasedRow getRow(int rowId) {
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

  public ColumnarBatch(StructType schema, ColumnVector[] columns, int capacity) {
    this.schema = schema;
    this.columns = columns;
    this.capacity = capacity;
    this.nullFilteredColumns = new HashSet<>();
    this.filteredRows = new boolean[capacity];
    this.row = new VectorBasedRow(this);
  }
}

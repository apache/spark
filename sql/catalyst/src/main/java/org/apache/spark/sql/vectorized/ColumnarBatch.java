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

import java.util.*;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this
 * batch so that Spark can access the data row by row. Instance of it is meant to be reused during
 * the entire data loading process. A data source may extend this class with customized logic.
 */
@DeveloperApi
public class ColumnarBatch implements AutoCloseable {
  protected int numRows;
  protected final ColumnVector[] columns;

  // Staging row returned from `getRow`.
  protected final ColumnarBatchRow row;

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  @Override
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  /**
   * Returns an iterator over the rows in this batch.
   */
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final ColumnarBatchRow row = new ColumnarBatchRow(columns);
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
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
   * Sets the number of rows in this batch.
   */
  public void setNumRows(int numRows) {
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
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < numRows);
    row.rowId = rowId;
    return row;
  }

  public ColumnarBatch(ColumnVector[] columns) {
    this(columns, 0);
  }

  /**
   * Create a new batch from existing column vectors.
   * @param columns The columns of this batch
   * @param numRows The number of rows in this batch
   */
  public ColumnarBatch(ColumnVector[] columns, int numRows) {
    this.columns = columns;
    this.numRows = numRows;
    this.row = new ColumnarBatchRow(columns);
  }
}

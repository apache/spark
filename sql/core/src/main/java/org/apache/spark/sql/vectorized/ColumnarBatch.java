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

import org.apache.spark.annotation.Evolving;

/**
 * This class wraps multiple {@link ColumnVector}s as a table-like data batch. Instance of it is
 * meant to be reused during the entire data loading process.
 */
@Evolving
public final class ColumnarBatch {
  private int numRows;
  private final ColumnVector[] columns;

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
   * Returns all the columns of this batch.
   */
  public ColumnVector[] columns() {
    return columns;
  }

  public ColumnarBatch(ColumnVector[] columns) {
    this.columns = columns;
  }
}

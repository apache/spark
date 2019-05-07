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
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class provides a row view of a {@link ColumnarBatch}, so that Spark can access the data
 * row by row
 */
public final class ColumnarBatchRowView {

  private final ColumnarBatch batch;

  // Staging row returned from `getRow`.
  private final MutableColumnarRow row;

  public ColumnarBatchRowView(ColumnarBatch batch) {
    this.batch = batch;
    this.row = new MutableColumnarRow(batch.columns());
  }

  /**
   * Returns an iterator over the rows in this batch.
   */
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = batch.numRows();
    final MutableColumnarRow row = new MutableColumnarRow(batch.columns());
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
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < batch.numRows());
    row.rowId = rowId;
    return row;
  }
}

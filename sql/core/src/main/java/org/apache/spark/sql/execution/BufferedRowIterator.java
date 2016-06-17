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

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.LinkedList;

import scala.collection.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.vectorized.ColumnVector;

/**
 * An iterator interface used to pull the output from generated function for multiple operators
 * (whole stage codegen).
 */
public abstract class BufferedRowIterator {
  protected LinkedList<InternalRow> currentRows = new LinkedList<>();
  protected ColumnarBatch columnarBatch;
  protected java.util.Iterator<ColumnarBatch.Row> rowIterator;
  protected boolean isColumnarBatchAccessed = false;
  // used when there is no column in output
  protected UnsafeRow unsafeRow = new UnsafeRow(0);
  private long startTimeNs = System.nanoTime();

  protected int partitionIndex = -1;

  public boolean hasNext() throws IOException {
    if (currentRows.isEmpty()) {
      processNext();
    }
    if (!isColumnarBatchAccessed) { return !currentRows.isEmpty(); }
    if (rowIterator == null) { rowIterator = columnarBatch.rowIterator(); }
    return rowIterator.hasNext();
  }

  public InternalRow next() {
    if (!isColumnarBatchAccessed) { return currentRows.remove(); }
    if (rowIterator == null) { rowIterator = columnarBatch.rowIterator(); }
    return rowIterator.next().copyUnsafeRow();
  }

  /**
   * Returns the elapsed time since this object is created. This object represents a pipeline so
   * this is a measure of how long the pipeline has been running.
   */
  public long durationMs() {
    return (System.nanoTime() - startTimeNs) / (1000 * 1000);
  }

  /**
   * Initializes from array of iterators of InternalRow.
   */
  public abstract void init(int index, Iterator<InternalRow>[] iters);

  /**
   * Append a row to currentRows.
   */
  protected void append(InternalRow row) {
    currentRows.add(row);
  }

  /**
   * Returns whether `processNext()` should stop processing next row from `input` or not.
   *
   * If it returns true, the caller should exit the loop (return from processNext()).
   */
  protected boolean shouldStop() {
    if (!isColumnarBatchAccessed) { return !currentRows.isEmpty(); }
    return false;  // TODO: currentIdx < allocated # of rows of CS
  }

  protected boolean isColumnarBatch() { return this.columnarBatch != null; }
  protected int numColumns() { return this.columnarBatch.numCols(); }
  protected int numRows() { return this.columnarBatch.numRows(); }
  protected ColumnVector column(int i) { return this.columnarBatch.column(i); }
  protected void registerColumnarBatch(ColumnarBatch columnarBatch) { this.columnarBatch = columnarBatch; }

  /**
   * Increase the peak execution memory for current task.
   */
  protected void incPeakExecutionMemory(long size) {
    TaskContext.get().taskMetrics().incPeakExecutionMemory(size);
  }

  /**
   * Processes the input until have a row as output (currentRow).
   *
   * After it's called, if currentRow is still null, it means no more rows left.
   */
  protected abstract void processNext() throws IOException;
}

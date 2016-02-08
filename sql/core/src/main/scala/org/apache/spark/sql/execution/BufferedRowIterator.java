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

/**
 * An iterator interface used to pull the output from generated function for multiple operators
 * (whole stage codegen).
 *
 * TODO: replaced it by batched columnar format.
 */
public class BufferedRowIterator {
  protected LinkedList<InternalRow> currentRows = new LinkedList<>();
  protected Iterator<InternalRow> input;
  // used when there is no column in output
  protected UnsafeRow unsafeRow = new UnsafeRow(0);

  public boolean hasNext() throws IOException {
    if (currentRows.isEmpty()) {
      processNext();
    }
    return !currentRows.isEmpty();
  }

  public InternalRow next() {
    return currentRows.remove();
  }

  public void setInput(Iterator<InternalRow> iter) {
    input = iter;
  }

  /**
   * Returns whether `processNext()` should stop processing next row from `input` or not.
   *
   * If it returns true, the caller should exit the loop (return from processNext()).
   */
  protected boolean shouldStop() {
    return !currentRows.isEmpty();
  }

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
  protected void processNext() throws IOException {
    if (input.hasNext()) {
      currentRows.add(input.next());
    }
  }
}

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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.parquet.column.ColumnDescriptor;

import java.util.PrimitiveIterator;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 */
final class ParquetReadState {
  /** The row indexes to include, only not-null if the column index is present. */
  private final PrimitiveIterator.OfLong rowIndexes;

  /**
   * The current row range, as its bounds rather than as an object: one range per surviving row is
   * what a filter with scattered survivors produces, for every column reader of every row group.
   *
   * <p>With no row indexes they are the whole range, since every row must be included. Once the
   * indexes are exhausted they are inverted, which says that every row from there on is to be
   * skipped.
   */
  private long currentRangeStart;
  private long currentRangeEnd;

  /** The row index that ended the current range by not continuing it, so it starts the next one. */
  private long pendingRowIndex;
  private boolean hasPendingRowIndex;

  /** Maximum repetition level for the Parquet column */
  final int maxRepetitionLevel;

  /** Maximum definition level for the Parquet column */
  final int maxDefinitionLevel;

  /** Whether this column is required */
  final boolean isRequired;

  /** The current index over all rows within the column chunk. This is used to check if the
   * current row should be skipped by comparing against the row ranges. */
  long rowId;

  /** The offset in the current batch to put the next value in value vector */
  int valueOffset;

  /** The offset in the current batch to put the next value in repetition & definition vector */
  int levelOffset;

  /** The remaining number of values to read in the current page */
  int valuesToReadInPage;

  /** The remaining number of rows to read in the current batch */
  int rowsToReadInBatch;


  /* The following fields are only used when reading repeated values */

  /** When processing repeated values, whether we've found the beginning of the first list after the
   *  current batch. */
  boolean lastListCompleted;

  /** When processing repeated types, the number of accumulated definition levels to process */
  int numBatchedDefLevels;

  /** When processing repeated types, whether we should skip the current batch of definition
   * levels. */
  boolean shouldSkip;

  ParquetReadState(
      ColumnDescriptor descriptor,
      boolean isRequired,
      PrimitiveIterator.OfLong rowIndexes) {
    this.maxRepetitionLevel = descriptor.getMaxRepetitionLevel();
    this.maxDefinitionLevel = descriptor.getMaxDefinitionLevel();
    this.isRequired = isRequired;
    this.rowIndexes = rowIndexes;
    nextRange();
  }

  /**
   * Must be called at the beginning of reading a new batch.
   */
  void resetForNewBatch(int batchSize) {
    this.valueOffset = 0;
    this.levelOffset = 0;
    this.rowsToReadInBatch = batchSize;
    this.lastListCompleted = this.maxRepetitionLevel == 0; // always true for non-repeated column
    this.numBatchedDefLevels = 0;
    this.shouldSkip = false;
  }

  /**
   * Must be called at the beginning of reading a new page.
   */
  void resetForNewPage(int totalValuesInPage, long pageFirstRowIndex) {
    this.valuesToReadInPage = totalValuesInPage;
    this.rowId = pageFirstRowIndex;
  }

  /**
   * Returns the start index of the current row range.
   */
  long currentRangeStart() {
    return currentRangeStart;
  }

  /**
   * Returns the end index of the current row range.
   */
  long currentRangeEnd() {
    return currentRangeEnd;
  }

  /**
   * Advances to the next range, coalescing the run of ascending row indexes that forms it. For
   * example `[0, 1, 2, 4, 5, 7, 8, 9]` yields `[0-2]`, then `[4-5]`, then `[7-9]`.
   *
   * <p>One range at a time on purpose. They are consumed once, in order, so holding them all buys
   * nothing and costs a list per column reader of the row group, which for scattered survivors is
   * one entry per row in every one of those lists.
   */
  void nextRange() {
    if (rowIndexes == null) {
      currentRangeStart = Long.MIN_VALUE;
      currentRangeEnd = Long.MAX_VALUE;
      return;
    }
    if (!hasPendingRowIndex && !rowIndexes.hasNext()) {
      currentRangeStart = Long.MAX_VALUE;
      currentRangeEnd = Long.MIN_VALUE;
      return;
    }
    long start = hasPendingRowIndex ? pendingRowIndex : rowIndexes.nextLong();
    hasPendingRowIndex = false;
    long end = start;
    // A range can only be closed by seeing the index that does not continue it, so that index is
    // held back for the next call.
    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (idx == end + 1) {
        end = idx;
      } else {
        pendingRowIndex = idx;
        hasPendingRowIndex = true;
        break;
      }
    }
    currentRangeStart = start;
    currentRangeEnd = end;
  }
}

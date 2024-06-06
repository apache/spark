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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 */
final class ParquetReadState {
  /** A special row range used when there is no row indexes (hence all rows must be included) */
  private static final RowRange MAX_ROW_RANGE = new RowRange(Long.MIN_VALUE, Long.MAX_VALUE);

  /**
   * A special row range used when the row indexes are present AND all the row ranges have been
   * processed. This serves as a sentinel at the end indicating that all rows come after the last
   * row range should be skipped.
   */
  private static final RowRange END_ROW_RANGE = new RowRange(Long.MAX_VALUE, Long.MIN_VALUE);

  /** Iterator over all row ranges, only not-null if column index is present */
  private final Iterator<RowRange> rowRanges;

  /** The current row range */
  private RowRange currentRange;

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
    this.rowRanges = constructRanges(rowIndexes);
    nextRange();
  }

  /**
   * Construct a list of row ranges from the given `rowIndexes`. For example, suppose the
   * `rowIndexes` are `[0, 1, 2, 4, 5, 7, 8, 9]`, it will be converted into 3 row ranges:
   * `[0-2], [4-5], [7-9]`.
   */
  private Iterator<RowRange> constructRanges(PrimitiveIterator.OfLong rowIndexes) {
    if (rowIndexes == null) {
      return null;
    }

    List<RowRange> rowRanges = new ArrayList<>();
    long currentStart = Long.MIN_VALUE;
    long previous = Long.MIN_VALUE;

    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (currentStart == Long.MIN_VALUE) {
        currentStart = idx;
      } else if (previous + 1 != idx) {
        RowRange range = new RowRange(currentStart, previous);
        rowRanges.add(range);
        currentStart = idx;
      }
      previous = idx;
    }

    if (previous != Long.MIN_VALUE) {
      rowRanges.add(new RowRange(currentStart, previous));
    }

    return rowRanges.iterator();
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
    return currentRange.start;
  }

  /**
   * Returns the end index of the current row range.
   */
  long currentRangeEnd() {
    return currentRange.end;
  }

  /**
   * Advance to the next range.
   */
  void nextRange() {
    if (rowRanges == null) {
      currentRange = MAX_ROW_RANGE;
    } else if (!rowRanges.hasNext()) {
      currentRange = END_ROW_RANGE;
    } else {
      currentRange = rowRanges.next();
    }
  }

  /**
   * Helper struct to represent a range of row indexes `[start, end]`.
   */
  private record RowRange(long start, long end) {
  }
}

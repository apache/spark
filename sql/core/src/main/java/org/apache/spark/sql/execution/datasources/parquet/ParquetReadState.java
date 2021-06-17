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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 */
final class ParquetReadState {
  private static final RowRange MAX_ROW_RANGE = new RowRange(Long.MIN_VALUE, Long.MAX_VALUE);
  private static final RowRange MIN_ROW_RANGE = new RowRange(Long.MAX_VALUE, Long.MIN_VALUE);

  /** Iterator over all row ranges, only not-null if column index is present */
  private final Iterator<RowRange> rowRanges;

  /** The current row range */
  private RowRange currentRange;

  /** Maximum definition level */
  final int maxDefinitionLevel;

  /** The current index overall all rows within the column chunk. This is used to check if the
   * current row should be skipped by comparing the index against the row ranges. */
  long rowId;

  /** The offset in the current batch to put the next value */
  int offset;

  /** The remaining number of values to read in the current page */
  int valuesToReadInPage;

  /** The remaining number of values to read in the current batch */
  int valuesToReadInBatch;

  ParquetReadState(int maxDefinitionLevel, PrimitiveIterator.OfLong rowIndexes) {
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.rowRanges = rowIndexes == null ? null : constructRanges(rowIndexes);
    nextRange();
  }

  private Iterator<RowRange> constructRanges(PrimitiveIterator.OfLong rowIndexes) {
    List<RowRange> rowRanges = new ArrayList<>();
    long currentStart, previous;
    currentStart = previous = Long.MIN_VALUE;

    while (rowIndexes.hasNext()) {
      long idx = rowIndexes.nextLong();
      if (previous == Long.MIN_VALUE) {
        currentStart = previous = idx;
      } else if (previous + 1 != idx) {
        RowRange range = new RowRange(currentStart, previous);
        rowRanges.add(range);
        currentStart = previous = idx;
      } else {
        previous = idx;
      }
    }

    if (previous != Long.MIN_VALUE) {
      rowRanges.add(new RowRange(currentStart, previous));
    }

    return rowRanges.iterator();
  }

  /**
   * Called at the beginning of reading a new batch.
   */
  void resetForBatch(int batchSize) {
    this.offset = 0;
    this.valuesToReadInBatch = batchSize;
  }

  /**
   * Called at the beginning of reading a new page.
   */
  void resetForPage(int totalValuesInPage, long pageFirstRowIndex) {
    this.valuesToReadInPage = totalValuesInPage;
    this.rowId = pageFirstRowIndex;
  }

  /**
   * Returns whether there are more values to read in the current page.
   */
  boolean hasMoreInPage(int newOffset, long newRowId) {
    return newRowId - rowId < valuesToReadInPage && newOffset - offset < valuesToReadInBatch;
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
   * Advance the current offset and rowId to the new values.
   */
  void advanceOffset(int newOffset, long newRowId) {
    valuesToReadInBatch -= (newOffset - offset);
    valuesToReadInPage -= (newRowId - rowId);
    offset = newOffset;
    rowId = newRowId;
  }

  void nextRange() {
    if (rowRanges == null) {
      currentRange = MAX_ROW_RANGE;
    } else {
      if (!rowRanges.hasNext()) {
        currentRange = MIN_ROW_RANGE;
      } else {
        currentRange = rowRanges.next();
      }
    }
  }

  /**
   * Helper struct to represent a range of row indexes `[start, end]`.
   */
  private static class RowRange {
    long start;
    long end;

    RowRange(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }
}

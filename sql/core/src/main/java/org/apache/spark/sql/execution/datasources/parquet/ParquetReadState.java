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
import org.apache.parquet.filter2.columnindex.RowRanges;

import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 *
 * <p>There is one subclass per way of saying which rows to include, which is every row of the
 * chunk, the ranges the caller of the read already held, or the one row index per row that a page
 * store describes parquet's own filtering with. {@link #forRead} picks between them. Everything
 * else about a read is the same whichever it is, and lives here.
 */
abstract class ParquetReadState {
  /**
   * The current row range, as its bounds rather than as whatever object they were read out of, so
   * that the check the value readers make per run of levels touches nothing else. Inverted bounds
   * say every row from here on is to be skipped.
   */
  private long currentRangeStart;
  private long currentRangeEnd;

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

  private ParquetReadState(ColumnDescriptor descriptor, boolean isRequired) {
    this.maxRepetitionLevel = descriptor.getMaxRepetitionLevel();
    this.maxDefinitionLevel = descriptor.getMaxDefinitionLevel();
    this.isRequired = isRequired;
  }

  /**
   * The state for reading {@code descriptor}, told which rows to include the best way the caller
   * can say it.
   *
   * <p>{@code rowRanges} is the rows the read was asked for, when whoever asked knew them. A caller
   * that filtered the rows itself holds their ranges, and every column reader of the row group can
   * then walk that one list. Null says it does not, and the page store is left to describe its
   * rows, which it can only do as {@code rowIndexes}, one index per row. No row indexes in turn
   * means every row of the chunk is included, which is why ranges without them are refused.
   */
  static ParquetReadState forRead(
      ColumnDescriptor descriptor,
      boolean isRequired,
      RowRanges rowRanges,
      PrimitiveIterator.OfLong rowIndexes) {
    ParquetReadState state;
    if (rowRanges != null) {
      if (rowIndexes == null) {
        // Only a store parquet filtered itself indexes its rows, and only its pages carry the first
        // row index that places them in the row group. Pages read whole would each place their
        // first row at 0, and the ranges would then select the wrong rows with no error.
        throw new IllegalStateException(String.format(
            "Row ranges were given for column %s, but its pages carry no row indexes to find "
                + "those rows by", descriptor));
      }
      state = new RangeListState(descriptor, isRequired, rowRanges.getRanges());
    } else if (rowIndexes == null) {
      state = new AllRowsState(descriptor, isRequired);
    } else {
      state = new RowIndexState(descriptor, isRequired, rowIndexes);
    }
    // Here rather than in the constructors, because reading the first range needs the state each
    // subclass sets, and a constructor calling `nextRange` would be calling into its own subclass.
    state.nextRange();
    return state;
  }

  /**
   * Must be called at the beginning of reading a new batch.
   */
  final void resetForNewBatch(int batchSize) {
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
  final void resetForNewPage(int totalValuesInPage, long pageFirstRowIndex) {
    this.valuesToReadInPage = totalValuesInPage;
    this.rowId = pageFirstRowIndex;
  }

  /**
   * Returns the start index of the current row range.
   */
  final long currentRangeStart() {
    return currentRangeStart;
  }

  /**
   * Returns the end index of the current row range.
   */
  final long currentRangeEnd() {
    return currentRangeEnd;
  }

  /** Advances to the next range of rows to include. */
  abstract void nextRange();

  final void includeRange(long from, long to) {
    currentRangeStart = from;
    currentRangeEnd = to;
  }

  /** Inverts the bounds, which says that every row from here on is to be skipped. */
  final void includeNothingMore() {
    includeRange(Long.MAX_VALUE, Long.MIN_VALUE);
  }

  /** Every row of the chunk is included, so the range is set once and never moves. */
  private static final class AllRowsState extends ParquetReadState {
    AllRowsState(ColumnDescriptor descriptor, boolean isRequired) {
      super(descriptor, isRequired);
    }

    @Override
    void nextRange() {
      includeRange(Long.MIN_VALUE, Long.MAX_VALUE);
    }
  }

  /**
   * Walks the caller's range list with a cursor, so a range costs one lookup however many rows it
   * covers, and the list itself is built once for the whole row group.
   */
  private static final class RangeListState extends ParquetReadState {
    private final List<RowRanges.Range> ranges;
    private int nextRangeIndex;

    RangeListState(
        ColumnDescriptor descriptor, boolean isRequired, List<RowRanges.Range> ranges) {
      super(descriptor, isRequired);
      this.ranges = ranges;
    }

    @Override
    void nextRange() {
      if (nextRangeIndex == ranges.size()) {
        includeNothingMore();
      } else {
        RowRanges.Range range = ranges.get(nextRangeIndex++);
        includeRange(range.from, range.to);
      }
    }
  }

  /**
   * Coalesces the runs of ascending row indexes into one range each, so `[0, 1, 2, 4, 5, 7, 8, 9]`
   * yields `[0-2]`, then `[4-5]`, then `[7-9]`.
   *
   * <p>One range at a time on purpose. They are consumed once, in order, so holding them all buys
   * nothing and costs a list per column reader of the row group, which for scattered rows is one
   * entry per row in every one of those lists.
   */
  private static final class RowIndexState extends ParquetReadState {
    private final PrimitiveIterator.OfLong rowIndexes;

    /** The row index that ended the previous range by not continuing it, so it starts the next. */
    private long pendingRowIndex;
    private boolean hasPendingRowIndex;

    RowIndexState(
        ColumnDescriptor descriptor, boolean isRequired, PrimitiveIterator.OfLong rowIndexes) {
      super(descriptor, isRequired);
      this.rowIndexes = rowIndexes;
    }

    @Override
    void nextRange() {
      if (!hasPendingRowIndex && !rowIndexes.hasNext()) {
        includeNothingMore();
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
      includeRange(start, end);
    }
  }
}

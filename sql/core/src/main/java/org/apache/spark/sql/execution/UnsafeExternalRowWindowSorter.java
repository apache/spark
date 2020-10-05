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

import com.google.common.annotations.VisibleForTesting;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.io.IOException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Iterator;
import scala.math.Ordering;

import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;

public final class UnsafeExternalRowWindowSorter extends AbstractUnsafeExternalRowSorter {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeExternalRowWindowSorter.class);

  private final StructType schema;
  private final UnsafeProjection partitionSpecProjection;
  private final Ordering<InternalRow> orderingOfPartitionKey;
  private final Ordering<InternalRow> orderingInWindow;
  private final Ordering<InternalRow> orderingAcrossWindows;
  private final PrefixComparator prefixComparatorInWindow;
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputerInWindow;
  private final boolean canUseRadixSortInWindow;
  private final long pageSizeBytes;
  private static final int windowSorterMapMaxSize = 3;
  private static final int totalNumSorters = windowSorterMapMaxSize + 1;
  private final HashMap<UnsafeRow,AbstractUnsafeExternalRowSorter> windowSorterMap;
  private final UnsafeExternalRowSorter mainSorter;
  private final RowComparator partitionKeyComparator;

  private long numRowsInserted = 0;

  private UnsafeExternalRowSorter createUnsafeExternalRowSorterForWindow() throws IOException {
    UnsafeExternalRowSorter sorter = null;
    try {
      if (this.orderingInWindow == null) {
        sorter = UnsafeExternalRowSorter.createWithRecordComparator(
          this.schema,
          (Supplier<RecordComparator>)null,
          prefixComparatorInWindow,
          prefixComputerInWindow,
          pageSizeBytes/totalNumSorters,
          false);
      } else {
        sorter = UnsafeExternalRowSorter.create(
          this.schema,
          this.orderingInWindow,
          this.prefixComparatorInWindow,
          this.prefixComputerInWindow,
          this.pageSizeBytes/totalNumSorters,
          this.canUseRadixSortInWindow);
      }
    } catch (SparkOutOfMemoryError e) {
      logger.error("Unable to create UnsafeExternalRowSorter due to SparkOutOfMemoryError.");
      return null;
    }
    return sorter;
  }

  /**
  * Returns an UnsafeExternalRowWindowSorter object.
  * @param  schema  The schema of each input row
  * @param  partitionSpecProjection an UnsafeProjection object created from
  *                                 a sequence of partition expressions
  * @param  orderingOfPartitionKey an ordering of internal rows that compares
  *                                internal rows based on a partition key
  * @param orderingInWindow an ordering of internal rows inside a window
  * @param orderingAcrossWindows an ordering of internal rows across different
  *                              windows on a Spark physical partition. This
  *                              ordering is obtained from a partition key plus
  *                              an ordering inside a window
  * @param prefixComparatorInWindow a prefix comparator for sorting rows in a window
  * @param prefixComparatorAcrossWindows a prefix comparator for sorting rows across
  *                                      different windows on a Spark physical partition
  * @param prefixComputerInWindow a prefix computer to calculate the prefix of a row
  *                               based on the sort order inside a window
  * @param prefixComputerAcrossWindows a prefix computer to calculate the prefix of a row
  *                                    based on the sort order across different windows on
  *                                    a Spark physical partition
  * @param canUseRadixSortInWindow whether to use radix sort to sort the rows inside
  *                                a window
  * @param canUseRadixSortAcrossWindows whether to use radix sort to sort the rows across
  *                                     different windows on a Spark physical partition
  * @param pageSizeBytes the size of a page in bytes
  */
  public static UnsafeExternalRowWindowSorter create(
      StructType schema,
      UnsafeProjection partitionSpecProjection,
      Ordering<InternalRow> orderingOfPartitionKey,
      Ordering<InternalRow> orderingInWindow,
      Ordering<InternalRow> orderingAcrossWindows,
      PrefixComparator prefixComparatorInWindow,
      PrefixComparator prefixComparatorAcrossWindows,
      UnsafeExternalRowSorter.PrefixComputer prefixComputerInWindow,
      UnsafeExternalRowSorter.PrefixComputer prefixComputerAcrossWindows,
      boolean canUseRadixSortInWindow,
      boolean canUseRadixSortAcrossWindows,
      long pageSizeBytes) throws IOException {
    UnsafeExternalRowSorter mainSorter = UnsafeExternalRowSorter.create(
      schema,
      orderingAcrossWindows,
      prefixComparatorAcrossWindows,
      prefixComputerAcrossWindows,
      pageSizeBytes/totalNumSorters,
      canUseRadixSortAcrossWindows);

    return new UnsafeExternalRowWindowSorter(
      mainSorter,
      schema,
      partitionSpecProjection,
      orderingOfPartitionKey,
      orderingInWindow,
      orderingAcrossWindows,
      prefixComparatorInWindow,
      prefixComputerInWindow,
      canUseRadixSortInWindow,
      pageSizeBytes);
  }

  private UnsafeExternalRowWindowSorter(
      UnsafeExternalRowSorter mainSorter,
      StructType schema,
      UnsafeProjection partitionSpecProjection,
      Ordering<InternalRow> orderingOfPartitionKey,
      Ordering<InternalRow> orderingInWindow,
      Ordering<InternalRow> orderingAcrossWindows,
      PrefixComparator prefixComparatorInWindow,
      UnsafeExternalRowSorter.PrefixComputer prefixComputerInWindow,
      boolean canUseRadixSortInWindow,
      long pageSizeBytes) {
    this.mainSorter = mainSorter;
    this.schema = schema;
    this.partitionSpecProjection = partitionSpecProjection;
    this.orderingOfPartitionKey = orderingOfPartitionKey;
    this.orderingInWindow = orderingInWindow;
    this.orderingAcrossWindows = orderingAcrossWindows;
    this.prefixComparatorInWindow = prefixComparatorInWindow;
    this.prefixComputerInWindow = prefixComputerInWindow;
    this.canUseRadixSortInWindow = canUseRadixSortInWindow;
    this.pageSizeBytes = pageSizeBytes;
    this.windowSorterMap = new HashMap<UnsafeRow,AbstractUnsafeExternalRowSorter>(
      windowSorterMapMaxSize);
    this.partitionKeyComparator = new RowComparator(orderingOfPartitionKey);
  }

  /**
   * If the partition key is found in the hash map, then the rows will be inserted to the
   * unsafe external row sorter corresponding to the partition key. Otherwise a new unsafe
   * will be created, and this row will be added to the newly created sorter, and then the
   * pair of partition key and newly created sorter will be added into the hash map. If the
   * size of hash map is above its maximum size, then all the rows that the hash map points
   * to will be moved to the sort based merger.
   */
  @Override
  public void insertRow(UnsafeRow row) throws IOException {
    UnsafeRow windowSorterKey = this.partitionSpecProjection.apply(row);
    AbstractUnsafeExternalRowSorter windowSorter = this.windowSorterMap.get(windowSorterKey);

    if (windowSorter != null) {
      windowSorter.insertRow(row);
    } else if (this.windowSorterMap.size() == this.windowSorterMapMaxSize) {
      this.mainSorter.insertRow(row);
    } else {
      AbstractUnsafeExternalRowSorter sorter = createUnsafeExternalRowSorterForWindow();

      if (sorter == null) {
        this.mainSorter.spill();
        this.mainSorter.insertRow(row);
      } else {
        sorter.insertRow(row);
        this.windowSorterMap.put(windowSorterKey.copy(), sorter);
      }
    }
    numRowsInserted++;
  }

  public long getNumRowsInserted() {
    return numRowsInserted;
  }

  @Override
  public Iterator<InternalRow> sort() throws IOException {
    if (this.mainSorter.getNumRowsInserted() == 0) {
      return getSortedIteratorFromSorterMap();
    } else {
      final SortBasedMergerIterator mergeIterator = new SortBasedMergerIterator(
        RowIterator.fromScala(getSortedIteratorFromSorterMap()),
        RowIterator.fromScala(getSortedIteratorFromMainSorter()),
        orderingAcrossWindows);

      return mergeIterator.toScala();
    }
  }

  @Override
  public Iterator<InternalRow> getIterator() throws IOException {
    throw new IOException("This method is not supported.");
  }

  @Override
  public Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }
    return sort();
  }

  /**
   * Forces spills to occur every `frequency` records. Only for use in tests.
   */
  @Override
  @VisibleForTesting
  void setTestSpillFrequency(int frequency) {
    this.mainSorter.setTestSpillFrequency(frequency);
    for (Entry<UnsafeRow,AbstractUnsafeExternalRowSorter> entry: this.windowSorterMap.entrySet()) {
      entry.getValue().setTestSpillFrequency(frequency);
    }
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  @Override
  public long getSortTimeNanos() {
    long sortTimeNanos = this.mainSorter.getSortTimeNanos();
    for (Entry<UnsafeRow,AbstractUnsafeExternalRowSorter> entry: this.windowSorterMap.entrySet()) {
      sortTimeNanos = sortTimeNanos + entry.getValue().getSortTimeNanos();
    }
    return sortTimeNanos;
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  @Override
  public long getPeakMemoryUsage() {
    long peakMemoryUsage = this.mainSorter.getPeakMemoryUsage();
    for (Entry<UnsafeRow,AbstractUnsafeExternalRowSorter> entry: this.windowSorterMap.entrySet()) {
      peakMemoryUsage = peakMemoryUsage + entry.getValue().getPeakMemoryUsage();
    }
    return peakMemoryUsage;
  }

  @Override
  public void cleanupResources() {
    for (Entry<UnsafeRow,AbstractUnsafeExternalRowSorter> entry: this.windowSorterMap.entrySet()) {
      entry.getValue().cleanupResources();
    }
    this.mainSorter.cleanupResources();
  }

  private Iterator<InternalRow> getSortedIteratorFromMainSorter() throws IOException {
    return this.mainSorter.sort();
  }

  private Iterator<InternalRow> getSortedIteratorFromSorterMap() throws IOException {

    TreeMap<UnsafeRow,AbstractUnsafeExternalRowSorter> partitionKeySortedSorterMap =
      new TreeMap<UnsafeRow,AbstractUnsafeExternalRowSorter>(this.partitionKeyComparator);

    partitionKeySortedSorterMap.putAll(this.windowSorterMap);
    Queue<RowIterator> queue = new LinkedList<>();

    for (Entry<UnsafeRow,AbstractUnsafeExternalRowSorter> entry:
        partitionKeySortedSorterMap.entrySet()) {
      if (orderingInWindow != null) {
        queue.add(RowIterator.fromScala(entry.getValue().sort()));
      } else {
        queue.add(RowIterator.fromScala(entry.getValue().getIterator()));
      }
    }

    Iterator<InternalRow> sortedIterator;
    if (queue.size() == 0) {
      sortedIterator = new RowIterator() {
        @Override
        public boolean advanceNext() { return false; }

        @Override
        public UnsafeRow getRow() { return null; }
      }.toScala();
    } else {
      final ChainedIterator chainedIterator = new ChainedIterator(queue);
      sortedIterator = chainedIterator.toScala();
    }
    return sortedIterator;
  }

  /**
   * Chain multiple UnsafeSorterIterators from windowSorterMap together as single one.
   */
  private static final class ChainedIterator extends RowIterator {
    private final Queue<RowIterator> iterators;
    private RowIterator current;
    private UnsafeRow row;

    ChainedIterator(Queue<RowIterator> iterators) {
      assert iterators.size() > 0;
      this.iterators = iterators;
      this.current = iterators.remove();
      UnsafeRow row = new UnsafeRow(0);
    }

    @Override
    public boolean advanceNext() {
      boolean result = this.current.advanceNext();
      while (!result && !this.iterators.isEmpty()) {
        this.current = iterators.remove();
        result = this.current.advanceNext();
      }
      if (!result) {
        this.row = null;
      } else {
        this.row = (UnsafeRow)this.current.getRow();
      }
      return result;
    }

    @Override
    public UnsafeRow getRow() {
      return row;
    }
  }

  /**
   * This iterator merges two sorted iterators. While it contains two sorted iterators, it always
   * returns the result of the sorted iterator that points to the row with smaller order.
   */
  private static final class SortBasedMergerIterator extends RowIterator {
    private final Ordering<InternalRow> ordering;

    private final RowIterator sortedIterator1;
    private UnsafeRow row1;

    private final RowIterator sortedIterator2;
    private UnsafeRow row2;

    private boolean hasStarted = false;

    private boolean toAdvanceIter1 = false;

    SortBasedMergerIterator(
        RowIterator sortedIterator1,
        RowIterator sortedIterator2,
        Ordering<InternalRow> ordering) {
      this.sortedIterator1 = sortedIterator1;
      this.sortedIterator2 = sortedIterator2;
      this.ordering = ordering;
      row1 = new UnsafeRow(0);
      row2 = new UnsafeRow(0);
    }

    private int compare(UnsafeRow row1, UnsafeRow row2) {
      assert row1 != null;
      assert row2 != null;
      return ordering.compare(row1, row2);
    }

    @Override
    public boolean advanceNext() {
      boolean result = true;
      if (!hasStarted) { // at first both rows have zero field
        boolean result1 = sortedIterator1.advanceNext();
        boolean result2 = sortedIterator2.advanceNext();
        result = result1 || result2;
        row1 = (UnsafeRow)sortedIterator1.getRow();
        row2 = (UnsafeRow)sortedIterator2.getRow();
        hasStarted = true;
      } else if (row1 == null) { // we reach the end of sortedIterator1
        result = sortedIterator2.advanceNext();
        row2 = (UnsafeRow)sortedIterator2.getRow();
      } else if (row2 == null) { // we reach the end of sortedIterator2
        result = sortedIterator1.advanceNext();
        row1 = (UnsafeRow)sortedIterator1.getRow();
      } else {
        if (toAdvanceIter1) {
          sortedIterator1.advanceNext();
          row1 = (UnsafeRow)sortedIterator1.getRow();
        } else {
          sortedIterator2.advanceNext();
          row2 = (UnsafeRow)sortedIterator2.getRow();
        }
      }
      return result;
    }

    @Override
    public UnsafeRow getRow() {
      if (row1 == null && row2 == null) {
        return null;
      } else if (row1 == null) {
        return row2;
      } else if (row2 == null) {
        return row1;
      } else {
        if (compare(row1, row2) <= 0) {
          toAdvanceIter1 = true;
          return row1;
        } else {
          toAdvanceIter1 = false;
          return row2;
        }
      }
    }
  }

  private static final class RowComparator implements Comparator<UnsafeRow> {
    private final Ordering<InternalRow> ordering;

    RowComparator(Ordering<InternalRow> ordering) {
      this.ordering = ordering;
    }

    @Override
    public int compare(
        UnsafeRow row1,
        UnsafeRow row2) {
      return ordering.compare(row1, row2);
    }
  }
}

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
import java.util.function.Supplier;

import scala.collection.Iterator;
import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

public final class UnsafeExternalRowSorter {

  /**
   * If positive, forces records to be spilled to disk at the given frequency (measured in numbers
   * of records). This is only intended to be used in tests.
   */
  private int testSpillFrequency = 0;

  private long numRowsInserted = 0;

  private final StructType schema;
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;

  // This flag makes sure the cleanupResource() has been called. After the cleanup work,
  // iterator.next should always return false. Downstream operator triggers the resource
  // cleanup while they found there's no need to keep the iterator any more.
  // See more details in SPARK-21492.
  private boolean isReleased = false;

  public abstract static class PrefixComputer {

    public static class Prefix {
      /** Key prefix value, or the null prefix value if isNull = true. **/
      public long value;

      /** Whether the key is null. */
      public boolean isNull;
    }

    /**
     * Computes prefix for the given row. For efficiency, the returned object may be reused in
     * further calls to a given PrefixComputer.
     */
    public abstract Prefix computePrefix(InternalRow row);
  }

  public static UnsafeExternalRowSorter createWithRecordComparator(
      StructType schema,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) throws IOException {
    return new UnsafeExternalRowSorter(schema, recordComparatorSupplier, prefixComparator,
      prefixComputer, pageSizeBytes, canUseRadixSort);
  }

  public static UnsafeExternalRowSorter create(
      StructType schema,
      Ordering<InternalRow> ordering,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) throws IOException {
    Supplier<RecordComparator> recordComparatorSupplier =
      () -> new RowComparator(ordering, schema.length());
    return new UnsafeExternalRowSorter(schema, recordComparatorSupplier, prefixComparator,
      prefixComputer, pageSizeBytes, canUseRadixSort);
  }

  private UnsafeExternalRowSorter(
      StructType schema,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) {
    this.schema = schema;
    this.prefixComputer = prefixComputer;
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();
    sorter = UnsafeExternalSorter.create(
      taskContext.taskMemoryManager(),
      sparkEnv.blockManager(),
      sparkEnv.serializerManager(),
      taskContext,
      recordComparatorSupplier,
      prefixComparator,
      (int) (long) sparkEnv.conf().get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
      pageSizeBytes,
      (int) SparkEnv.get().conf().get(
        package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD()),
      canUseRadixSort
    );
  }

  /**
   * Forces spills to occur every `frequency` records. Only for use in tests.
   */
  @VisibleForTesting
  void setTestSpillFrequency(int frequency) {
    assert frequency > 0 : "Frequency must be positive";
    testSpillFrequency = frequency;
  }

  public void insertRow(UnsafeRow row) throws IOException {
    final PrefixComputer.Prefix prefix = prefixComputer.computePrefix(row);
    sorter.insertRecord(
      row.getBaseObject(),
      row.getBaseOffset(),
      row.getSizeInBytes(),
      prefix.value,
      prefix.isNull
    );
    numRowsInserted++;
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      sorter.spill();
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsage() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return sorter.getSortTimeNanos();
  }

  public void cleanupResources() {
    isReleased = true;
    sorter.cleanupResources();
  }

  public Iterator<InternalRow> sort() throws IOException {
    try {
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      if (!sortedIterator.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      return new RowIterator() {

        private final int numFields = schema.length();
        private UnsafeRow row = new UnsafeRow(numFields);

        @Override
        public boolean advanceNext() {
          try {
            if (!isReleased && sortedIterator.hasNext()) {
              sortedIterator.loadNext();
              row.pointTo(
                  sortedIterator.getBaseObject(),
                  sortedIterator.getBaseOffset(),
                  sortedIterator.getRecordLength());
              // Here is the initial bug fix in SPARK-9364: the bug fix of use-after-free bug
              // when returning the last row from an iterator. For example, in
              // [[GroupedIterator]], we still use the last row after traversing the iterator
              // in `fetchNextGroupIterator`
              if (!sortedIterator.hasNext()) {
                row = row.copy(); // so that we don't have dangling pointers to freed page
                cleanupResources();
              }
              return true;
            } else {
              row = null; // so that we don't keep references to the base object
              return false;
            }
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        }

        @Override
        public UnsafeRow getRow() { return row; }

      }.toScala();
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }

  public Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }
    return sort();
  }

  private static final class RowComparator extends RecordComparator {
    private final Ordering<InternalRow> ordering;
    private final UnsafeRow row1;
    private final UnsafeRow row2;

    RowComparator(Ordering<InternalRow> ordering, int numFields) {
      this.row1 = new UnsafeRow(numFields);
      this.row2 = new UnsafeRow(numFields);
      this.ordering = ordering;
    }

    @Override
    public int compare(
        Object baseObj1,
        long baseOff1,
        int baseLen1,
        Object baseObj2,
        long baseOff2,
        int baseLen2) {
      // Note that since ordering doesn't need the total length of the record, we just pass 0
      // into the row.
      row1.pointTo(baseObj1, baseOff1, 0);
      row2.pointTo(baseObj2, baseOff2, 0);
      return ordering.compare(row1, row2);
    }
  }
}

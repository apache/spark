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

import scala.collection.Iterator;
import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.AbstractScalaRowIterator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

final class UnsafeExternalRowSorter {

  /**
   * If positive, forces records to be spilled to disk at the given frequency (measured in numbers
   * of records). This is only intended to be used in tests.
   */
  private int testSpillFrequency = 0;

  private long numRowsInserted = 0;

  private final StructType schema;
  private final PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;

  public static abstract class PrefixComputer {
    abstract long computePrefix(InternalRow row);
  }

  public UnsafeExternalRowSorter(
      StructType schema,
      Ordering<InternalRow> ordering,
      PrefixComparator prefixComparator,
      PrefixComputer prefixComputer,
      long pageSizeBytes) throws IOException {
    this.schema = schema;
    this.prefixComputer = prefixComputer;
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();
    sorter = UnsafeExternalSorter.create(
      taskContext.taskMemoryManager(),
      sparkEnv.shuffleMemoryManager(),
      sparkEnv.blockManager(),
      taskContext,
      new RowComparator(ordering, schema.length()),
      prefixComparator,
      /* initialSize */ 4096,
      pageSizeBytes
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

  @VisibleForTesting
  void insertRow(UnsafeRow row) throws IOException {
    final long prefix = prefixComputer.computePrefix(row);
    sorter.insertRecord(
      row.getBaseObject(),
      row.getBaseOffset(),
      row.getSizeInBytes(),
      prefix
    );
    numRowsInserted++;
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      spill();
    }
  }

  @VisibleForTesting
  void spill() throws IOException {
    sorter.spill();
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsage() {
    return sorter.getPeakMemoryUsedBytes();
  }

  private void cleanupResources() {
    sorter.cleanupResources();
  }

  @VisibleForTesting
  Iterator<UnsafeRow> sort() throws IOException {
    try {
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      if (!sortedIterator.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      return new AbstractScalaRowIterator<UnsafeRow>() {

        private final int numFields = schema.length();
        private UnsafeRow row = new UnsafeRow();

        @Override
        public boolean hasNext() {
          return sortedIterator.hasNext();
        }

        @Override
        public UnsafeRow next() {
          try {
            sortedIterator.loadNext();
            row.pointTo(
              sortedIterator.getBaseObject(),
              sortedIterator.getBaseOffset(),
              numFields,
              sortedIterator.getRecordLength());
            if (!hasNext()) {
              UnsafeRow copy = row.copy(); // so that we don't have dangling pointers to freed page
              row = null; // so that we don't keep references to the base object
              cleanupResources();
              return copy;
            } else {
              return row;
            }
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        };
      };
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }


  public Iterator<UnsafeRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }
    return sort();
  }

  /**
   * Return true if UnsafeExternalRowSorter can sort rows with the given schema, false otherwise.
   */
  public static boolean supportsSchema(StructType schema) {
    return UnsafeProjection.canSupport(schema);
  }

  private static final class RowComparator extends RecordComparator {
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    public RowComparator(Ordering<InternalRow> ordering, int numFields) {
      this.numFields = numFields;
      this.ordering = ordering;
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      // TODO: Why are the sizes -1?
      row1.pointTo(baseObj1, baseOff1, numFields, -1);
      row2.pointTo(baseObj2, baseOff2, numFields, -1);
      return ordering.compare(row1, row2);
    }
  }
}

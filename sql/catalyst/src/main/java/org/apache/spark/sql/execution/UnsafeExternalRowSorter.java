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
import org.apache.spark.sql.catalyst.expressions.ObjectUnsafeColumnWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeColumnWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRowConverter;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
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
  private final UnsafeRowConverter rowConverter;
  private final PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;
  private byte[] rowConversionBuffer = new byte[1024 * 8];

  public static abstract class PrefixComputer {
    abstract long computePrefix(InternalRow row);
  }

  public UnsafeExternalRowSorter(
      StructType schema,
      Ordering<InternalRow> ordering,
      PrefixComparator prefixComparator,
      PrefixComputer prefixComputer) throws IOException {
    this.schema = schema;
    this.rowConverter = new UnsafeRowConverter(schema);
    this.prefixComputer = prefixComputer;
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();
    sorter = new UnsafeExternalSorter(
      taskContext.taskMemoryManager(),
      sparkEnv.shuffleMemoryManager(),
      sparkEnv.blockManager(),
      taskContext,
      new RowComparator(ordering, schema.length(), null),
      prefixComparator,
      4096,
      sparkEnv.conf()
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
  void insertRow(InternalRow row) throws IOException {
    final int sizeRequirement = rowConverter.getSizeRequirement(row);
    if (sizeRequirement > rowConversionBuffer.length) {
      rowConversionBuffer = new byte[sizeRequirement];
    }
    final int bytesWritten = rowConverter.writeRow(
      row, rowConversionBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, sizeRequirement, null);
    assert (bytesWritten == sizeRequirement);
    final long prefix = prefixComputer.computePrefix(row);
    sorter.insertRecord(
      rowConversionBuffer,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      sizeRequirement,
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

  private void cleanupResources() {
    sorter.freeMemory();
  }

  @VisibleForTesting
  Iterator<InternalRow> sort() throws IOException {
    try {
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      if (!sortedIterator.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      return new AbstractScalaRowIterator() {

        private final int numFields = schema.length();
        private final UnsafeRow row = new UnsafeRow();

        @Override
        public boolean hasNext() {
          return sortedIterator.hasNext();
        }

        @Override
        public InternalRow next() {
          try {
            sortedIterator.loadNext();
            row.pointTo(
              sortedIterator.getBaseObject(),
              sortedIterator.getBaseOffset(),
              numFields,
              sortedIterator.getRecordLength(),
              null);
            if (!hasNext()) {
              row.copy(); // so that we don't have dangling pointers to freed page
              cleanupResources();
            }
            return row;
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            PlatformDependent.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        };
      };
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }


  public Iterator<InternalRow> sort(Iterator<InternalRow> inputIterator) throws IOException {
      while (inputIterator.hasNext()) {
        insertRow(inputIterator.next());
      }
      return sort();
  }

  /**
   * Return true if UnsafeExternalRowSorter can sort rows with the given schema, false otherwise.
   */
  public static boolean supportsSchema(StructType schema) {
    // TODO: add spilling note to explain why we do this for now:
    for (StructField field : schema.fields()) {
      if (UnsafeColumnWriter.forType(field.dataType()) instanceof ObjectUnsafeColumnWriter) {
        return false;
      }
    }
    return true;
  }

  private static final class RowComparator extends RecordComparator {
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final ObjectPool objPool;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    public RowComparator(Ordering<InternalRow> ordering, int numFields, ObjectPool objPool) {
      this.numFields = numFields;
      this.ordering = ordering;
      this.objPool = objPool;
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      row1.pointTo(baseObj1, baseOff1, numFields, -1, objPool);
      row2.pointTo(baseObj2, baseOff2, numFields, -1, objPool);
      return ordering.compare(row1, row2);
    }
  }
}

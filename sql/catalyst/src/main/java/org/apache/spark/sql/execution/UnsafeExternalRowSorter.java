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
import java.util.Arrays;

import scala.Function1;
import scala.collection.Iterator;
import scala.math.Ordering;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.AbstractScalaRowIterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRowConverter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

final class UnsafeExternalRowSorter {

  private final StructType schema;
  private final UnsafeRowConverter rowConverter;
  private final RowComparator rowComparator;
  private final PrefixComparator prefixComparator;
  private final Function1<Row, Long> prefixComputer;

  public UnsafeExternalRowSorter(
      StructType schema,
      Ordering<Row> ordering,
      PrefixComparator prefixComparator,
      // TODO: if possible, avoid this boxing of the return value
      Function1<Row, Long> prefixComputer) {
    this.schema = schema;
    this.rowConverter = new UnsafeRowConverter(schema);
    this.rowComparator = new RowComparator(ordering, schema);
    this.prefixComparator = prefixComparator;
    this.prefixComputer = prefixComputer;
  }

  public Iterator<Row> sort(Iterator<Row> inputIterator) throws IOException {
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();
    byte[] rowConversionBuffer = new byte[1024 * 8];
    final UnsafeExternalSorter sorter = new UnsafeExternalSorter(
      taskContext.taskMemoryManager(),
      sparkEnv.shuffleMemoryManager(),
      sparkEnv.blockManager(),
      taskContext,
      rowComparator,
      prefixComparator,
      4096,
      sparkEnv.conf()
    );
    try {
      while (inputIterator.hasNext()) {
        final Row row = inputIterator.next();
        final int sizeRequirement = rowConverter.getSizeRequirement(row);
        if (sizeRequirement > rowConversionBuffer.length) {
          rowConversionBuffer = new byte[sizeRequirement];
        } else {
          // Zero out the buffer that's used to hold the current row. This is necessary in order
          // to ensure that rows hash properly, since garbage data from the previous row could
          // otherwise end up as padding in this row. As a performance optimization, we only zero
          // out the portion of the buffer that we'll actually write to.
          Arrays.fill(rowConversionBuffer, 0, sizeRequirement, (byte) 0);
        }
        final int bytesWritten =
          rowConverter.writeRow(row, rowConversionBuffer, PlatformDependent.BYTE_ARRAY_OFFSET);
        assert (bytesWritten == sizeRequirement);
        final long prefix = prefixComputer.apply(row);
        sorter.insertRecord(
          rowConversionBuffer,
          PlatformDependent.BYTE_ARRAY_OFFSET,
          sizeRequirement,
          prefix
        );
      }
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      return new AbstractScalaRowIterator() {

        private final int numFields = schema.length();
        private final UnsafeRow row = new UnsafeRow();

        @Override
        public boolean hasNext() {
          return sortedIterator.hasNext();
        }

        @Override
        public Row next() {
          try {
            sortedIterator.loadNext();
            if (hasNext()) {
              row.pointTo(
                sortedIterator.getBaseObject(), sortedIterator.getBaseOffset(), numFields, schema);
              return row;
            } else {
              final byte[] rowDataCopy = new byte[sortedIterator.getRecordLength()];
              PlatformDependent.copyMemory(
                sortedIterator.getBaseObject(),
                sortedIterator.getBaseOffset(),
                rowDataCopy,
                PlatformDependent.BYTE_ARRAY_OFFSET,
                sortedIterator.getRecordLength()
              );
              row.backingArray = rowDataCopy;
              row.pointTo(rowDataCopy, PlatformDependent.BYTE_ARRAY_OFFSET, numFields, schema);
              sorter.freeMemory();
              return row;
            }
          } catch (IOException e) {
            // TODO: we need to ensure that files are cleaned properly after an exception,
            // so we need better cleanup methods than freeMemory().
            sorter.freeMemory();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            PlatformDependent.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        };
      };
    } catch (IOException e) {
      // TODO: we need to ensure that files are cleaned properly after an exception,
      // so we need better cleanup methods than freeMemory().
      sorter.freeMemory();
      throw e;
    }
  }

  private static final class RowComparator extends RecordComparator {
    private final StructType schema;
    private final Ordering<Row> ordering;
    private final int numFields;
    private final UnsafeRow row1 = new UnsafeRow();
    private final UnsafeRow row2 = new UnsafeRow();

    public RowComparator(Ordering<Row> ordering, StructType schema) {
      this.schema = schema;
      this.numFields = schema.length();
      this.ordering = ordering;
    }

    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      row1.pointTo(baseObj1, baseOff1, numFields, schema);
      row2.pointTo(baseObj2, baseOff2, numFields, schema);
      return ordering.compare(row1, row2);
    }
  }
}

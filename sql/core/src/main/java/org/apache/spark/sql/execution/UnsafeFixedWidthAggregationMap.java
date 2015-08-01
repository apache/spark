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

import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeInMemorySorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This map supports a maximum of 2 billion keys.
 */
public final class UnsafeFixedWidthAggregationMap {

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   */
  private final byte[] emptyAggregationBuffer;

  private final StructType aggregationBufferSchema;

  private final StructType groupingKeySchema;

  /**
   * Encodes grouping keys as UnsafeRows.
   */
  private final UnsafeProjection groupingKeyProjection;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentAggregationBuffer = new UnsafeRow();

  private final boolean enablePerfMetrics;

  /**
   * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!UnsafeRow.isFixedLength(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param taskMemoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param shuffleMemoryManager the shuffle memory manager, for coordinating our memory usage with
   *                             other tasks.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskMemoryManager taskMemoryManager,
      ShuffleMemoryManager shuffleMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(
      taskMemoryManager, shuffleMemoryManager, initialCapacity, pageSizeBytes, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    assert(this.emptyAggregationBuffer.length == aggregationBufferSchema.length() * 8 +
      UnsafeRow.calculateBitSetWidthInBytes(aggregationBufferSchema.length()));
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object. If additional memory could not be allocated, then this method will
   * signal an error by returning null.
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      unsafeGroupingKeyRow.getBaseObject(),
      unsafeGroupingKeyRow.getBaseOffset(),
      unsafeGroupingKeyRow.getSizeInBytes());
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      boolean putSucceeded = loc.putNewKey(
        unsafeGroupingKeyRow.getBaseObject(),
        unsafeGroupingKeyRow.getBaseOffset(),
        unsafeGroupingKeyRow.getSizeInBytes(),
        emptyAggregationBuffer,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );
      if (!putSucceeded) {
        return null;
      }
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentAggregationBuffer.pointTo(
      address.getBaseObject(),
      address.getBaseOffset(),
      aggregationBufferSchema.length(),
      loc.getValueLength()
    );
    return currentAggregationBuffer;
  }

  /**
   * Returns an iterator over the keys and values in this map.
   *
   * For efficiency, each call returns the same object.
   */
  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private final BytesToBytesMap.BytesToBytesMapIterator mapLocationIterator = map.iterator();
      private final UnsafeRow key = new UnsafeRow();
      private final UnsafeRow value = new UnsafeRow();

      @Override
      public boolean next() {
        if (mapLocationIterator.hasNext()) {
          final BytesToBytesMap.Location loc = mapLocationIterator.next();
          final MemoryLocation keyAddress = loc.getKeyAddress();
          final MemoryLocation valueAddress = loc.getValueAddress();
          key.pointTo(
            keyAddress.getBaseObject(),
            keyAddress.getBaseOffset(),
            groupingKeySchema.length(),
            loc.getKeyLength()
          );
          value.pointTo(
            valueAddress.getBaseObject(),
            valueAddress.getBaseOffset(),
            aggregationBufferSchema.length(),
            loc.getValueLength()
          );
          return true;
        } else {
          return false;
        }
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // Do nothing.
      }
    };
  }

  /**
   * Free the unsafe memory associated with this map.sp
   */
  public void free() {
    map.free();
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
  }

  /**
   * Sorts the key, value data in this map in place, and return them as an iterator.
   *
   * The only memory that is allocated is the address/prefix array, 16 bytes per record.
   */
  public KVIterator<UnsafeRow, UnsafeRow> sortedIterator() {
    int numElements = map.numElements();
    final int numKeyFields = groupingKeySchema.size();
    TaskMemoryManager memoryManager = map.getTaskMemoryManager();

    UnsafeExternalRowSorter.PrefixComputer prefixComp =
      SortPrefixUtils.createPrefixGenerator(groupingKeySchema);
    PrefixComparator prefixComparator = SortPrefixUtils.getPrefixComparator(groupingKeySchema);

    final BaseOrdering ordering = GenerateOrdering.create(groupingKeySchema);
    RecordComparator recordComparator = new RecordComparator() {
      private final UnsafeRow row1 = new UnsafeRow();
      private final UnsafeRow row2 = new UnsafeRow();

      @Override
      public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
        row1.pointTo(baseObj1, baseOff1 + 4, numKeyFields, -1);
        row2.pointTo(baseObj2, baseOff2 + 4, numKeyFields, -1);
        return ordering.compare(row1, row2);
      }
    };

    // Insert the records into the in-memory sorter.
    final UnsafeInMemorySorter sorter = new UnsafeInMemorySorter(
      memoryManager, recordComparator, prefixComparator, numElements);

    BytesToBytesMap.BytesToBytesMapIterator iter = map.iterator();
    UnsafeRow row = new UnsafeRow();
    while (iter.hasNext()) {
      final BytesToBytesMap.Location loc = iter.next();
      final Object baseObject = loc.getKeyAddress().getBaseObject();
      final long baseOffset = loc.getKeyAddress().getBaseOffset();

      // Get encoded memory address
      MemoryBlock page = loc.getMemoryPage();
      long address = memoryManager.encodePageNumberAndOffset(page, baseOffset - 8);

      // Compute prefix
      row.pointTo(baseObject, baseOffset, numKeyFields, loc.getKeyLength());
      final long prefix = prefixComp.computePrefix(row);

      sorter.insertRecord(address, prefix);
    }

    // Return the sorted result as an iterator.
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      private final UnsafeRow key = new UnsafeRow();
      private final UnsafeRow value = new UnsafeRow();
      private int numValueFields = aggregationBufferSchema.size();

      @Override
      public boolean next() throws IOException {
        if (sortedIterator.hasNext()) {
          sortedIterator.loadNext();
          Object baseObj = sortedIterator.getBaseObject();
          long recordOffset = sortedIterator.getBaseOffset();
          int recordLen = sortedIterator.getRecordLength();
          int keyLen = PlatformDependent.UNSAFE.getInt(baseObj, recordOffset);
          key.pointTo(baseObj, recordOffset + 4, numKeyFields, keyLen);
          value.pointTo(baseObj, recordOffset + 4 + keyLen, numValueFields, recordLen - keyLen);
          return true;
        } else {
          return false;
        }
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // Do nothing
      }
    };
  }
}

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

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

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
      if (!UnsafeRow.isMutable(field.dataType())) {
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
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object. If additional memory could not be allocated, then this method will
   * signal an error by returning null.
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

    return getAggregationBufferFromUnsafeRow(unsafeGroupingKeyRow);
  }

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow unsafeGroupingKeyRow) {
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
        Platform.BYTE_ARRAY_OFFSET,
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
   * Returns an iterator over the keys and values in this map. This uses destructive iterator of
   * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
   * been called.
   *
   * For efficiency, each call returns the same object.
   */
  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private final BytesToBytesMap.BytesToBytesMapIterator mapLocationIterator =
        map.destructiveIterator();
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
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    return map.getPeakMemoryUsedBytes();
  }

  @VisibleForTesting
  public int getNumDataPages() {
    return map.getNumDataPages();
  }

  /**
   * Free the memory associated with this map. This is idempotent and can be called multiple times.
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
   * Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVExternalSorter]]
   * that can be used to insert more records to do external sorting.
   *
   * The only memory that is allocated is the address/prefix array, 16 bytes per record.
   *
   * Note that this destroys the map, and as a result, the map cannot be used anymore after this.
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    UnsafeKVExternalSorter sorter = new UnsafeKVExternalSorter(
      groupingKeySchema, aggregationBufferSchema,
      SparkEnv.get().blockManager(), map.getShuffleMemoryManager(), map.getPageSizeBytes(), map);
    return sorter;
  }
}

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

package org.apache.spark.sql.catalyst.expressions;

import java.util.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
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
   * @return true if UnsafeFixedWidthAggregationMap supports grouping keys with the given schema,
   *         false otherwise.
   */
  public static boolean supportsGroupKeySchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!UnsafeRow.readableFieldTypes.contains(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!UnsafeRow.settableFieldTypes.contains(field.dataType())) {
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
   * @param memoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskMemoryManager memoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map =
      new BytesToBytesMap(memoryManager, initialCapacity, pageSizeBytes, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    assert(this.emptyAggregationBuffer.length == aggregationBufferSchema.length() * 8 +
      UnsafeRow.calculateBitSetWidthInBytes(aggregationBufferSchema.length()));
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
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
      loc.putNewKey(
        unsafeGroupingKeyRow.getBaseObject(),
        unsafeGroupingKeyRow.getBaseOffset(),
        unsafeGroupingKeyRow.getSizeInBytes(),
        emptyAggregationBuffer,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );
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
   * Mutable pair object returned by {@link UnsafeFixedWidthAggregationMap#iterator()}.
   */
  public static class MapEntry {
    private MapEntry() { };
    public final UnsafeRow key = new UnsafeRow();
    public final UnsafeRow value = new UnsafeRow();
  }

  /**
   * Returns an iterator over the keys and values in this map.
   *
   * For efficiency, each call returns the same object.
   */
  public Iterator<MapEntry> iterator() {
    return new Iterator<MapEntry>() {

      private final MapEntry entry = new MapEntry();
      private final Iterator<BytesToBytesMap.Location> mapLocationIterator = map.iterator();

      @Override
      public boolean hasNext() {
        return mapLocationIterator.hasNext();
      }

      @Override
      public MapEntry next() {
        final BytesToBytesMap.Location loc = mapLocationIterator.next();
        final MemoryLocation keyAddress = loc.getKeyAddress();
        final MemoryLocation valueAddress = loc.getValueAddress();
        entry.key.pointTo(
          keyAddress.getBaseObject(),
          keyAddress.getBaseOffset(),
          groupingKeySchema.length(),
          loc.getKeyLength()
        );
        entry.value.pointTo(
          valueAddress.getBaseObject(),
          valueAddress.getBaseOffset(),
          aggregationBufferSchema.length(),
          loc.getValueLength()
        );
        return entry;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Free the unsafe memory associated with this map.
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

}

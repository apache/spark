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

package org.apache.spark.sql.catalyst.expressions.managedmemory;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.managedmemory.ManagedMemoryRowConverter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.managedmemory.PlatformDependent;
import org.apache.spark.managedmemory.map.BytesToBytesMap;
import org.apache.spark.managedmemory.memory.MemoryLocation;
import org.apache.spark.managedmemory.memory.TaskMemoryManager;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This map supports a maximum of 2 billion keys.
 */
public final class FixedWidthAggregationMap {

  /**
   * An empty aggregation buffer, encoded in ManagedMemoryRow format. When inserting a new key into
   * the map, we copy this buffer and use it as the value.
   */
  private final long[] emptyAggregationBuffer;

  private final StructType aggregationBufferSchema;

  private final StructType groupingKeySchema;

  /**
   * Encodes grouping keys as ManagedMemoryRows.
   */
  private final ManagedMemoryRowConverter groupingKeyToManagedMemoryRowConverter;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final ManagedMemoryRow currentAggregationBuffer = new ManagedMemoryRow();

  /**
   * Scratch space that is used when encoding grouping keys into ManagedMemoryRow format.
   *
   * By default, this is a 1MB array, but it will grow as necessary in case larger keys are
   * encountered.
   */
  private long[] groupingKeyConversionScratchSpace = new long[1024 / 8];

  private final boolean enablePerfMetrics;

  /**
   * @return true if FixedWidthAggregationMap supports grouping keys with the given schema,
   *         false otherwise.
   */
  public static boolean supportsGroupKeySchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!ManagedMemoryRow.readableFieldTypes.contains(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if FixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!ManagedMemoryRow.settableFieldTypes.contains(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new FixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param memoryManager the memory manager used to allocate our managed memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public FixedWidthAggregationMap(
      Row emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this.emptyAggregationBuffer =
      convertToManagedMemoryRow(emptyAggregationBuffer, aggregationBufferSchema);
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.groupingKeyToManagedMemoryRowConverter = new ManagedMemoryRowConverter(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(memoryManager, initialCapacity, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;
  }

  /**
   * Convert a Java object row into a ManagedMemoryRow, allocating it into a new long array.
   */
  private static long[] convertToManagedMemoryRow(Row javaRow, StructType schema) {
    final ManagedMemoryRowConverter converter = new ManagedMemoryRowConverter(schema);
    final long[] managedMemoryRow = new long[converter.getSizeRequirement(javaRow)];
    final long writtenLength =
      converter.writeRow(javaRow, managedMemoryRow, PlatformDependent.LONG_ARRAY_OFFSET);
    assert (writtenLength == managedMemoryRow.length): "Size requirement calculation was wrong!";
    return managedMemoryRow;
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public ManagedMemoryRow getAggregationBuffer(Row groupingKey) {
    final int groupingKeySize = groupingKeyToManagedMemoryRowConverter.getSizeRequirement(groupingKey);
    // Make sure that the buffer is large enough to hold the key. If it's not, grow it:
    if (groupingKeySize > groupingKeyConversionScratchSpace.length) {
      // This new array will be initially zero, so there's no need to zero it out here
      groupingKeyConversionScratchSpace = new long[groupingKeySize];
    } else {
      // Zero out the buffer that's used to hold the current row. This is necessary in order
      // to ensure that rows hash properly, since garbage data from the previous row could
      // otherwise end up as padding in this row. As a performance optimization, we only zero out
      // the portion of the buffer that we'll actually write to.
      Arrays.fill(groupingKeyConversionScratchSpace, 0, groupingKeySize, 0);
    }
    final long actualGroupingKeySize = groupingKeyToManagedMemoryRowConverter.writeRow(
      groupingKey,
      groupingKeyConversionScratchSpace,
      PlatformDependent.LONG_ARRAY_OFFSET);
    assert (groupingKeySize == actualGroupingKeySize) : "Size requirement calculation was wrong!";

    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      groupingKeyConversionScratchSpace,
      PlatformDependent.LONG_ARRAY_OFFSET,
      groupingKeySize);
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      loc.putNewKey(
        groupingKeyConversionScratchSpace,
        PlatformDependent.LONG_ARRAY_OFFSET,
        groupingKeySize,
        emptyAggregationBuffer,
        PlatformDependent.LONG_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentAggregationBuffer.pointTo(
      address.getBaseObject(),
      address.getBaseOffset(),
      aggregationBufferSchema.length(),
      aggregationBufferSchema
    );
    return currentAggregationBuffer;
  }

  /**
   * Mutable pair object returned by {@link FixedWidthAggregationMap#iterator()}.
   */
  public static class MapEntry {
    private MapEntry() { };
    public final ManagedMemoryRow key = new ManagedMemoryRow();
    public final ManagedMemoryRow value = new ManagedMemoryRow();
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
          groupingKeySchema
        );
        entry.value.pointTo(
          valueAddress.getBaseObject(),
          valueAddress.getBaseOffset(),
          aggregationBufferSchema.length(),
          aggregationBufferSchema
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
   * Free the managed memory associated with this map.
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

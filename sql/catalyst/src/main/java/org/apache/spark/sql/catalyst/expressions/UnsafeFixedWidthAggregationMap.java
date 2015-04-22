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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryLocation;

/**
 * Unsafe-based HashMap for performing aggregations in which the aggregated values are
 * fixed-width.  This is NOT threadsafe.
 */
public final class UnsafeFixedWidthAggregationMap {

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   */
  private final long[] emptyAggregationBuffer;

  private final StructType aggregationBufferSchema;

  private final StructType groupingKeySchema;

  /**
   * Encodes grouping keys as UnsafeRows.
   */
  private final UnsafeRowConverter groupingKeyToUnsafeRowConverter;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentAggregationBuffer = new UnsafeRow();

  /**
   * Scratch space that is used when encoding grouping keys into UnsafeRow format.
   *
   * By default, this is a 1MB array, but it will grow as necessary in case larger keys are
   * encountered.
   */
  private long[] groupingKeyConversionScratchSpace = new long[1024 / 8];

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param allocator the memory allocator used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   */
  public UnsafeFixedWidthAggregationMap(
      Row emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      MemoryAllocator allocator,
      long initialCapacity) {
    this.emptyAggregationBuffer =
      convertToUnsafeRow(emptyAggregationBuffer, aggregationBufferSchema);
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.groupingKeyToUnsafeRowConverter = new UnsafeRowConverter(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(allocator, initialCapacity);
  }

  /**
   * Convert a Java object row into an UnsafeRow, allocating it into a new long array.
   */
  private static long[] convertToUnsafeRow(Row javaRow, StructType schema) {
    final UnsafeRowConverter converter = new UnsafeRowConverter(schema);
    final long[] unsafeRow = new long[converter.getSizeRequirement(javaRow)];
    final long writtenLength =
      converter.writeRow(javaRow, unsafeRow, PlatformDependent.LONG_ARRAY_OFFSET);
    assert (writtenLength == unsafeRow.length): "Size requirement calculation was wrong!";
    return unsafeRow;
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public UnsafeRow getAggregationBuffer(Row groupingKey) {
    // Zero out the buffer that's used to hold the current row. This is necessary in order
    // to ensure that rows hash properly, since garbage data from the previous row could
    // otherwise end up as padding in this row.
    Arrays.fill(groupingKeyConversionScratchSpace, 0);
    final int groupingKeySize = groupingKeyToUnsafeRowConverter.getSizeRequirement(groupingKey);
    if (groupingKeySize > groupingKeyConversionScratchSpace.length) {
      groupingKeyConversionScratchSpace = new long[groupingKeySize];
    }
    final long actualGroupingKeySize = groupingKeyToUnsafeRowConverter.writeRow(
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
      loc.storeKeyAndValue(
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
    currentAggregationBuffer.set(
      address.getBaseObject(),
      address.getBaseOffset(),
      aggregationBufferSchema.length(),
      aggregationBufferSchema
    );
    return currentAggregationBuffer;
  }

  public static class MapEntry {
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
        entry.key.set(
          keyAddress.getBaseObject(),
          keyAddress.getBaseOffset(),
          groupingKeySchema.length(),
          groupingKeySchema
        );
        entry.value.set(
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
   * Free the unsafe memory associated with this map.
   */
  public void free() {
    map.free();
  }

}

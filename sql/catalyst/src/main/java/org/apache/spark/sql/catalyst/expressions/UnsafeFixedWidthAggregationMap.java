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

import scala.Function1;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.sql.catalyst.util.UniqueObjectPool;
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
  private final byte[] emptyBuffer;

  /**
   * An empty row used by `initProjection`
   */
  private static final InternalRow emptyRow = new GenericInternalRow();

  /**
   * Whether can the empty aggregation buffer be reuse without calling `initProjection` or not.
   */
  private final boolean reuseEmptyBuffer;

  /**
   * The projection used to initialize the emptyBuffer
   */
  private final Function1<InternalRow, InternalRow> initProjection;

  /**
   * Encodes grouping keys or buffers as UnsafeRows.
   */
  private final UnsafeRowConverter keyConverter;
  private final UnsafeRowConverter bufferConverter;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * An object pool for objects that are used in grouping keys.
   */
  private final UniqueObjectPool keyPool;

  /**
   * An object pool for objects that are used in aggregation buffers.
   */
  private final ObjectPool bufferPool;

  /**
   * Re-used pointer to the current aggregation buffer
   */
  private final UnsafeRow currentBuffer = new UnsafeRow();

  /**
   * Scratch space that is used when encoding grouping keys into UnsafeRow format.
   *
   * By default, this is a 8 kb array, but it will grow as necessary in case larger keys are
   * encountered.
   */
  private byte[] groupingKeyConversionScratchSpace = new byte[1024 * 8];

  private final boolean enablePerfMetrics;

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param initProjection the default value for new keys (a "zero" of the agg. function)
   * @param keyConverter the converter of the grouping key, used for row conversion.
   * @param bufferConverter the converter of the aggregation buffer, used for row conversion.
   * @param memoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      Function1<InternalRow, InternalRow> initProjection,
      UnsafeRowConverter keyConverter,
      UnsafeRowConverter bufferConverter,
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this.initProjection = initProjection;
    this.keyConverter = keyConverter;
    this.bufferConverter = bufferConverter;
    this.enablePerfMetrics = enablePerfMetrics;

    this.map = new BytesToBytesMap(memoryManager, initialCapacity, enablePerfMetrics);
    this.keyPool = new UniqueObjectPool(100);
    this.bufferPool = new ObjectPool(initialCapacity);

    InternalRow initRow = initProjection.apply(emptyRow);
    this.emptyBuffer = new byte[bufferConverter.getSizeRequirement(initRow)];
    int writtenLength = bufferConverter.writeRow(
      initRow, emptyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, bufferPool);
    assert (writtenLength == emptyBuffer.length): "Size requirement calculation was wrong!";
    // re-use the empty buffer only when there is no object saved in pool.
    reuseEmptyBuffer = bufferPool.size() == 0;
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final int groupingKeySize = keyConverter.getSizeRequirement(groupingKey);
    // Make sure that the buffer is large enough to hold the key. If it's not, grow it:
    if (groupingKeySize > groupingKeyConversionScratchSpace.length) {
      groupingKeyConversionScratchSpace = new byte[groupingKeySize];
    }
    final int actualGroupingKeySize = keyConverter.writeRow(
      groupingKey,
      groupingKeyConversionScratchSpace,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      keyPool);
    assert (groupingKeySize == actualGroupingKeySize) : "Size requirement calculation was wrong!";

    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      groupingKeyConversionScratchSpace,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      groupingKeySize);
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      if (!reuseEmptyBuffer) {
        // There is some objects referenced by emptyBuffer, so generate a new one
        InternalRow initRow = initProjection.apply(emptyRow);
        bufferConverter.writeRow(initRow, emptyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET,
          bufferPool);
      }
      loc.putNewKey(
        groupingKeyConversionScratchSpace,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        groupingKeySize,
        emptyBuffer,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        emptyBuffer.length
      );
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentBuffer.pointTo(
      address.getBaseObject(),
      address.getBaseOffset(),
      bufferConverter.numFields(),
      bufferPool
    );
    return currentBuffer;
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
          keyConverter.numFields(),
          keyPool
        );
        entry.value.pointTo(
          valueAddress.getBaseObject(),
          valueAddress.getBaseOffset(),
          bufferConverter.numFields(),
          bufferPool
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
    System.out.println("Number of unique objects in keys: " + keyPool.size());
    System.out.println("Number of objects in buffers: " + bufferPool.size());
  }

}

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

import scala.collection.Seq;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BaseMutableProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeProjection;
import org.apache.spark.sql.catalyst.util.ObjectPool;
import org.apache.spark.sql.catalyst.util.UniqueObjectPool;
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
  private UnsafeRow emptyBuffer;

  /**
   * An empty row used by `initProjectionection`
   */
  private static final InternalRow emptyRow = new GenericInternalRow();

  /**
   * Whether can the empty aggregation buffer be reuse without calling `initProjectionection` or not.
   */
  private final boolean reuseEmptyBuffer;

  /**
   * The projection used to initialize the emptyBuffer
   */
  private final UnsafeProjection initProjection;
  private final BaseMutableProjection updateProjection;

  /**
   * The projection used to generate the key
   */
  private final UnsafeProjection keyProjection;

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
   * Re-used pointer to the current aggregation key and buffer
   */
  private final UnsafeRow currentBuffer;

  private final int numFieldsInKey;
  private final int numFieldsInBuffer;
  private JoinedRow3 joinedRow = new JoinedRow3();

  private final boolean enablePerfMetrics;

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param initValuesExprs the expressions to generate default value for new keys
   *                        (a "zero" of the agg. function)
   * @param keyExprs the expressions to generate the grouping key.
   * @param updateProjection the projection to update the aggregate buffer.
   * @param memoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      Seq<Expression> initValuesExprs,
      Seq<Expression> keyExprs,
      BaseMutableProjection updateProjection,
      TaskMemoryManager memoryManager,
      int initialCapacity,
      boolean enablePerfMetrics) {
    this.numFieldsInBuffer = initValuesExprs.size();
    this.initProjection = GenerateUnsafeProjection.generate(initValuesExprs);
    this.numFieldsInKey = keyExprs.size();
    this.keyProjection = GenerateUnsafeProjection.generate(keyExprs);
    this.updateProjection = updateProjection;
    this.enablePerfMetrics = enablePerfMetrics;

    this.map = new BytesToBytesMap(memoryManager, initialCapacity, enablePerfMetrics);
    this.keyPool = new UniqueObjectPool(100);
    this.keyProjection.setPool(keyPool);

    this.bufferPool = new ObjectPool(initialCapacity);
    this.currentBuffer = new UnsafeRow(bufferPool);

    this.initProjection.setPool(bufferPool);
    this.emptyBuffer = initProjection.apply(emptyRow);
    // re-use the empty buffer only when there is no object saved in pool.
    reuseEmptyBuffer = bufferPool.size() == 0;
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object.
   */
  public UnsafeRow getAggregationBuffer(UnsafeRow groupingKey) {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      groupingKey.getBaseObject(),
      groupingKey.getBaseOffset(),
      groupingKey.getSizeInBytes());
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      if (!reuseEmptyBuffer) {
        // There is some objects referenced by emptyBuffer, so generate a new one
        emptyBuffer = initProjection.apply(emptyRow);
      }
      loc.putNewKey(
        groupingKey.getBaseObject(),
        groupingKey.getBaseOffset(),
        groupingKey.getSizeInBytes(),
        emptyBuffer.getBaseObject(),
        emptyBuffer.getBaseOffset(),
        emptyBuffer.getSizeInBytes()
      );
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    final MemoryLocation address = loc.getValueAddress();
    currentBuffer.pointTo(
      address.getBaseObject(),
      address.getBaseOffset(),
      numFieldsInBuffer,
      loc.getValueLength()
    );
    return currentBuffer;
  }

  public UnsafeRow update(InternalRow row) {
    UnsafeRow groupKey = keyProjection.apply(row);
    UnsafeRow aggregationBuffer = getAggregationBuffer(groupKey);
    updateProjection.target(aggregationBuffer);
    return (UnsafeRow) updateProjection.apply(joinedRow.apply(aggregationBuffer, row));
  }

  /**
   * Mutable pair object returned by {@link UnsafeFixedWidthAggregationMap#iterator()}.
   */
  public class MapEntry {
    private MapEntry() { };
    public final UnsafeRow key = new UnsafeRow(keyPool);
    public final UnsafeRow value = new UnsafeRow(bufferPool);
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
          numFieldsInKey,
          loc.getKeyLength()
        );
        entry.value.pointTo(
          valueAddress.getBaseObject(),
          valueAddress.getBaseOffset(),
          numFieldsInBuffer,
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
    bufferPool.clear();
    keyPool.clear();
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

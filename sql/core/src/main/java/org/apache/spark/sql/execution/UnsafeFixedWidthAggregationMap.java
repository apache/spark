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

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;

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
  private final UnsafeRow currentAggregationBuffer;

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
   * @param taskContext the current task context.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskContext taskContext,
      int initialCapacity,
      long pageSizeBytes) {
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.currentAggregationBuffer = new UnsafeRow(aggregationBufferSchema.length());
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(
      taskContext.taskMemoryManager(), initialCapacity, pageSizeBytes);

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the aggregation map's output (e.g. aggregate followed by limit).
    taskContext.addTaskCompletionListener(context -> {
      free();
    });
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

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key) {
    return getAggregationBufferFromUnsafeRow(key, key.hashCode());
  }

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key, int hash) {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      key.getBaseObject(),
      key.getBaseOffset(),
      key.getSizeInBytes(),
      hash);
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      boolean putSucceeded = loc.append(
        key.getBaseObject(),
        key.getBaseOffset(),
        key.getSizeInBytes(),
        emptyAggregationBuffer,
        Platform.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );
      if (!putSucceeded) {
        return null;
      }
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    currentAggregationBuffer.pointTo(
      loc.getValueBase(),
      loc.getValueOffset(),
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

      private final BytesToBytesMap.MapIterator mapLocationIterator =
        map.destructiveIterator();
      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());

      @Override
      public boolean next() {
        if (mapLocationIterator.hasNext()) {
          final BytesToBytesMap.Location loc = mapLocationIterator.next();
          key.pointTo(
            loc.getKeyBase(),
            loc.getKeyOffset(),
            loc.getKeyLength()
          );
          value.pointTo(
            loc.getValueBase(),
            loc.getValueOffset(),
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

  /**
   * Free the memory associated with this map. This is idempotent and can be called multiple times.
   */
  public void free() {
    map.free();
  }

  /**
   * Gets the average number of hash probes per key lookup in the underlying `BytesToBytesMap`.
   */
  public double getAvgHashProbesPerKey() {
    return map.getAvgHashProbesPerKey();
  }

  /**
   * Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVExternalSorter]]
   *
   * Note that the map will be reset for inserting new records, and the returned sorter can NOT be
   * used to insert records.
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    return new UnsafeKVExternalSorter(
      groupingKeySchema,
      aggregationBufferSchema,
      SparkEnv.get().blockManager(),
      SparkEnv.get().serializerManager(),
      map.getPageSizeBytes(),
      (int) SparkEnv.get().conf().get(
        package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD()),
      map);
  }
}

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

package org.apache.spark.sql.execution.aggregate;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;

/**
 * Shared, type-independent machinery for the row-based fast hash map that {@code HashAggregateExec}
 * generates as the level-1 cache during hash aggregation (see {@code RowBasedHashMapGenerator}).
 *
 * The generated subclass supplies only the schema-specific, strongly-typed methods that must be
 * codegen'd for speed -- {@code findOrInsert}, {@code equals}, and {@code hash} -- while the
 * boilerplate that does not depend on the key/value schema (the batch and bucket bookkeeping, the
 * empty-buffer setup, the row iterator, and {@code close}) lives here so it is written and JIT'd
 * once per JVM rather than re-emitted into every aggregation stage's generated class.
 *
 * The hot path is unchanged: the bucket scan, hash, equals, and typed key write still run in the
 * generated subclass; the one helper the subclass calls into here ({@link #appendCurrentKey(int)})
 * is {@code final} so HotSpot can inline it.
 *
 * NOTE: like the generated map it replaces, this does not support nullable keys; the caller falls
 * back to the {@code BytesToBytesMap} for those.
 */
public abstract class RowBasedAggregateHashMap implements AutoCloseable {
  // NOTE: these field names are part of the contract with the generated subclass --
  // RowBasedHashMapGenerator emits code that references them by name (e.g. `rowWriter`,
  // `buckets`, `numBuckets`), so renaming one only fails when Janino compiles the generated
  // code at runtime, not at build time.
  protected final RowBasedKeyValueBatch batch;
  protected final int[] buckets;
  protected final int capacity;
  protected final int numBuckets;
  protected final int maxSteps = 2;
  protected int numRows = 0;
  protected final Object emptyVBase;
  protected final long emptyVOff;
  protected final int emptyVLen;
  protected boolean isBatchFull = false;
  protected final UnsafeRowWriter rowWriter;

  protected RowBasedAggregateHashMap(
      StructType keySchema,
      StructType valueSchema,
      TaskMemoryManager taskMemoryManager,
      InternalRow emptyAggregationBuffer,
      int capacity,
      int numKeyFields,
      int keyVarLenBufferSize) {
    this.capacity = capacity;
    double loadFactor = 0.5;
    this.numBuckets = (int) (capacity / loadFactor);
    this.batch =
      RowBasedKeyValueBatch.allocate(keySchema, valueSchema, taskMemoryManager, capacity);

    UnsafeProjection valueProjection = UnsafeProjection.create(valueSchema);
    byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    this.emptyVBase = emptyBuffer;
    this.emptyVOff = Platform.BYTE_ARRAY_OFFSET;
    this.emptyVLen = emptyBuffer.length;

    this.rowWriter = new UnsafeRowWriter(numKeyFields, keyVarLenBufferSize);

    this.buckets = new int[numBuckets];
    java.util.Arrays.fill(this.buckets, -1);
  }

  /**
   * Appends the key currently staged in {@link #rowWriter} (the generated subclass has already
   * called {@code reset()} and written the typed key columns) together with the empty aggregation
   * buffer, and records the new row's bucket. Returns the value row to aggregate into, or
   * {@code null} when the batch is full.
   */
  protected final UnsafeRow appendCurrentKey(int idx) {
    UnsafeRow aggResult = rowWriter.getRow();
    UnsafeRow vRow = batch.appendRow(
      aggResult.getBaseObject(), aggResult.getBaseOffset(), aggResult.getSizeInBytes(),
      emptyVBase, emptyVOff, emptyVLen);
    if (vRow == null) {
      isBatchFull = true;
    } else {
      buckets[idx] = numRows++;
    }
    return vRow;
  }

  public final KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
    return batch.rowIterator();
  }

  @Override
  public final void close() {
    batch.close();
  }
}

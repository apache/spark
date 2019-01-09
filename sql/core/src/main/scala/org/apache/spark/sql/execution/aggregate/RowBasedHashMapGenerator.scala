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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.types._

/**
 * This is a helper class to generate an append-only row-based hash map that can act as a 'cache'
 * for extremely fast key-value lookups while evaluating aggregates (and fall back to the
 * `BytesToBytesMap` if a given key isn't found). This is 'codegened' in HashAggregate to speed
 * up aggregates w/ key.
 *
 * We also have VectorizedHashMapGenerator, which generates a append-only vectorized hash map.
 * We choose one of the two as the 1st level, fast hash map during aggregation.
 *
 * NOTE: This row-based hash map currently doesn't support nullable keys and falls back to the
 * `BytesToBytesMap` to store them.
 */
class RowBasedHashMapGenerator(
    ctx: CodegenContext,
    aggregateExpressions: Seq[AggregateExpression],
    generatedClassName: String,
    groupingKeySchema: StructType,
    bufferSchema: StructType,
    bitMaxCapacity: Int)
  extends HashMapGenerator (ctx, aggregateExpressions, generatedClassName,
    groupingKeySchema, bufferSchema) {

  override protected def initializeAggregateHashMap(): String = {
    val keySchema = ctx.addReferenceObj("keySchemaTerm", groupingKeySchema)
    val valueSchema = ctx.addReferenceObj("valueSchemaTerm", bufferSchema)

    val numVarLenFields = groupingKeys.map(_.dataType).count {
      case dt if UnsafeRow.isFixedLength(dt) => false
      // TODO: consider large decimal and interval type
      case _ => true
    }

    s"""
       |  private org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch batch;
       |  private int[] buckets;
       |  private int capacity = 1 << $bitMaxCapacity;
       |  private double loadFactor = 0.5;
       |  private int numBuckets = (int) (capacity / loadFactor);
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |  private Object emptyVBase;
       |  private long emptyVOff;
       |  private int emptyVLen;
       |  private boolean isBatchFull = false;
       |  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter;
       |
       |
       |  public $generatedClassName(
       |    org.apache.spark.memory.TaskMemoryManager taskMemoryManager,
       |    InternalRow emptyAggregationBuffer) {
       |    batch = org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch
       |      .allocate($keySchema, $valueSchema, taskMemoryManager, capacity);
       |
       |    final UnsafeProjection valueProjection = UnsafeProjection.create($valueSchema);
       |    final byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
       |
       |    emptyVBase = emptyBuffer;
       |    emptyVOff = Platform.BYTE_ARRAY_OFFSET;
       |    emptyVLen = emptyBuffer.length;
       |
       |    agg_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(
       |      ${groupingKeySchema.length}, ${numVarLenFields * 32});
       |
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
     """.stripMargin
  }

  /**
   * Generates a method that returns true if the group-by keys exist at a given index in the
   * associated [[org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch]].
   *
   */
  protected def generateEquals(): String = {

    def genEqualsForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        s"""(${ctx.genEqual(key.dataType, CodeGenerator.getValue("row",
          key.dataType, ordinal.toString()), key.name)})"""
      }.mkString(" && ")
    }

    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  UnsafeRow row = batch.getKeyRow(buckets[idx]);
       |  return ${genEqualsForKeys(groupingKeys)};
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns a
   * [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated
   * [[org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch]].
   *
   */
   protected def generateFindOrInsert(): String = {
    val createUnsafeRowForKey = groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
      key.dataType match {
        case t: DecimalType =>
          s"agg_rowWriter.write(${ordinal}, ${key.name}, ${t.precision}, ${t.scale})"
        case t: DataType =>
          if (!t.isInstanceOf[StringType] && !CodeGenerator.isPrimitiveType(t)) {
            throw new IllegalArgumentException(s"cannot generate code for unsupported type: $t")
          }
          s"agg_rowWriter.write(${ordinal}, ${key.name})"
      }
    }.mkString(";\n")

    val resetNullBits = if (groupingKeySchema.map(_.nullable).forall(_ == false)) {
      ""
    } else {
      "agg_rowWriter.zeroOutNullBytes();"
    }

    s"""
       |public org.apache.spark.sql.catalyst.expressions.UnsafeRow findOrInsert(${
            groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_.name).mkString(", ")});
       |  int step = 0;
       |  int idx = (int) h & (numBuckets - 1);
       |  while (step < maxSteps) {
       |    // Return bucket index if it's either an empty slot or already contains the key
       |    if (buckets[idx] == -1) {
       |      if (numRows < capacity && !isBatchFull) {
       |        agg_rowWriter.reset();
       |        $resetNullBits
       |        ${createUnsafeRowForKey};
       |        org.apache.spark.sql.catalyst.expressions.UnsafeRow agg_result
       |          = agg_rowWriter.getRow();
       |        Object kbase = agg_result.getBaseObject();
       |        long koff = agg_result.getBaseOffset();
       |        int klen = agg_result.getSizeInBytes();
       |
       |        UnsafeRow vRow
       |            = batch.appendRow(kbase, koff, klen, emptyVBase, emptyVOff, emptyVLen);
       |        if (vRow == null) {
       |          isBatchFull = true;
       |        } else {
       |          buckets[idx] = numRows++;
       |        }
       |        return vRow;
       |      } else {
       |        // No more space
       |        return null;
       |      }
       |    } else if (equals(idx, ${groupingKeys.map(_.name).mkString(", ")})) {
       |      return batch.getValueRow(buckets[idx]);
       |    }
       |    idx = (idx + 1) & (numBuckets - 1);
       |    step++;
       |  }
       |  // Didn't find it
       |  return null;
       |}
     """.stripMargin
  }

  protected def generateRowIterator(): String = {
    s"""
       |public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
       |  return batch.rowIterator();
       |}
     """.stripMargin
  }
}

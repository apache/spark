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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.execution.vectorized.{MutableColumnarRow, OnHeapColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This is a helper class to generate an append-only vectorized hash map that can act as a 'cache'
 * for extremely fast key-value lookups while evaluating aggregates (and fall back to the
 * `BytesToBytesMap` if a given key isn't found). This is 'codegened' in HashAggregate to speed
 * up aggregates w/ key.
 *
 * It is backed by a power-of-2-sized array for index lookups and a columnar batch that stores the
 * key-value pairs. The index lookups in the array rely on linear probing (with a small number of
 * maximum tries) and use an inexpensive hash function which makes it really efficient for a
 * majority of lookups. However, using linear probing and an inexpensive hash function also makes it
 * less robust as compared to the `BytesToBytesMap` (especially for a large number of keys or even
 * for certain distribution of keys) and requires us to fall back on the latter for correctness. We
 * also use a secondary columnar batch that logically projects over the original columnar batch and
 * is equivalent to the `BytesToBytesMap` aggregate buffer.
 *
 * NOTE: This vectorized hash map currently doesn't support nullable keys and falls back to the
 * `BytesToBytesMap` to store them.
 */
class VectorizedHashMapGenerator(
    ctx: CodegenContext,
    aggregateExpressions: Seq[AggregateExpression],
    generatedClassName: String,
    groupingKeySchema: StructType,
    bufferSchema: StructType,
    bitMaxCapacity: Int)
  extends HashMapGenerator (ctx, aggregateExpressions, generatedClassName,
    groupingKeySchema, bufferSchema) {

  override protected def initializeAggregateHashMap(): String = {
    val schemaStructType = new StructType((groupingKeySchema ++ bufferSchema).toArray)
    val schema = ctx.addReferenceObj("schemaTerm", schemaStructType)
    val aggBufferSchemaFieldsLength = bufferSchema.fields.length

    s"""
       |  private ${classOf[OnHeapColumnVector].getName}[] vectors;
       |  private ${classOf[ColumnarBatch].getName} batch;
       |  private ${classOf[MutableColumnarRow].getName} aggBufferRow;
       |  private int[] buckets;
       |  private int capacity = 1 << $bitMaxCapacity;
       |  private double loadFactor = 0.5;
       |  private int numBuckets = (int) (capacity / loadFactor);
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |
       |  public $generatedClassName() {
       |    vectors = ${classOf[OnHeapColumnVector].getName}.allocateColumns(capacity, $schema);
       |    batch = new ${classOf[ColumnarBatch].getName}(vectors);
       |
       |    // Generates a projection to return the aggregate buffer only.
       |    ${classOf[OnHeapColumnVector].getName}[] aggBufferVectors =
       |      new ${classOf[OnHeapColumnVector].getName}[$aggBufferSchemaFieldsLength];
       |    for (int i = 0; i < $aggBufferSchemaFieldsLength; i++) {
       |      aggBufferVectors[i] = vectors[i + ${groupingKeys.length}];
       |    }
       |    aggBufferRow = new ${classOf[MutableColumnarRow].getName}(aggBufferVectors);
       |
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
     """.stripMargin
  }


  /**
   * Generates a method that returns true if the group-by keys exist at a given index in the
   * associated [[org.apache.spark.sql.execution.vectorized.OnHeapColumnVector]]. For instance,
   * if we have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private boolean equals(int idx, long agg_key, long agg_key1) {
   *   return vectors[0].getLong(buckets[idx]) == agg_key &&
   *     vectors[1].getLong(buckets[idx]) == agg_key1;
   * }
   * }}}
   */
  protected def generateEquals(): String = {

    def genEqualsForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        val value = CodeGenerator.getValueFromVector(s"vectors[$ordinal]", key.dataType,
          "buckets[idx]")
        s"(${ctx.genEqual(key.dataType, value, key.name)})"
      }.mkString(" && ")
    }

    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  return ${genEqualsForKeys(groupingKeys)};
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns a
   * [[org.apache.spark.sql.execution.vectorized.MutableColumnarRow]] which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated
   * [[org.apache.spark.sql.execution.vectorized.OnHeapColumnVector]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * public MutableColumnarRow findOrInsert(long agg_key, long agg_key1) {
   *   long h = hash(agg_key, agg_key1);
   *   int step = 0;
   *   int idx = (int) h & (numBuckets - 1);
   *   while (step < maxSteps) {
   *     // Return bucket index if it's either an empty slot or already contains the key
   *     if (buckets[idx] == -1) {
   *       if (numRows < capacity) {
   *         vectors[0].putLong(numRows, agg_key);
   *         vectors[1].putLong(numRows, agg_key1);
   *         vectors[2].putLong(numRows, 0);
   *         buckets[idx] = numRows++;
   *         aggBufferRow.rowId = numRows;
   *         return aggBufferRow;
   *       } else {
   *         // No more space
   *         return null;
   *       }
   *     } else if (equals(idx, agg_key, agg_key1)) {
   *       aggBufferRow.rowId = buckets[idx];
   *       return aggBufferRow;
   *     }
   *     idx = (idx + 1) & (numBuckets - 1);
   *     step++;
   *   }
   *   // Didn't find it
   *   return null;
   * }
   * }}}
   */
  protected def generateFindOrInsert(): String = {

    def genCodeToSetKeys(groupingKeys: Seq[Buffer]): Seq[String] = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        CodeGenerator.setValue(s"vectors[$ordinal]", "numRows", key.dataType, key.name)
      }
    }

    def genCodeToSetAggBuffers(bufferValues: Seq[Buffer]): Seq[String] = {
      bufferValues.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        CodeGenerator.updateColumn(s"vectors[${groupingKeys.length + ordinal}]", "numRows",
          key.dataType, buffVars(ordinal), nullable = true)
      }
    }

    s"""
       |public ${classOf[MutableColumnarRow].getName} findOrInsert($groupingKeySignature) {
       |  long h = hash(${groupingKeys.map(_.name).mkString(", ")});
       |  int step = 0;
       |  int idx = (int) h & (numBuckets - 1);
       |  while (step < maxSteps) {
       |    // Return bucket index if it's either an empty slot or already contains the key
       |    if (buckets[idx] == -1) {
       |      if (numRows < capacity) {
       |
       |        // Initialize aggregate keys
       |        ${genCodeToSetKeys(groupingKeys).mkString("\n")}
       |
       |        ${buffVars.map(_.code).mkString("\n")}
       |
       |        // Initialize aggregate values
       |        ${genCodeToSetAggBuffers(bufferValues).mkString("\n")}
       |
       |        buckets[idx] = numRows++;
       |        aggBufferRow.rowId = buckets[idx];
       |        return aggBufferRow;
       |      } else {
       |        // No more space
       |        return null;
       |      }
       |    } else if (equals(idx, ${groupingKeys.map(_.name).mkString(", ")})) {
       |      aggBufferRow.rowId = buckets[idx];
       |      return aggBufferRow;
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
       |public java.util.Iterator<${classOf[InternalRow].getName}> rowIterator() {
       |  batch.setNumRows(numRows);
       |  return batch.rowIterator();
       |}
     """.stripMargin
  }
}

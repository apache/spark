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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext}
import org.apache.spark.sql.types._

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
    bufferSchema: StructType)
  extends HashMapGenerator (ctx, aggregateExpressions, generatedClassName,
    groupingKeySchema, bufferSchema) {

  override protected def initializeAggregateHashMap(): String = {
    val generatedSchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        (groupingKeySchema ++ bufferSchema).map { key =>
          val keyName = ctx.addReferenceObj(key.name)
          key.dataType match {
            case d: DecimalType =>
              s""".add("$keyName", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("$keyName", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    val generatedAggBufferSchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        bufferSchema.map { key =>
          val keyName = ctx.addReferenceObj(key.name)
          key.dataType match {
            case d: DecimalType =>
              s""".add("$keyName", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("$keyName", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    s"""
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch batch;
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch aggregateBufferBatch;
       |  private int[] buckets;
       |  private int capacity = 1 << 16;
       |  private double loadFactor = 0.5;
       |  private int numBuckets = (int) (capacity / loadFactor);
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |  private org.apache.spark.sql.types.StructType aggregateBufferSchema =
       |    $generatedAggBufferSchema
       |
       |  public $generatedClassName() {
       |    batch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(schema,
       |      org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    // TODO: Possibly generate this projection in HashAggregate directly
       |    aggregateBufferBatch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(
       |      aggregateBufferSchema, org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    for (int i = 0 ; i < aggregateBufferBatch.numCols(); i++) {
       |       aggregateBufferBatch.setColumn(i, batch.column(i+${groupingKeys.length}));
       |    }
       |
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
     """.stripMargin
  }


  /**
   * Generates a method that returns true if the group-by keys exist at a given index in the
   * associated [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private boolean equals(int idx, long agg_key, long agg_key1) {
   *   return batch.column(0).getLong(buckets[idx]) == agg_key &&
   *     batch.column(1).getLong(buckets[idx]) == agg_key1;
   * }
   * }}}
   */
  protected def generateEquals(): String = {

    def genEqualsForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        s"""(${ctx.genEqual(key.dataType, ctx.getValue("batch", "buckets[idx]",
          key.dataType, ordinal), key.name)})"""
      }.mkString(" && ")
    }

    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  return ${genEqualsForKeys(groupingKeys)};
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns a mutable
   * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row]] which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated
   * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(
   *     long agg_key, long agg_key1) {
   *   long h = hash(agg_key, agg_key1);
   *   int step = 0;
   *   int idx = (int) h & (numBuckets - 1);
   *   while (step < maxSteps) {
   *     // Return bucket index if it's either an empty slot or already contains the key
   *     if (buckets[idx] == -1) {
   *       batch.column(0).putLong(numRows, agg_key);
   *       batch.column(1).putLong(numRows, agg_key1);
   *       batch.column(2).putLong(numRows, 0);
   *       buckets[idx] = numRows++;
   *       return batch.getRow(buckets[idx]);
   *     } else if (equals(idx, agg_key, agg_key1)) {
   *       return batch.getRow(buckets[idx]);
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
        ctx.setValue("batch", "numRows", key.dataType, ordinal, key.name)
      }
    }

    def genCodeToSetAggBuffers(bufferValues: Seq[Buffer]): Seq[String] = {
      bufferValues.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        ctx.updateColumn("batch", "numRows", key.dataType, groupingKeys.length + ordinal,
          buffVars(ordinal), nullable = true)
      }
    }

    s"""
       |public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(${
            groupingKeySignature}) {
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
       |        batch.setNumRows(numRows);
       |        aggregateBufferBatch.setNumRows(numRows);
       |        return aggregateBufferBatch.getRow(buckets[idx]);
       |      } else {
       |        // No more space
       |        return null;
       |      }
       |    } else if (equals(idx, ${groupingKeys.map(_.name).mkString(", ")})) {
       |      return aggregateBufferBatch.getRow(buckets[idx]);
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
       |public java.util.Iterator<org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row>
       |    rowIterator() {
       |  return batch.rowIterator();
       |}
     """.stripMargin
  }
}

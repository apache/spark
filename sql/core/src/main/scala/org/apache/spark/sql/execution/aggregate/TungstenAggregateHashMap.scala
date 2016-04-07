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

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.StructType

class TungstenAggregateHashMap(
    ctx: CodegenContext,
    generatedClassName: String,
    groupingKeySchema: StructType,
    bufferSchema: StructType) {
  val groupingKeys = groupingKeySchema.map(k => (k.dataType.typeName, ctx.freshName("key")))
  val bufferValues = bufferSchema.map(k => (k.dataType.typeName, ctx.freshName("value")))
  val groupingKeySignature = groupingKeys.map(_.productIterator.toList.mkString(" ")).mkString(", ")

  def generate(): String = {
    s"""
       |public class $generatedClassName {
       |${initializeAggregateHashMap()}
       |
       |${generateFindOrInsert()}
       |
       |${generateEquals()}
       |
       |${generateHashFunction()}
       |}
     """.stripMargin
  }

  def initializeAggregateHashMap(): String = {
    val generatedSchema: String =
      s"""
         |new org.apache.spark.sql.types.StructType()
         |${(groupingKeySchema ++ bufferSchema).map(key =>
          s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})""")
          .mkString("\n")};
      """.stripMargin

    s"""
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch batch;
       |  private int[] buckets;
       |  private int numBuckets;
       |  private int maxSteps;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |
       |  public $generatedClassName(int capacity, double loadFactor, int maxSteps) {
       |    assert (capacity > 0 && ((capacity & (capacity - 1)) == 0));
       |    this.maxSteps = maxSteps;
       |    numBuckets = (int) (capacity / loadFactor);
       |    batch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(schema,
       |      org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
       |
       |  public $generatedClassName() {
       |    new $generatedClassName(1 << 16, 0.25, 5);
       |  }
     """.stripMargin
  }

  def generateHashFunction(): String = {
    s"""
       |// TODO: Improve this Hash Function
       |private long hash($groupingKeySignature) {
       |  return ${groupingKeys.map(_._2).mkString(" ^ ")};
       |}
     """.stripMargin
  }

  def generateEquals(): String = {
    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  return ${groupingKeys.zipWithIndex.map(k =>
            s"batch.column(${k._2}).getLong(buckets[idx]) == ${k._1._2}").mkString(" && ")};
       |}
     """.stripMargin
  }

  def generateFindOrInsert(): String = {
    s"""
       |public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(${
          groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_._2).mkString(", ")});
       |  int step = 0;
       |  int idx = (int) h & (numBuckets - 1);
       |  while (step < maxSteps) {
       |    // Return bucket index if it's either an empty slot or already contains the key
       |    if (buckets[idx] == -1) {
       |      ${groupingKeys.zipWithIndex.map(k =>
                s"batch.column(${k._2}).putLong(numRows, ${k._1._2});").mkString("\n")}
       |      ${bufferValues.zipWithIndex.map(k =>
                s"batch.column(${groupingKeys.length + k._2}).putLong(numRows, 0);")
                .mkString("\n")}
       |      buckets[idx] = numRows++;
       |      return batch.getRow(buckets[idx]);
       |    } else if (equals(idx, ${groupingKeys.map(_._2).mkString(", ")})) {
       |      return batch.getRow(buckets[idx]);
       |    }
       |    idx = (idx + 1) & (numBuckets - 1);
       |    step++;
       |  }
       |// Didn't find it
       |return null;
       |}
     """.stripMargin
  }
}

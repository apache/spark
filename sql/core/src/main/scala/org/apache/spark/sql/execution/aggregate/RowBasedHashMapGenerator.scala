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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
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
    bufferSchema: StructType) {
  case class Buffer(dataType: DataType, name: String)
  val groupingKeys = groupingKeySchema.map(k => Buffer(k.dataType, ctx.freshName("key")))
  val bufferValues = bufferSchema.map(k => Buffer(k.dataType, ctx.freshName("value")))
  val groupingKeySignature =
    groupingKeys.map(key => s"${ctx.javaType(key.dataType)} ${key.name}").mkString(", ")
  val buffVars: Seq[ExprCode] = {
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      val ev = e.genCode(ctx)
      val initVars =
        s"""
           | $isNull = ${ev.isNull};
           | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }
  }

  def generate(): String = {
    s"""
       |public class $generatedClassName extends org.apache.spark.memory.MemoryConsumer{
       |${initializeAggregateHashMap()}
       |
       |${generateFindOrInsert()}
       |
       |${generateEquals()}
       |
       |${generateHashFunction()}
       |
       |${generateRowIterator()}
       |
       |${generateClose()}
       |
       |${generateSpill()}
       |}
     """.stripMargin
  }

  private def initializeAggregateHashMap(): String = {
    val generatedKeySchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        groupingKeySchema.map { key =>
          key.dataType match {
            case d: DecimalType =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    val generatedValueSchema: String =
      s"new org.apache.spark.sql.types.StructType()" +
        bufferSchema.map { key =>
          key.dataType match {
            case d: DecimalType =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.createDecimalType(
                  |${d.precision}, ${d.scale}))""".stripMargin
            case _ =>
              s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})"""
          }
        }.mkString("\n").concat(";")

    s"""
       |  private org.apache.spark.sql.catalyst.expressions.SimpleRowBatch batch;
       |  private int[] buckets;
       |  private int capacity = 1 << 16;
       |  private double loadFactor = 0.5;
       |  private int numBuckets = (int) (capacity / loadFactor);
       |  private int maxSteps = 2;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType keySchema = $generatedKeySchema
       |  private org.apache.spark.sql.types.StructType valueSchema = $generatedValueSchema
       |  private Object emptyVBase;
       |  private long emptyVOff;
       |  private int emptyVLen;
       |  private boolean isBatchFull = false;
       |
       |
       |  public $generatedClassName(
       |    org.apache.spark.memory.TaskMemoryManager taskMemoryManager,
       |    InternalRow emptyAggregationBuffer) {
       |    super(taskMemoryManager,
       |      taskMemoryManager.pageSizeBytes(),
       |      taskMemoryManager.getTungstenMemoryMode());
       |    batch = org.apache.spark.sql.catalyst.expressions.SimpleRowBatch.allocate(keySchema,
       |      valueSchema, taskMemoryManager, capacity);
       |
       |    final UnsafeProjection valueProjection = UnsafeProjection.create(valueSchema);
       |    final byte[] emptyBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
       |
       |    emptyVBase = emptyBuffer;
       |    emptyVOff = Platform.BYTE_ARRAY_OFFSET;
       |    emptyVLen = emptyBuffer.length;
       |
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
     """.stripMargin
  }

  /**
   * Generates a method that computes a hash by currently xor-ing all individual group-by keys. For
   * instance, if we have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private long hash(long agg_key, long agg_key1) {
   *   return agg_key ^ agg_key1;
   *   }
   * }}}
   */
  private def generateHashFunction(): String = {
    val hash = ctx.freshName("hash")

    def genHashForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.map { key =>
        val result = ctx.freshName("result")
        s"""
           |${genComputeHash(ctx, key.name, key.dataType, result)}
           |$hash = ($hash ^ (0x9e3779b9)) + $result + ($hash << 6) + ($hash >>> 2);
          """.stripMargin
      }.mkString("\n")
    }

    s"""
       |private long hash($groupingKeySignature) {
       |  long $hash = 0;
       |  ${genHashForKeys(groupingKeys)}
       |  return $hash;
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns true if the group-by keys exist at a given index in the
   * associated [[org.apache.spark.sql.catalyst.expressions.RowBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   */
  private def generateEquals(): String = {

    def genEqualsForKeys(groupingKeys: Seq[Buffer]): String = {
      groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
        s"""(${ctx.genEqual(key.dataType, ctx.getValue("row",
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
   * [[org.apache.spark.sql.catalyst.expressions.RowBatch]].
   *
   * TODO: add an example instance
   *
   */
  private def generateFindOrInsert(): String = {
    val numVarLenFields = groupingKeys.map(_.dataType).count {
      case dt if UnsafeRow.isFixedLength(dt) => false
      // TODO: consider large decimal and interval type
      case _ => true
    }

    val createUnsafeRowForKey = groupingKeys.zipWithIndex.map { case (key: Buffer, ordinal: Int) =>
      s"agg_rowWriter.write(${ordinal}, ${key.name})"}
      .mkString(";\n")

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
       |        // creating the unsafe for new entry
       |        UnsafeRow agg_result = new UnsafeRow(${groupingKeySchema.length});
       |        org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder agg_holder
       |          = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(agg_result,
       |            ${numVarLenFields * 32});
       |        org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter agg_rowWriter
       |          = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(
       |              agg_holder,
       |              ${groupingKeySchema.length});
       |        agg_holder.reset(); //TODO: investigate if reset or zeroout are actually needed
       |        agg_rowWriter.zeroOutNullBytes();
       |        ${createUnsafeRowForKey};
       |        agg_result.setTotalSize(agg_holder.totalSize());
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

  private def generateRowIterator(): String = {
    s"""
       |public org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator() {
       |  return batch.rowIterator();
       |}
     """.stripMargin
  }

  private def generateClose(): String = {
    s"""
       |public void close() {
       |  batch.close();
       |}
     """.stripMargin
  }

  private def generateSpill(): String = {
    s"""
       |public long spill(long size, org.apache.spark.memory.MemoryConsumer trigger)
       |  throws java.io.IOException {
       |  return batch.spill(0, this);
       |}
     """.stripMargin
  }

  private def genComputeHash(
      ctx: CodegenContext,
      input: String,
      dataType: DataType,
      result: String): String = {
    def hashInt(i: String): String = s"int $result = $i;"
    def hashLong(l: String): String = s"long $result = $l;"
    def hashBytes(b: String): String = {
      val hash = ctx.freshName("hash")
      s"""
         |int $result = 0;
         |for (int i = 0; i < $b.length; i++) {
         |  ${genComputeHash(ctx, s"$b[i]", ByteType, hash)}
         |  $result = ($result ^ (0x9e3779b9)) + $hash + ($result << 6) + ($result >>> 2);
         |}
       """.stripMargin
    }

    dataType match {
      case BooleanType => hashInt(s"$input ? 1 : 0")
      case ByteType | ShortType | IntegerType | DateType => hashInt(input)
      case LongType | TimestampType => hashLong(input)
      case FloatType => hashInt(s"Float.floatToIntBits($input)")
      case DoubleType => hashLong(s"Double.doubleToLongBits($input)")
      case d: DecimalType =>
        if (d.precision <= Decimal.MAX_LONG_DIGITS) {
          hashLong(s"$input.toUnscaledLong()")
        } else {
          val bytes = ctx.freshName("bytes")
          s"""
            final byte[] $bytes = $input.toJavaBigDecimal().unscaledValue().toByteArray();
            ${hashBytes(bytes)}
          """
        }
      case StringType => hashBytes(s"$input.getBytes()")
    }
  }
}

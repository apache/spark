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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

/**
 * This is a helper class to generate an append-only row-based hash map that can act as a 'cache'
 * for extremely fast key-value lookups while evaluating aggregates (and fall back to the
 * `BytesToBytesMap` if a given key isn't found). This is 'codegened' in HashAggregate to speed
 * up aggregates w/ key.
 *
 * NOTE: the generated hash map currently doesn't support nullable keys and falls back to the
 * `BytesToBytesMap` to store them.
 */
abstract class HashMapGenerator(
    ctx: CodegenContext,
    aggregateExpressions: Seq[AggregateExpression],
    generatedClassName: String,
    groupingKeySchema: StructType,
    bufferSchema: StructType) {
  case class Buffer(dataType: DataType, name: String)

  val groupingKeys = groupingKeySchema.map(k => Buffer(k.dataType, ctx.freshName("key")))
  val bufferValues = bufferSchema.map(k => Buffer(k.dataType, ctx.freshName("value")))
  val groupingKeySignature =
    groupingKeys.map(key => s"${CodeGenerator.javaType(key.dataType)} ${key.name}").mkString(", ")
  val buffVars: Seq[ExprCode] = {
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    initExpr.map { e =>
      val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "bufIsNull")
      val value = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "bufValue")
      val ev = e.genCode(ctx)
      val initVars =
        code"""
           | $isNull = ${ev.isNull};
           | $value = ${ev.value};
       """.stripMargin
      ExprCode(
        ev.code + initVars,
        JavaCode.isNullGlobal(isNull),
        JavaCode.global(value, e.dataType))
    }
  }

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
       |
       |${generateRowIterator()}
       |
       |${generateClose()}
       |}
     """.stripMargin
  }

  protected def initializeAggregateHashMap(): String

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
  protected final def generateHashFunction(): String = {
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
   * Generates a method that returns true if the group-by keys exist at a given index.
   */
  protected def generateEquals(): String

  /**
   * Generates a method that returns a row which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated key value batch.
   */
  protected def generateFindOrInsert(): String

  protected def generateRowIterator(): String

  protected final def generateClose(): String = {
    s"""
       |public void close() {
       |  batch.close();
       |}
     """.stripMargin
  }

  protected final def genComputeHash(
      ctx: CodegenContext,
      input: String,
      dataType: DataType,
      result: String): String = {
    def hashInt(i: String): String = s"int $result = $i;"
    def hashLong(l: String): String = s"long $result = $l;"
    def hashBytes(b: String): String = {
      val hash = ctx.freshName("hash")
      val bytes = ctx.freshName("bytes")
      s"""
         |int $result = 0;
         |byte[] $bytes = $b;
         |for (int i = 0; i < $bytes.length; i++) {
         |  ${genComputeHash(ctx, s"$bytes[i]", ByteType, hash)}
         |  $result = ($result ^ (0x9e3779b9)) + $hash + ($result << 6) + ($result >>> 2);
         |}
       """.stripMargin
    }

    dataType match {
      case BooleanType => hashInt(s"$input ? 1 : 0")
      case ByteType | ShortType | IntegerType | DateType | _: YearMonthIntervalType =>
        hashInt(input)
      case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType => hashLong(input)
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

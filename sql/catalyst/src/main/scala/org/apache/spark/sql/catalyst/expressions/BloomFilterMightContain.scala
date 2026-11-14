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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLId, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, JavaCode, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.optimizer.ScalarSubqueryReference
import org.apache.spark.sql.catalyst.trees.TreePattern.{OUTER_REFERENCE, PLAN_EXPRESSION, SCALAR_SUBQUERY_REFERENCE}
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.BloomFilter

/**
 * An internal scalar function that returns the membership check result (either true or false)
 * for values of `valueExpression` in the Bloom filter represented by `bloomFilterExpression`.
 * Not that since the function is "might contain", always returning true regardless is not
 * wrong.
 * Note that this expression requires that `bloomFilterExpression` is a constant value, an
 * uncorrelated scalar subquery, or an expression without subqueries. The last is read from each
 * input row, such as one bloom per partition key on the other side of a join. This is sufficient
 * for the Bloom filter join rewrites.
 *
 * @param bloomFilterExpression the Binary data of Bloom filter.
 * @param valueExpression the Long value to be tested for the membership of `bloomFilterExpression`.
 */
case class BloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression) extends BinaryExpression with Predicate {

  override def nullable: Boolean = true
  override def left: Expression = bloomFilterExpression
  override def right: Expression = valueExpression
  override def prettyName: String = "might_contain"

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (BinaryType, NullType) | (NullType, LongType) | (NullType, NullType) |
           (BinaryType, LongType) =>
        bloomFilterExpression match {
          case e : Expression if e.foldable => TypeCheckResult.TypeCheckSuccess
          case subquery : PlanExpression[_] if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case GetStructField(subquery: PlanExpression[_], _, _)
            if !subquery.containsPattern(OUTER_REFERENCE) =>
            TypeCheckResult.TypeCheckSuccess
          case _: ScalarSubqueryReference => TypeCheckResult.TypeCheckSuccess
          case e if !hasSubquery(e) => TypeCheckResult.TypeCheckSuccess
          case _ =>
            DataTypeMismatch(
              errorSubClass = "BLOOM_FILTER_BINARY_OP_WRONG_TYPE",
              messageParameters = Map(
                "functionName" -> toSQLId(prettyName),
                "actual" -> toSQLExpr(bloomFilterExpression)
              )
            )
        }
      case _ =>
        DataTypeMismatch(
          errorSubClass = "BLOOM_FILTER_WRONG_TYPE",
          messageParameters = Map(
            "functionName" -> toSQLId(prettyName),
            "expectedLeft" -> toSQLType(BinaryType),
            "expectedRight" -> toSQLType(LongType),
            "actual" -> Seq(left.dataType, right.dataType).map(toSQLType).mkString(", ")
          )
        )
    }
  }

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): BloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression,
      valueExpression = newValueExpression)

  private def hasSubquery(e: Expression): Boolean =
    e.containsAnyPattern(PLAN_EXPRESSION, SCALAR_SUBQUERY_REFERENCE)

  // Whether the bloom comes from the input row, so it has to be read per row. A constant or a
  // scalar subquery is read once.
  @transient private lazy val bloomIsPerRow =
    !bloomFilterExpression.foldable && !hasSubquery(bloomFilterExpression)

  // The last bloom read per row, with the bytes it was deserialized from, for both `eval` and the
  // generated code. The bloom is reused while rows hand over the same array. One field, so a
  // reader never pairs one bloom's bytes with another's filter.
  @transient private var lastBloomFilter: (Array[Byte], BloomFilter) = _

  private[sql] def bloomFilterOf(bytes: Array[Byte]): BloomFilter = {
    val last = lastBloomFilter
    if (last != null && (last._1 eq bytes)) {
      last._2
    } else {
      val bloomFilter = deserialize(bytes)
      lastBloomFilter = (bytes, bloomFilter)
      bloomFilter
    }
  }

  // The bloom filter created from `bloomFilterExpression`.
  @transient private lazy val bloomFilter = {
    val bytes = bloomFilterExpression.eval().asInstanceOf[Array[Byte]]
    if (bytes == null) null else deserialize(bytes)
  }

  override def eval(input: InternalRow): Any = {
    if (bloomIsPerRow) {
      super.eval(input)
    } else if (bloomFilter == null) {
      null
    } else {
      val value = valueExpression.eval(input)
      if (value == null) null else bloomFilter.mightContainLong(value.asInstanceOf[Long])
    }
  }

  // Only for a bloom read per row, see `eval`.
  override protected def nullSafeEval(bytes: Any, value: Any): Any =
    bloomFilterOf(bytes.asInstanceOf[Array[Byte]]).mightContainLong(value.asInstanceOf[Long])

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (bloomIsPerRow && left.dataType != NullType && right.dataType != NullType) {
      // Calls `bloomFilterOf`, which keeps the last bloom. `BloomFilter.readFrom` declares a
      // checked exception, so the generated Java does not call it directly.
      val self = ctx.addReferenceObj("bloomFilterMightContain", this)
      nullSafeCodeGen(ctx, ev, (bytes, value) =>
        s"${ev.value} = $self.bloomFilterOf($bytes).mightContainLong($value);")
    } else if (bloomIsPerRow || bloomFilter == null) {
      // Null on every row, for a null bloom read once or a bloom read per row with a null-typed
      // side. Such a side has no Java type to call the bloom with.
      ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    } else {
      val bf = ctx.addReferenceObj("bloomFilter", bloomFilter, classOf[BloomFilter].getName)
      val valueEval = valueExpression.genCode(ctx)
      ev.copy(code = code"""
      ${valueEval.code}
      boolean ${ev.isNull} = ${valueEval.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = $bf.mightContainLong((Long)${valueEval.value});
      }""")
    }
  }

  final def deserialize(bytes: Array[Byte]): BloomFilter = BloomFilter.readFrom(bytes)
}

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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.BinaryPrefixComparator
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.DoublePrefixComparator

abstract sealed class SortDirection {
  def sql: String
}

case object Ascending extends SortDirection {
  override def sql: String = "ASC"
}

case object Descending extends SortDirection {
  override def sql: String = "DESC"
}

/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 */
case class SortOrder(child: Expression, direction: SortDirection)
  extends UnaryExpression with Unevaluable {

  /** Sort order is not foldable because we don't have an eval for it. */
  override def foldable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (RowOrdering.isOrderable(dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"cannot sort data type ${dataType.simpleString}")
    }
  }

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  override def toString: String = s"$child ${direction.sql}"
  override def sql: String = child.sql + " " + direction.sql

  def isAscending: Boolean = direction == Ascending
}

/**
 * An expression to generate a 64-bit long prefix used in sorting.
 */
case class SortPrefix(child: SortOrder) extends UnaryExpression {

  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.child.genCode(ctx)
    val input = childCode.value
    val BinaryPrefixCmp = classOf[BinaryPrefixComparator].getName
    val DoublePrefixCmp = classOf[DoublePrefixComparator].getName

    val (nullValue: Long, prefixCode: String) = child.child.dataType match {
      case BooleanType =>
        (Long.MinValue, s"$input ? 1L : 0L")
      case _: IntegralType =>
        (Long.MinValue, s"(long) $input")
      case DateType | TimestampType =>
        (Long.MinValue, s"(long) $input")
      case FloatType | DoubleType =>
        (0L, s"$DoublePrefixCmp.computePrefix((double)$input)")
      case StringType => (0L, s"$input.getPrefix()")
      case BinaryType => (0L, s"$BinaryPrefixCmp.computePrefix($input)")
      case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
        val prefix = if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
          s"$input.toUnscaledLong()"
        } else {
          // reduce the scale to fit in a long
          val p = Decimal.MAX_LONG_DIGITS
          val s = p - (dt.precision - dt.scale)
          s"$input.changePrecision($p, $s) ? $input.toUnscaledLong() : ${Long.MinValue}L"
        }
        (Long.MinValue, prefix)
      case dt: DecimalType =>
        (DoublePrefixComparator.computePrefix(Double.NegativeInfinity),
          s"$DoublePrefixCmp.computePrefix($input.toDouble())")
      case _ => (0L, "0L")
    }

    ev.copy(code = childCode.code +
      s"""
         |long ${ev.value} = ${nullValue}L;
         |boolean ${ev.isNull} = false;
         |if (!${childCode.isNull}) {
         |  ${ev.value} = $prefixCode;
         |}
      """.stripMargin)
  }

  override def dataType: DataType = LongType
}

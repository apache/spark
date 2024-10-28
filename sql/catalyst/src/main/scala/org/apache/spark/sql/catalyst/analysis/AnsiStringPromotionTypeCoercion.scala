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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  BinaryOperator,
  Cast,
  DateAdd,
  DateSub,
  Expression,
  Literal,
  SubtractDates,
  SubtractTimestamps,
  TimeAdd,
  UnaryMinus,
  UnaryPositive
}
import org.apache.spark.sql.types.{
  AnsiIntervalType,
  AtomicType,
  DataType,
  DateType,
  DoubleType,
  FractionalType,
  IntegerType,
  IntegralType,
  LongType,
  NullType,
  StringType,
  StringTypeExpression,
  TimestampType
}

/**
 * ANSI type coercion helper that matches against expressions in order to type coerce children to
 * a wider type when one of the children is a string.
 */
object AnsiStringPromotionTypeCoercion {
  def apply(expression: Expression): Expression = expression match {
    case b @ BinaryOperator(left, right)
        if findWiderTypeForString(left.dataType, right.dataType).isDefined =>
      val promoteType = findWiderTypeForString(left.dataType, right.dataType).get
      b.withNewChildren(Seq(castExpr(left, promoteType), castExpr(right, promoteType)))

    case Abs(e @ StringTypeExpression(), failOnError) => Abs(Cast(e, DoubleType), failOnError)
    case m @ UnaryMinus(e @ StringTypeExpression(), _) =>
      m.withNewChildren(Seq(Cast(e, DoubleType)))
    case UnaryPositive(e @ StringTypeExpression()) => UnaryPositive(Cast(e, DoubleType))

    case d @ DateAdd(left @ StringTypeExpression(), _) =>
      d.copy(startDate = Cast(d.startDate, DateType))
    case d @ DateAdd(_, right @ StringTypeExpression()) =>
      d.copy(days = Cast(right, IntegerType))
    case d @ DateSub(left @ StringTypeExpression(), _) =>
      d.copy(startDate = Cast(d.startDate, DateType))
    case d @ DateSub(_, right @ StringTypeExpression()) =>
      d.copy(days = Cast(right, IntegerType))

    case s @ SubtractDates(left @ StringTypeExpression(), _, _) =>
      s.copy(left = Cast(s.left, DateType))
    case s @ SubtractDates(_, right @ StringTypeExpression(), _) =>
      s.copy(right = Cast(s.right, DateType))
    case t @ TimeAdd(left @ StringTypeExpression(), _, _) =>
      t.copy(start = Cast(t.start, TimestampType))
    case t @ SubtractTimestamps(left @ StringTypeExpression(), _, _, _) =>
      t.copy(left = Cast(t.left, t.right.dataType))
    case t @ SubtractTimestamps(_, right @ StringTypeExpression(), _, _) =>
      t.copy(right = Cast(right, t.left.dataType))

    case other => other
  }

  /** Promotes StringType to other data types. */
  @scala.annotation.tailrec
  private[catalyst] def findWiderTypeForString(dt1: DataType, dt2: DataType): Option[DataType] = {
    (dt1, dt2) match {
      case (_: StringType, _: IntegralType) => Some(LongType)
      case (_: StringType, _: FractionalType) => Some(DoubleType)
      case (st: StringType, NullType) => Some(st)
      // If a binary operation contains interval type and string, we can't decide which
      // interval type the string should be promoted as. There are many possible interval
      // types, such as year interval, month interval, day interval, hour interval, etc.
      case (_: StringType, _: AnsiIntervalType) => None
      // [SPARK-50060] If a binary operation contains two collated string types with different
      // collation IDs, we can't decide which collation ID the result should have.
      case (st1: StringType, st2: StringType) if st1.collationId != st2.collationId => None
      case (_: StringType, a: AtomicType) => Some(a)
      case (other, st: StringType) if !other.isInstanceOf[StringType] =>
        findWiderTypeForString(st, other)
      case _ => None
    }
  }

  private def castExpr(expr: Expression, targetType: DataType): Expression = {
    expr.dataType match {
      case NullType => Literal.create(null, targetType)
      case l if l != targetType => Cast(expr, targetType)
      case _ => expr
    }
  }
}

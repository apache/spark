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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Adds a number of days to startdate: date_add('2008-12-31', 1) = '2009-01-01'.
 */
case class DateAdd(startDate: Expression, days: Expression) extends Expression {
  override def children: Seq[Expression] = startDate :: days :: Nil

  override def foldable: Boolean = startDate.foldable && days.foldable
  override def nullable: Boolean = startDate.nullable || days.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    val supportedLeftType = Seq(StringType, DateType, TimestampType, NullType)
    if (!supportedLeftType.contains(startDate.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of startdate expression in DateAdd should be string/timestamp/date," +
          s" not ${startDate.dataType}")
    } else if (days.dataType != IntegerType && days.dataType != NullType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of days expression in DateAdd should be int, not ${days.dataType}.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    val start = startDate.eval(input)
    if (start == null) {
      null
    } else {
      val d = days.eval(input)
      if (d == null) {
        null
      } else {
        val offset = d.asInstanceOf[Int]
        val resultDays = startDate.dataType match {
          case StringType =>
            DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(
              start.asInstanceOf[UTF8String].toString).getTime) + offset
          case TimestampType =>
            DateTimeUtils.timestampTypeToDateType(start.asInstanceOf[Long]) + offset
          case DateType => start.asInstanceOf[Int] + offset
        }
        resultDays
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalStart = startDate.gen(ctx)
    val evalDays = days.gen(ctx)
    val dateUtils = "org.apache.spark.sql.catalyst.util.DateTimeUtils"
    val startToDay: String = startDate.dataType match {
      case StringType =>
        s"""$dateUtils.millisToDays(
          $dateUtils.stringToTime(${evalStart.primitive}.toString()).getTime())"""
      case TimestampType =>
        s"$dateUtils.millisToDays($dateUtils.toJavaTimestamp(${evalStart.primitive}).getTime())"
      case DateType => evalStart.primitive
      case _ => "" // for NullType
    }
    evalStart.code + evalDays.code + s"""
      boolean ${ev.isNull} = ${evalStart.isNull} || ${evalDays.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = $startToDay + ${evalDays.primitive};
      }
    """
  }
}

/**
 * Subtracts a number of days to startdate: date_sub('2008-12-31', 1) = '2008-12-30'.
 */
case class DateSub(startDate: Expression, days: Expression) extends Expression {
  override def children: Seq[Expression] = startDate :: days :: Nil

  override def foldable: Boolean = startDate.foldable && days.foldable
  override def nullable: Boolean = startDate.nullable || days.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    val supportedLeftType = Seq(StringType, DateType, TimestampType, NullType)
    if (!supportedLeftType.contains(startDate.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of startdate expression in DateSub should be string/timestamp/date," +
          s" not ${startDate.dataType}")
    } else if (days.dataType != IntegerType && days.dataType != NullType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of days expression in DateSub should be int, not ${days.dataType}.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    val start = startDate.eval(input)
    if (start == null) {
      null
    } else {
      val d = days.eval(input)
      if (d == null) {
        null
      } else {
        val offset = d.asInstanceOf[Int]
        val resultDays = startDate.dataType match {
          case StringType =>
            DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(
              start.asInstanceOf[UTF8String].toString).getTime) - offset
          case TimestampType =>
            DateTimeUtils.timestampTypeToDateType(start.asInstanceOf[Long]) - offset
          case DateType => start.asInstanceOf[Int] - offset
        }
        resultDays
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalStart = startDate.gen(ctx)
    val evalDays = days.gen(ctx)
    val dateUtils = "org.apache.spark.sql.catalyst.util.DateTimeUtils"
    val startToDay: String = startDate.dataType match {
      case StringType =>
        s"""$dateUtils.millisToDays(
          $dateUtils.stringToTime(${evalStart.primitive}.toString()).getTime())"""
      case TimestampType =>
        s"$dateUtils.millisToDays($dateUtils.toJavaTimestamp(${evalStart.primitive}).getTime())"
      case DateType => evalStart.primitive
      case _ => "" // for NullType
    }
    evalStart.code + evalDays.code + s"""
      boolean ${ev.isNull} = ${evalStart.isNull} || ${evalDays.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.primitive} = $startToDay - ${evalDays.primitive};
      }
    """
  }
}

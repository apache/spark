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

import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  override def dataType: DataType = StringType

  override def toString: String = s"DateAdd($startDate, $days)"

  override def eval(input: Row): Any = {
    val start = startDate.eval(input)
    val d = days.eval(input)
    if (start == null || d == null) {
      null
    } else {
      val offset = d.asInstanceOf[Int]
      val resultDays = startDate.dataType match {
        case StringType =>
          DateUtils.millisToDays(DateUtils.stringToTime(
            start.asInstanceOf[UTF8String].toString).getTime) + offset
        case TimestampType =>
          DateUtils.millisToDays(DateUtils.toJavaTimestamp(
            start.asInstanceOf[Long]).getTime) + offset
        case DateType => start.asInstanceOf[Int] + offset
      }
      UTF8String.fromString(DateUtils.toString(resultDays))
    }
  }
}

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

  override def dataType: DataType = StringType

  override def toString: String = s"DateSub($startDate, $days)"

  override def eval(input: Row): Any = {
    val start = startDate.eval(input)
    val d = days.eval(input)
    if (start == null || d == null) {
      null
    } else {
      val offset = d.asInstanceOf[Int]
      val resultDays = startDate.dataType match {
        case StringType =>
          DateUtils.millisToDays(DateUtils.stringToTime(
            start.asInstanceOf[UTF8String].toString).getTime) - offset
        case TimestampType =>
          DateUtils.millisToDays(DateUtils.toJavaTimestamp(
            start.asInstanceOf[Long]).getTime) - offset
        case DateType => start.asInstanceOf[Int] - offset
      }
      UTF8String.fromString(DateUtils.toString(resultDays))
    }
  }
}

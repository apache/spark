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

import java.util.{TimeZone, Calendar}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns the date that is num_months after start_date.
 */
case class AddMonths(startDate: Expression, months: Expression) extends Expression {
  override def children: Seq[Expression] = startDate :: months :: Nil

  override def foldable: Boolean = startDate.foldable && months.foldable
  override def nullable: Boolean = startDate.nullable || months.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    val supportedLeftType = Seq(StringType, DateType, TimestampType, NullType)
    if (!supportedLeftType.contains(startDate.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of startdate expression in AddMonths should be string/timestamp/date," +
          s" not ${startDate.dataType}")
    } else if (months.dataType != IntegerType && months.dataType != NullType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of months expression in AddMonths should be int, not ${months.dataType}.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = DateType

  private val calendar = Calendar.getInstance()

  override def eval(input: InternalRow): Any = {
    val start = startDate.eval(input)
    if (start == null) {
      null
    } else {
      val m = months.eval(input)
      if (m == null) {
        null
      } else {
        val resultDayMillis = startDate.dataType match {
          case StringType =>
            DateTimeUtils.stringToTime(start.asInstanceOf[UTF8String].toString).getTime
          case TimestampType =>
            start.asInstanceOf[Long] * 100L
          case DateType => DateTimeUtils.toMillisSinceEpoch(start.asInstanceOf[Int])
        }
        calendar.setTime(new java.util.Date(resultDayMillis))
        calendar.add(Calendar.MONTH, m.asInstanceOf[Int])
        DateTimeUtils.millisToDays(calendar.getTime.getTime)
      }
    }
  }

}

/**
 * Returns number of months between dates date1 and date2.
 */
case class MonthsBetween(date1: Expression, date2: Expression) extends Expression {
  override def children: Seq[Expression] = date1 :: date2 :: Nil

  override def foldable: Boolean = date1.foldable && date2.foldable
  override def nullable: Boolean = date1.nullable || date2.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    val supportedType = Seq(StringType, DateType, TimestampType, NullType)
    if (!supportedType.contains(date1.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of date1 expression in MonthsBetween should be string/timestamp/date," +
          s" not ${date1.dataType}")
    } else if (!supportedType.contains(date2.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of date2 expression in MonthsBetween should be string/timestamp/date," +
          s" not ${date2.dataType}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = DoubleType
  private val calendar = Calendar.getInstance()

  override def eval(input: InternalRow): Any = {
    val d1 = date1.eval(input)
    val d2 = date2.eval(input)
    if (d1 == null || d2 == null) {
      null
    } else {
      def extractDays(raw: Any, dt: DataType): Long = dt match {
        case StringType =>
          DateTimeUtils.stringToTime(raw.asInstanceOf[UTF8String].toString).getTime
        case TimestampType =>
          raw.asInstanceOf[Long] * 100L
        case DateType => DateTimeUtils.toMillisSinceEpoch(raw.asInstanceOf[Int])
      }
      calendar.setTime(new java.util.Date(extractDays(d1, date1.dataType)))
      val dom1 = calendar.get(Calendar.DAY_OF_MONTH)
      val y1 = calendar.get(Calendar.YEAR)
      val m1 = calendar.get(Calendar.MONTH)
      val daysInMonth1 = calendar.getMaximum(Calendar.DAY_OF_MONTH)
      calendar.setTime(new java.util.Date(extractDays(d2, date2.dataType)))
      val dom2 = calendar.get(Calendar.DAY_OF_MONTH)
      val y2 = calendar.get(Calendar.YEAR)
      val m2 = calendar.get(Calendar.MONTH)
      val daysInMonth2 = calendar.getMaximum(Calendar.DAY_OF_MONTH)
      if (dom1 == dom2 || (dom1 == daysInMonth1 && dom2 == daysInMonth2)) {
        ((y1 - y2) * 12 + (m1 - m2)).toDouble
      } else {
        ((y1 - y2) * 12 + (m1 - m2)).toDouble + (dom1 - dom2).toDouble / 31.0
      }
    }
  }
}

case class DateDiff(endDate: Expression, startDate: Expression) extends Expression {

  override def children: Seq[Expression] = startDate :: endDate :: Nil

  override def foldable: Boolean = startDate.foldable && endDate.foldable
  override def nullable: Boolean = startDate.nullable || endDate.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    val supportedType = Seq(StringType, DateType, TimestampType, NullType)
    if (!supportedType.contains(startDate.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of startdate expression in DateDiff should be string/timestamp/date," +
          s" not ${startDate.dataType}")
    } else if (!supportedType.contains(endDate.dataType)) {
      TypeCheckResult.TypeCheckFailure(
        s"type of enddate expression in DateDiff should be string/timestamp/date," +
          s" not ${endDate.dataType}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = IntegerType

  override def toString: String = s"DateDiff($endDate, $startDate)"

  private val MILLIS_PER_DAY = 86400000

  @transient private val LOCAL_TIMEZONE = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  override def eval(input: InternalRow): Any = {
    val end = endDate.eval(input)
    val start = startDate.eval(input)
    if (start == null || end == null) {
      null
    } else {
      def extractUTCDays(raw: Any, dt: DataType): Int = {
        val localTime = dt match {
          case StringType => DateTimeUtils.stringToTime(raw.asInstanceOf[UTF8String].toString).getTime
          case TimestampType => DateTimeUtils.toJavaTimestamp(raw.asInstanceOf[Long]).getTime
          case DateType => DateTimeUtils.toJavaDate(raw.asInstanceOf[Int]).getTime
        }
        val utcTime = localTime + LOCAL_TIMEZONE.get().getOffset(localTime)
        (utcTime / MILLIS_PER_DAY).toInt
      }
      extractUTCDays(end, endDate.dataType) - extractUTCDays(start, startDate.dataType)
      //extractDays(end, endDate.dataType) - extractDays(start, startDate.dataType)
    }
  }
}

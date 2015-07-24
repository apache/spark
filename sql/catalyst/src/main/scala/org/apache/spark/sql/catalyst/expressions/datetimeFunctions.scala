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

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{Interval, UTF8String}

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentDate() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentTimestamp() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }
}

/**
 * Adds a number of days to startdate.
 */
case class DateAdd(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Int]
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.primitive} = $sd + $d;"""
    })
  }
}

/**
 * Subtracts a number of days to startdate.
 */
case class DateSub(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Int]
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.primitive} = $sd - $d;"""
    })
  }
}

case class Hour(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getHours($c)"""
    )
  }
}

case class Minute(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getMinutes($c)"""
    )
  }
}

case class Second(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getSeconds($c)"""
    )
  }
}

case class DayOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayInYear(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getDayInYear($c)"""
    )
  }
}


case class Year(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getYear(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c =>
      s"""$dtu.getYear($c)"""
    )
  }
}

case class Quarter(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getQuarter(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getQuarter($c)"""
    )
  }
}

case class Month(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMonth(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getMonth($c)"""
    )
  }
}

case class DayOfMonth(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (c) =>
      s"""$dtu.getDayOfMonth($c)"""
    )
  }
}

case class WeekOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c.setMinimalDaysInFirstWeek(4)
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.get(Calendar.WEEK_OF_YEAR)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (time) => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      s"""
        $cal $c = $cal.getInstance(java.util.TimeZone.getTimeZone("UTC"));
        $c.setFirstDayOfWeek($cal.MONDAY);
        $c.setMinimalDaysInFirstWeek(4);
        $c.setTimeInMillis($time * 1000L * 3600L * 24L);
        ${ev.primitive} = $c.get($cal.WEEK_OF_YEAR);
      """
    })
  }
}

case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def prettyName: String = "date_format"

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString)
    UTF8String.fromString(sdf.format(new Date(timestamp.asInstanceOf[Long] / 1000)))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (timestamp, format) => {
      s"""UTF8String.fromString((new $sdf($format.toString()))
          .format(new java.sql.Date($timestamp / 1000)))"""
    })
  }
}

/**
 * Time Adds Interval.
 */
case class TimeAdd(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def toString: String = s"$left + $right"
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), IntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[Interval]
    left.dataType match {
      case DateType =>
        DateTimeUtils.dateAddFullInterval(
          start.asInstanceOf[Int], itvl.months, itvl.microseconds)
      case TimestampType =>
        DateTimeUtils.timestampAddFullInterval(
          start.asInstanceOf[Long], itvl.months, itvl.microseconds)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    left.dataType match {
      case DateType =>
        defineCodeGen(ctx, ev, (sd, i) => {
          s"""$dtu.dateAddFullInterval($sd, $i.months, $i.microseconds)"""
        })
      case TimestampType => // TimestampType
        defineCodeGen(ctx, ev, (sd, i) => {
          s"""$dtu.timestampAddFullInterval($sd, $i.months, $i.microseconds)"""
        })
    }
  }
}

/**
 * Time Subtracts Interval.
 */
case class TimeSub(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def toString: String = s"$left - $right"
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), IntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[Interval]
    left.dataType match {
      case DateType =>
        DateTimeUtils.dateAddFullInterval(
          start.asInstanceOf[Int], 0 - itvl.months, 0 - itvl.microseconds)
      case TimestampType =>
        DateTimeUtils.timestampAddFullInterval(
          start.asInstanceOf[Long], 0 - itvl.months, 0 - itvl.microseconds)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    left.dataType match {
      case DateType =>
        defineCodeGen(ctx, ev, (sd, i) => {
          s"""$dtu.dateAddFullInterval($sd, 0 - $i.months, 0 - $i.microseconds)"""
        })
      case TimestampType => // TimestampType
        defineCodeGen(ctx, ev, (sd, i) => {
          s"""$dtu.timestampAddFullInterval($sd, 0 - $i.months, 0 - $i.microseconds)"""
        })
    }
  }
}

/**
 * Returns the date that is num_months after start_date.
 */
case class AddMonths(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddYearMonthInterval(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.dateAddYearMonthInterval($sd, $m)"""
    })
  }
}

/**
 * Returns number of months between dates date1 and date2.
 */
case class MonthsBetween(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(t1: Any, t2: Any): Any = {
    DateTimeUtils.monthsBetween(t1.asInstanceOf[Long], t2.asInstanceOf[Long])
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (l, r) => {
      s"""$dtu.monthsBetween($l, $r)"""
    })
  }
}

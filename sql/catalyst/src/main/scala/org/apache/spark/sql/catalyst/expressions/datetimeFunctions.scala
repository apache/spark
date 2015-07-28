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

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

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

  @transient private lazy val c = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c.setMinimalDaysInFirstWeek(4)
    c
  }

  override protected def nullSafeEval(date: Any): Any = {
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.get(Calendar.WEEK_OF_YEAR)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (time) => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      ctx.addMutableState(cal, c,
        s"""
          $c = $cal.getInstance(java.util.TimeZone.getTimeZone("UTC"));
          $c.setFirstDayOfWeek($cal.MONDAY);
          $c.setMinimalDaysInFirstWeek(4);
         """)
      s"""
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
    UTF8String.fromString(sdf.format(new java.util.Date(timestamp.asInstanceOf[Long] / 1000)))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (timestamp, format) => {
      s"""UTF8String.fromString((new $sdf($format.toString()))
          .format(new java.sql.Timestamp($timestamp / 1000)))"""
    })
  }
}

/**
 * Convert time string with given pattern
 * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
 * to Unix time stamp (in seconds), return null if fail.
 * If the second parameter is missing, use "yyyy-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
case class UnixTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType

  override def nullSafeEval(time: Any, format: Any): Any = {
    left.dataType match {
      case DateType =>
        DateTimeUtils.daysToMillis(time.asInstanceOf[Int]) / 1000L
      case TimestampType =>
        time.asInstanceOf[Long] / 1000000L
      case StringType =>
        val formatString = format.asInstanceOf[UTF8String].toString
        val sdf = new SimpleDateFormat(formatString)
        Try(sdf.parse(time.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    left.dataType match {
      case StringType =>
        val sdf = classOf[SimpleDateFormat].getName
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.primitive} =
                (new $sdf($format.toString())).parse($string.toString()).getTime() / 1000L;
            } catch (java.lang.Throwable e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        defineCodeGen(ctx, ev, (timestamp, format) => {
          s"""$timestamp / 1000000L"""
        })
      case DateType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        defineCodeGen(ctx, ev, (date, format) => {
          s"""$dtu.daysToMillis($date) / 1000L"""
        })
    }
  }
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 */
case class FromUnixTime(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(unix: Expression) = {
    this(unix, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override protected def nullSafeEval(time: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString)
    UTF8String.fromString(sdf.format(new java.util.Date(time.asInstanceOf[Long] * 1000L)))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (seconds, format) => {
      s"""UTF8String.fromString((new $sdf($format.toString())).format(
        new java.sql.Timestamp($seconds * 1000L)))""".stripMargin
    })
  }

}

/**
 * Returns the last day of the month which the date belongs to.
 */
case class LastDay(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def prettyName: String = "last_day"

  override def nullSafeEval(date: Any): Any = {
    val days = date.asInstanceOf[Int]
    DateTimeUtils.getLastDayOfMonth(days)
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd) => {
      s"$dtu.getLastDayOfMonth($sd)"
    })
  }
}

/**
 * Returns the first date which is later than startDate and named as dayOfWeek.
 * For example, NextDay(2015-07-27, Sunday) would return 2015-08-02, which is the first
 * sunday later than 2015-07-27.
 */
case class NextDay(startDate: Expression, dayOfWeek: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = dayOfWeek

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, dayOfW: Any): Any = {
    val dow = DateTimeUtils.getDayOfWeekFromString(dayOfW.asInstanceOf[UTF8String])
    if (dow == -1) {
      null
    } else {
      val sd = start.asInstanceOf[Int]
      DateTimeUtils.getNextDateForDayOfWeek(sd, dow)
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (sd, dowS) => {
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
      val dow = ctx.freshName("dow")
      val genDow = if (right.foldable) {
        val dowVal = DateTimeUtils.getDayOfWeekFromString(
          dayOfWeek.eval(InternalRow.empty).asInstanceOf[UTF8String])
        s"int $dow = $dowVal;"
      } else {
        s"int $dow = $dtu.getDayOfWeekFromString($dowS);"
      }
      genDow + s"""
        if ($dow == -1) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = $dtu.getNextDateForDayOfWeek($sd, $dow);
        }
       """
    })
  }
}

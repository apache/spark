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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
case class CurrentDate() extends LeafExpression {
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
case class CurrentTimestamp() extends LeafExpression {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }
}

/**
 * Abstract class for create time format expressions.
 */
abstract class TimeFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  protected val factorToMilli: Int

  protected val cntPerInterval: Int

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    val time = timestamp.asInstanceOf[Long] / 1000
    val longTime: Long = time + TimeZone.getDefault.getOffset(time)
    ((longTime / factorToMilli) % cntPerInterval).toInt
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val tz = classOf[TimeZone].getName
    defineCodeGen(ctx, ev, (c) =>
      s"""(${ctx.javaType(dataType)})
            ((($c / 1000) + $tz.getDefault().getOffset($c / 1000))
                / $factorToMilli % $cntPerInterval)"""
    )
  }
}

case class Hour(child: Expression) extends TimeFormatExpression {

  override protected val factorToMilli: Int = 1000 * 3600

  override protected val cntPerInterval: Int = 24
}

case class Minute(child: Expression) extends TimeFormatExpression {

  override protected val factorToMilli: Int = 1000 * 60

  override protected val cntPerInterval: Int = 60
}

case class Second(child: Expression) extends TimeFormatExpression {

  override protected val factorToMilli: Int = 1000

  override protected val cntPerInterval: Int = 60
}

abstract class DateFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  protected def isLeapYear(year: Int): Boolean = {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
  }

  def eval(input: InternalRow, f: (Int, Int, Long) => Int): Any = {
    val valueLeft = child.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val longTime: Long = valueLeft.asInstanceOf[Long] / 1000
      val days = longTime / 1000.0 / 3600.0 / 24.0
      val year = days / 365.24
      val dayInYear = days - year.toInt * 365.24
      f(dayInYear.toInt, 1970 + year.toInt, longTime)
    }
  }

  protected def defineCodeGen(
     ctx: CodeGenContext,
     ev: GeneratedExpressionCode,
     f: (String, String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (date) => {
      val longTime = ctx.freshName("longTime")
      val dayInYear = ctx.freshName("dayInYear")
      val days = ctx.freshName("days")
      val year = ctx.freshName("year")

      s"""
        long $longTime = $date / 1000;
        long $days = $longTime / 1000 / 3600 / 24;
        int $year = (int) ($days / 365.24);
        int $dayInYear = (int) ($days - $year * 365.24);
        $year += 1970;
        ${f(dayInYear, year, longTime)}
      """
    })
  }
}


case class Year(child: Expression) extends DateFormatExpression {

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, year, longTime) =>
      if (dayInYear > 3 && dayInYear < 363) {
        year
      } else {
        val c = Calendar.getInstance()
        c.setTimeInMillis(longTime)
        c.get(Calendar.YEAR)
      }
    )
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, year, longTime) =>
    s"""
       if ($day > 1 && $day < 360) {
         ${ev.primitive} = $year;
       } else {
         $cal c = $cal.getInstance();
         c.setTimeInMillis($longTime);
         ${ev.primitive} = c.get($cal.YEAR);
       }
     """)
  }
}

case class Quarter(child: Expression) extends DateFormatExpression {

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, year, longTime) => {
      val leap = if (isLeapYear(year)) 1 else 0
      dayInYear match {
        case i: Int if i > 3 && i < 88 + leap => 1
        case i: Int if i > 93 + leap && i < 179 + leap => 2
        case i: Int if i > 184 + leap && i < 271 + leap => 3
        case i: Int if i > 276 + leap && i < 363 + leap => 4
        case _ =>
          val c = Calendar.getInstance()
          c.setTimeInMillis(longTime)
          c.get(Calendar.MONTH) / 3 + 1
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, year, longTime) => {
      val leap = ctx.freshName("leap")
      s"""
         int $leap = ($year % 4 == 0 && ($year % 100 != 0 || $year % 400 == 0)) ? 1 : 0;
         if ($day > 3 && $day < 88 + $leap) {
           ${ev.primitive} = 1;
         } else if ($day > 93 + $leap && $day < 179 + $leap) {
           ${ev.primitive} = 2;
         } else if ($day > 184 + $leap && $day < 271 + $leap) {
           ${ev.primitive} = 3;
         } else if ($day > 276 + $leap && $day < 363 + $leap) {
           ${ev.primitive} = 4;
         } else {
           $cal c = $cal.getInstance();
           c.setTimeInMillis($longTime);
           ${ev.primitive} = c.get($cal.MONTH) / 3 + 1;
         }
       """})
  }
}

case class Month(child: Expression) extends DateFormatExpression {

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, year, longTime) => {
      val leap = if (isLeapYear(year)) 1 else 0
      dayInYear match {
        case i: Int if i > 3 && i < 29 => 1
        case i: Int if i > 34 && i < 57 + leap => 2
        case i: Int if i > 62 + leap && i < 88 + leap => 3
        case i: Int if i > 93 + leap && i < 118 + leap => 4
        case i: Int if i > 123 + leap && i < 149 + leap => 5
        case i: Int if i > 154 + leap && i < 179 + leap => 6
        case i: Int if i > 184 + leap && i < 210 + leap => 7
        case i: Int if i > 215 + leap && i < 241 + leap => 8
        case i: Int if i > 246 + leap && i < 271 + leap => 9
        case i: Int if i > 276 + leap && i < 302 + leap => 10
        case i: Int if i > 307 + leap && i < 332 + leap => 11
        case i: Int if i > 337 + leap && i < 363 + leap => 12
        case _ =>
          val c = Calendar.getInstance()
          c.setTimeInMillis(longTime)
          c.get(Calendar.MONTH) + 1
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, year, longTime) => {
      val leap = ctx.freshName("leap")
      s"""
         int $leap = ($year % 4 == 0 && ($year % 100 != 0 || $year % 400 == 0)) ? 1 : 0;
         if ($day > 3 && $day < 29) {
           ${ev.primitive} = 1;
         } else if($day > 34 && $day < 57 + $leap) {
            ${ev.primitive} = 2;
         } else if($day > 62 + $leap + $leap && $day < 88 + $leap) {
            ${ev.primitive} = 3;
         } else if($day > 93 + $leap && $day < 118 + $leap) {
            ${ev.primitive} = 4;
         } else if($day > 123 + $leap && $day < 149 + $leap) {
            ${ev.primitive} = 5;
         } else if($day > 154 + $leap && $day < 179 + $leap) {
            ${ev.primitive} = 6;
         } else if($day > 184 + $leap && $day < 210 + $leap) {
            ${ev.primitive} = 7;
         } else if($day > 215 + $leap && $day < 241 + $leap) {
            ${ev.primitive} = 8;
         } else if($day > 246 + $leap && $day < 271 + $leap) {
            ${ev.primitive} = 9;
         } else if($day > 276 + $leap && $day < 302 + $leap) {
            ${ev.primitive} = 10;
         } else if($day > 307 + $leap && $day < 332 + $leap) {
            ${ev.primitive} = 11;
         } else if($day > 337 + $leap && $day < 363 + $leap) {
            ${ev.primitive} = 12;
         } else {
           $cal c = $cal.getInstance();
           c.setTimeInMillis($longTime);
           ${ev.primitive} = c.get($cal.MONTH) + 1;
         }
       """})
  }
}

case class Day(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (date) => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      s"""
        $cal $c = $cal.getInstance();
        $c.setTimeInMillis($date / 1000);
        ${ev.primitive} = $c.get($cal.DAY_OF_MONTH);
      """
    })
  }

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val c = Calendar.getInstance()
      c.setTimeInMillis(child.eval(input).asInstanceOf[Long] / 1000)
      c.get(Calendar.DAY_OF_MONTH)
    }
  }
}

case class WeekOfYear(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    nullSafeCodeGen(ctx, ev, (time) => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      s"""
        $cal $c = $cal.getInstance();
        $c.setTimeInMillis($time / 1000);
        ${ev.primitive} = $c.get($cal.WEEK_OF_YEAR);
      """
    })

  override protected def nullSafeEval(input: Any): Any = {
    val c = Calendar.getInstance()
    c.setTimeInMillis(input.asInstanceOf[Long] / 1000)
    c.get(Calendar.WEEK_OF_YEAR)
  }
}

case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override protected def nullSafeEval(date: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString)
    UTF8String.fromString(sdf.format(new Date(date.asInstanceOf[Long] / 1000)))
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (date, format) => {
      s"""${ctx.stringType}.fromString((new $sdf($format.toString()))
          .format(new java.sql.Date($date / 1000)))"""
    })
  }
}

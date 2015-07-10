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

case class Second(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(time: Any): Any = {
    (time.asInstanceOf[Long] / 1000L / 1000L % 60L).toInt
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (time) => {
      s"""${ev.primitive} = (int) ($time / 1000L / 1000L % 60L);"""
    })
  }
}

private[sql] object DateFormatExpression {

  def isLeapYear(year: Int): Boolean = {
    (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0)
  }

  val yearBoundaries = new Array[Int](2330 - 1599)

  var i = 0
  yearBoundaries(i) = -135140
  (1601 to 2330) foreach { year =>
    i = i + 1
    if (isLeapYear(year - 1)) {
      yearBoundaries(i) = yearBoundaries(i - 1) + 366
    } else {
      yearBoundaries(i) = yearBoundaries(i - 1) + 365
    }
  }
}

abstract class DateFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  protected def calculateYearAndDayInYear(daysIn: Int): (Int, Int) = {
    var index: Int = 370 + (daysIn / 365.24).toInt - 1
    while (DateFormatExpression.yearBoundaries(index) < daysIn + 1) {
      index += 1
    }
    (index - 1  + 1600, daysIn - DateFormatExpression.yearBoundaries(index - 1) + 1)
  }

  protected def codeGen(ctx: CodeGenContext, ev: GeneratedExpressionCode, input: String,
      f: (String, String) => String): String = {
    val yb = ctx.freshName("yb")
    val i = ctx.freshName("counter")
    val x = ctx.freshName("counter")
    val index = ctx.freshName("index")
    val year = ctx.freshName("year")
    val dayInYear = ctx.freshName("dayInYear")

    s"""
       int[] $yb = new int[2330 - 1599];
       $yb[0] = -135140;
       int $x = 1;
       for(int $i = 1601; $i <= 2330; $i++, $x++) {
         $yb[$x] = (($i - 1) % 4 == 0 && (($i - 1) % 100 != 0 || ($i - 1) % 400 == 0)) ?
                          $yb[$x - 1] + 366 : $yb[$x - 1] + 365;
       }
       int $index = 370 + ((int) ($input / 365.24)) - 1;
       while ($yb[$index] < $input + 1) {
         $index++;
       }
       int $year = $index - 1 + 1600;
       int $dayInYear = $input - $yb[$index - 1] + 1;
       ${f(year, dayInYear)}
     """
  }
}


case class Year(child: Expression) extends DateFormatExpression {

  override protected def nullSafeEval(input: Any): Any = {
    calculateYearAndDayInYear(input.asInstanceOf[Int])._1
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, days => {
      codeGen(ctx, ev, days, (year, dayInYear) => {
        s"""${ev.primitive} = $year;"""
      })
    })
  }
}

case class Quarter(child: Expression) extends DateFormatExpression {

  override protected def nullSafeEval(input: Any): Any = {
    val (year, dayInYear) = calculateYearAndDayInYear(input.asInstanceOf[Int])
      val leap = if (DateFormatExpression.isLeapYear(year)) 1 else 0
      dayInYear match {
        case i: Int if i <= 90 + leap => 1
        case i: Int if i <= 181 + leap => 2
        case i: Int if i <= 273 + leap => 3
        case _ => 4
      }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, days => {
      codeGen(ctx, ev, days, (year, dayInYear) => {
        s"""
            if ($dayInYear <= 90) {
              ${ev.primitive} = 1;
            } else if ($dayInYear <= 181) {
              ${ev.primitive} = 2;
            } else if ($dayInYear <= 273) {
              ${ev.primitive} = 3;
            } else {
              ${ev.primitive} = 4;
            }
         """
      })
    })
  }
}

case class Month(child: Expression) extends DateFormatExpression {

  override protected def nullSafeEval(input: Any): Any = {
    val (year, dayInYear) = calculateYearAndDayInYear(input.asInstanceOf[Int])
    val leap = if (DateFormatExpression.isLeapYear(year)) 1 else 0
    dayInYear match {
      case i: Int if i <= 31 => 1
      case i: Int if i <= 59 + leap => 2
      case i: Int if i <= 90 + leap => 3
      case i: Int if i <= 120 + leap => 4
      case i: Int if i <= 151 + leap => 5
      case i: Int if i <= 181 + leap => 6
      case i: Int if i <= 212 + leap => 7
      case i: Int if i <= 243 + leap => 8
      case i: Int if i <= 273 + leap => 9
      case i: Int if i <= 304 + leap => 10
      case i: Int if i <= 334 + leap => 11
      case _ => 12
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, days => {
      codeGen(ctx, ev, days, (year, dayInYear) => {
        val leap = ctx.freshName("leap")
        s"""
            int $leap = ($year % 4) == 0 && (($year % 100) != 0 || ($year % 400) == 0) ? 1 : 0;
            if ($dayInYear <= 31) {
              ${ev.primitive} = 1;
            } else if ($dayInYear <= 59 + $leap) {
              ${ev.primitive} = 2;
            } else if ($dayInYear <= 90 + $leap) {
              ${ev.primitive} = 3;
            } else if ($dayInYear <= 120 + $leap) {
              ${ev.primitive} = 4;
            } else if ($dayInYear <= 151 + $leap) {
              ${ev.primitive} = 5;
            } else if ($dayInYear <= 181 + $leap) {
              ${ev.primitive} = 6;
            } else if ($dayInYear <= 212 + $leap) {
              ${ev.primitive} = 7;
            } else if ($dayInYear <= 243 + $leap) {
              ${ev.primitive} = 8;
            } else if ($dayInYear <= 273 + $leap) {
              ${ev.primitive} = 9;
            } else if ($dayInYear <= 304 + $leap) {
              ${ev.primitive} = 10;
            } else if ($dayInYear <= 334 + $leap) {
              ${ev.primitive} = 11;
            } else {
              ${ev.primitive} = 12;
            }
         """
      })
    })
  }
}

case class Day(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override protected def nullSafeEval(input: Any): Any = {
    val (year, dayInYear) = calculateYearAndDayInYear(input.asInstanceOf[Int])
    val leap = if (DateFormatExpression.isLeapYear(year)) 1 else 0
    dayInYear match {
      case i: Int if i <= 31 => i
      case i: Int if i <= 59 + leap => i - 31
      case i: Int if i <= 90 + leap => i - 59 + leap
      case i: Int if i <= 120 + leap => i - 90 + leap
      case i: Int if i <= 151 + leap => i - 120 + leap
      case i: Int if i <= 181 + leap => i - 151 + leap
      case i: Int if i <= 212 + leap => i - 181 + leap
      case i: Int if i <= 243 + leap => i - 212 + leap
      case i: Int if i <= 273 + leap => i - 243 + leap
      case i: Int if i <= 304 + leap => i - 273 + leap
      case i: Int if i <= 334 + leap => i - 304 + leap
      case i: Int => i - 334 + leap
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, days => {
      codeGen(ctx, ev, days, (year, dayInYear) => {
        val leap = ctx.freshName("leap")
        s"""
            int $leap = ($year % 4) == 0 && (($year % 100) != 0 || ($year % 400) == 0) ? 1 : 0;
            if ($dayInYear <= 31) {
              ${ev.primitive} = $dayInYear;
            } else if ($dayInYear <= 59 + $leap) {
              ${ev.primitive} = $dayInYear - 31;
            } else if ($dayInYear <= 90 + $leap) {
              ${ev.primitive} = $dayInYear - 59 + $leap;
            } else if ($dayInYear <= 120 + $leap) {
              ${ev.primitive} = $dayInYear - 90 + $leap;
            } else if ($dayInYear <= 151 + $leap) {
              ${ev.primitive} = $dayInYear - 120 + $leap;
            } else if ($dayInYear <= 181 + $leap) {
              ${ev.primitive} = $dayInYear - 151 + $leap;
            } else if ($dayInYear <= 212 + $leap) {
              ${ev.primitive} = $dayInYear - 181 + $leap;
            } else if ($dayInYear <= 243 + $leap) {
              ${ev.primitive} = $dayInYear - 212 + $leap;
            } else if ($dayInYear <= 273 + $leap) {
              ${ev.primitive} = $dayInYear - 243 + $leap;
            } else if ($dayInYear <= 304 + $leap) {
              ${ev.primitive} = $dayInYear - 273 + $leap;
            } else if ($dayInYear <= 334 + $leap) {
              ${ev.primitive} = $dayInYear - 304 + $leap;
            } else {
              ${ev.primitive} = $dayInYear - 334 + $leap;
            }
         """
      })
    })
  }

}

case class WeekOfYear(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    nullSafeCodeGen(ctx, ev, (time) => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      s"""
        $cal $c = $cal.getInstance();
        $c.setTimeInMillis($time * 1000L * 3600L * 24L);
        ${ev.primitive} = $c.get($cal.WEEK_OF_YEAR);
      """
    })

  override protected def nullSafeEval(input: Any): Any = {
    val c = Calendar.getInstance()
    c.setTimeInMillis(input.asInstanceOf[Int] * 1000L * 3600L * 24L)
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

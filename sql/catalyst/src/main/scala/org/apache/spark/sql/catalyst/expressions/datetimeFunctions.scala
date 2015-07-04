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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
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
 */
case class CurrentTimestamp() extends LeafExpression {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 10000L
  }
}

abstract class TimeFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  protected val format: Int

  protected val cntPerInterval: Int

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def eval(input: InternalRow): Any = {
    val valueLeft = child.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val time = valueLeft.asInstanceOf[Long] / 10000
      val utcTime: Long = time + TimeZone.getDefault.getOffset(time)
      ((utcTime / format) % cntPerInterval).toInt
    }
  }

  override def genCode(
                        ctx: CodeGenContext,
                        ev: GeneratedExpressionCode): String = {

    val tz = classOf[TimeZone].getName

    defineCodeGen(ctx, ev, (c) =>
      s"""(${ctx.javaType(dataType)})
            ((($c / 10000) + $tz.getDefault().getOffset($c / 10000)) / $format % $cntPerInterval)"""
    )
  }
}

case class Hour(child: Expression) extends TimeFormatExpression {

  override protected val format: Int = 1000 * 3600

  override protected val cntPerInterval: Int = 24

  override def dataType: DataType = IntegerType

  override def toString: String = s"Hour($child)"
}

case class Minute(child: Expression) extends TimeFormatExpression {

  override protected val format: Int = 1000 * 60

  override protected val cntPerInterval: Int = 60

  override def dataType: DataType = IntegerType

  override def toString: String = s"Minute($child)"
}

case class Second(child: Expression) extends TimeFormatExpression {

  override protected val format: Int = 1000

  override protected val cntPerInterval: Int = 60

  override def dataType: DataType = IntegerType

  override def toString: String = s"Second($child)"
}

abstract class DateFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  protected def defineCodeGen(
                               ctx: CodeGenContext,
                               ev: GeneratedExpressionCode,
                               f: (String, String) => String): String = {

    val tz = classOf[TimeZone].getName

    val utcTime = ctx.freshName("utcTime")
    val dayInYear = ctx.freshName("dayInYear")
    val days = ctx.freshName("days")
    val year = ctx.freshName("year")

    val eval = child.gen(ctx)
    ev.isNull = eval.isNull
    eval.code + s"""
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        long $utcTime = ${eval.primitive} / 10000;
        long $days = $utcTime / 1000 / 3600 / 24;
        int $year = (int) ($days / 365.24);
        int $dayInYear = (int) ($days - $year * 365.24);
        ${f(dayInYear, utcTime)}
      }
    """
  }

  def eval(input: InternalRow, f: (Int, Long) => Int): Any = {
    val valueLeft = child.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val utcTime: Long = valueLeft.asInstanceOf[Long] / 10000
      val days = utcTime / 1000 / 3600 / 24
      val year = days / 365.24
      val dayInYear = days - year.toInt * 365.24
      f(dayInYear.toInt, utcTime)
    }
  }

  override def toString: String = s"Year($child)"
}


case class Year(child: Expression) extends DateFormatExpression {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, utc) =>
      s"""
         if ($day > 1 && $day < 360) {
           ${ev.primitive} = (int) (1970 + ($utc / 1000 / 3600 / 24 / 365.24));
         } else {
           $cal c = $cal.getInstance();
           c.setTimeInMillis($utc);
           ${ev.primitive} = c.get($cal.YEAR);
         }
       """)
  }

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, utcTime) =>
      if (dayInYear > 1 && dayInYear < 360) {
        1970 + (utcTime / 1000 / 3600 / 24 / 365.24).toInt
      } else {
        val c = Calendar.getInstance()
        c.setTimeInMillis(utcTime)
        c.get(Calendar.YEAR)
      }
    )
  }

  override def toString: String = s"Year($child)"
}

case class Quarter(child: Expression) extends DateFormatExpression {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, utc) =>
      s"""
         if ($day > 1 && $day < 90) {
           ${ev.primitive} = 1;
         } else if ($day > 92 && $day < 181) {
           ${ev.primitive} = 2;
         } else if ($day > 183 && $day < 273) {
           ${ev.primitive} = 3;
         } else if ($day > 275 && $day < 364) {
           ${ev.primitive} = 4;
         } else {
           $cal c = $cal.getInstance();
           c.setTimeInMillis($utc);
           ${ev.primitive} = c.get($cal.MONTH) / 3 + 1;
         }
       """)
  }

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, utcTime) =>
      dayInYear match {
        case i: Int if i > 1 && i < 89 => 1
        case i: Int if i > 93 && i < 180 => 2
        case i: Int if i > 184 && i < 272 => 3
        case i: Int if i > 276 && i < 362 => 4
        case _ =>
          val c = Calendar.getInstance()
          c.setTimeInMillis(utcTime)
          c.get(Calendar.MONTH) / 3 + 1
      }
    )
  }

  override def toString: String = s"Quarter($child)"
}

case class Month(child: Expression) extends DateFormatExpression {

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    val cal = classOf[Calendar].getName
    defineCodeGen(ctx, ev, (day, utc) =>
      s"""
         if ($day > 1 && $day < 30) {
           ${ev.primitive} = 1;
         } else if($day > 33 && $day < 58) {
            ${ev.primitive} = 2;
         } else if($day > 62 && $day < 89) {
            ${ev.primitive} = 3;
         } else if($day > 93 && $day < 119) {
            ${ev.primitive} = 4;
         } else if($day > 123 && $day < 150) {
            ${ev.primitive} = 5;
         } else if($day > 154 && $day < 180) {
            ${ev.primitive} = 6;
         } else if($day > 184 && $day < 211) {
            ${ev.primitive} = 7;
         } else if($day > 215 && $day < 242) {
            ${ev.primitive} = 8;
         } else if($day > 246 && $day < 272) {
            ${ev.primitive} = 9;
         } else if($day > 276 && $day < 303) {
            ${ev.primitive} = 10;
         } else if($day > 307 && $day < 333) {
            ${ev.primitive} = 11;
         } else if($day > 337 && $day < 362) {
            ${ev.primitive} = 12;
         } else {
           $cal c = $cal.getInstance();
           c.setTimeInMillis($utc);
           ${ev.primitive} = c.get($cal.MONTH) + 1;
         }
       """)
  }

  override def eval(input: InternalRow): Any = {
    eval(input, (dayInYear, utcTime) =>
      dayInYear match {
        case i: Int if i > 1 && i < 30 => 1
        case i: Int if i > 33 && i < 58 => 2
        case i: Int if i > 62 && i < 89 => 3
        case i: Int if i > 93 && i < 119 => 4
        case i: Int if i > 123 && i < 150 => 5
        case i: Int if i > 154 && i < 180 => 6
        case i: Int if i > 184 && i < 211 => 7
        case i: Int if i > 215 && i < 242 => 8
        case i: Int if i > 246 && i < 272 => 9
        case i: Int if i > 276 && i < 303 => 10
        case i: Int if i > 307 && i < 333 => 11
        case i: Int if i > 337 && i < 362 => 12
        case _ =>
          val c = Calendar.getInstance()
          c.setTimeInMillis(utcTime)
          c.get(Calendar.MONTH) + 1
      }
    )
  }

  override def toString: String = s"Month($child)"
}

case class Day(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)

    val cal = classOf[Calendar].getName
    val c = ctx.freshName("cal")

    ev.isNull = eval.isNull
    eval.code + s"""
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $cal $c = $cal.getInstance();
        $c.setTimeInMillis(${eval.primitive} / 10000);
        ${ev.primitive} = $c.get($cal.DAY_OF_MONTH);
      }
    """
  }

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val c = Calendar.getInstance()
      c.setTimeInMillis(child.eval(input).asInstanceOf[Long] / 10000)
      c.get(Calendar.DAY_OF_MONTH)
    }
  }

  override def toString: String = s"Day($child)"
}

case class WeekOfYear(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)

    val cal = classOf[Calendar].getName
    val c = ctx.freshName("cal")

    ev.isNull = eval.isNull
    eval.code + s"""
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $cal $c = $cal.getInstance();
        $c.setTimeInMillis(${eval.primitive} / 10000);
        ${ev.primitive} = $c.get($cal.WEEK_OF_YEAR);
      }
    """
  }

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val c = Calendar.getInstance()
      c.setTimeInMillis(child.eval(input).asInstanceOf[Long] / 10000)
      c.get(Calendar.WEEK_OF_YEAR)
    }
  }

  override def toString: String = s"WeekOfYear($child)"
}


case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def toString: String = s"DateFormat($left, $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def eval(input: InternalRow): Any = {
    val valueLeft = left.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val valueRight = right.eval(input)
      if (valueRight == null) {
        null
      } else {
        val sdf = new SimpleDateFormat(valueRight.toString)
        UTF8String.fromString(sdf.format(new Date(valueLeft.asInstanceOf[Long] / 10000)))
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (x, y) => {
      s"""${ctx.stringType}.fromString((new $sdf($y.toString()))
          .format(new java.sql.Date($x / 10000)))"""
    })
  }
}

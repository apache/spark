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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class DateFormatExpression extends Expression { self: Product =>

  protected val format: String

  protected val caller: String
  
  protected val date: Expression

  override def foldable: Boolean = date.foldable

  override def nullable: Boolean = true

  override def children: Seq[Expression] = Seq(date)

  override def eval(input: InternalRow): Any = {
    val valueLeft = date.eval(input)
    if (valueLeft == null) {
      null
    } else {
      if (format == null) {
        null
      } else {
        val sdf = new SimpleDateFormat(format)
        date.dataType match {
          case TimestampType =>
            UTF8String.fromString(sdf.format(new Date(valueLeft.asInstanceOf[Long] / 10000)))
          case DateType =>
            UTF8String.fromString(sdf.format(DateTimeUtils.toJavaDate(valueLeft.asInstanceOf[Int])))
          case StringType =>
            UTF8String.fromString(
              sdf.format(DateTimeUtils.stringToTime(valueLeft.toString)))
        }
      }
    }
  }

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"$caller accepts date types as argument, " +
          s" not ${date.dataType}")
    }


  /**
   * Called by date format expressions to generate a code block that returns the result
   *
   * As an example, the following parse the result to int
   * {{{
   *   defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to parse an
   *          [[UTF8String]] to the expected output type
   */

  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {

    val sdf = classOf[SimpleDateFormat].getName
    val dtUtils = "org.apache.spark.sql.catalyst.util.DateTimeUtils"

    val eval1 = date.gen(ctx)

    val parseInput = date.dataType match {
      case StringType => s"new java.sql.Date($dtUtils.stringToTime(${eval1.primitive}.toString()).getTime())"
      case TimestampType => s"new java.sql.Date(${eval1.primitive} / 10000)"
      case DateType => s"$dtUtils.toJavaDate(${eval1.primitive})"
    }

    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $sdf sdf = new $sdf("$format");
        ${ctx.stringType} s = ${ctx.stringType}.fromString(sdf.format($parseInput));
        ${ev.primitive} = ${f("s")};
      } else {
        ${ev.isNull} = true;
      }
    """
  }
  
}

case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression {

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
      case (null, _) => TypeCheckResult.TypeCheckSuccess
      case (_, null) => TypeCheckResult.TypeCheckSuccess
      case (_: DateType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case (_: TimestampType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case (_: StringType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"DateFormat accepts date types as first argument, " +
          s"and string types as second, not ${left.dataType} and ${right.dataType}")
    }

  override def toString: String = s"DateFormat($left, $right)"

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
        left.dataType match {
          case TimestampType =>
            UTF8String.fromString(sdf.format(new Date(valueLeft.asInstanceOf[Long] / 10000)))
          case DateType =>
            UTF8String.fromString(sdf.format(DateTimeUtils.toJavaDate(valueLeft.asInstanceOf[Int])))
          case StringType =>
            UTF8String.fromString(
              sdf.format(DateTimeUtils.stringToTime(valueLeft.toString)))
        }
      }
    }   
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val sdf = classOf[SimpleDateFormat].getName
    val dtUtils = "org.apache.spark.sql.catalyst.util.DateTimeUtils"

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)

    val parseInput = left.dataType match {
      case StringType => s"new java.sql.Date($dtUtils.stringToTime(${eval1.primitive}.toString()).getTime())"
      case TimestampType => s"new java.sql.Date(${eval1.primitive} / 10000)"
      case DateType => s"$dtUtils.toJavaDate(${eval1.primitive})"
    }

    s"""
      ${eval1.code}
      ${eval2.code}
      boolean ${ev.isNull} = ${eval1.isNull} || ${eval2.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $sdf sdf = new $sdf(${eval2.primitive}.toString());
        ${ev.primitive} = ${ctx.stringType}.fromString(sdf.format($parseInput));
      } else {
        ${ev.isNull} = true;
      }
    """
  }
}

case class Year(date: Expression) extends DateFormatExpression {

  override protected val format: String = "y"

  override protected val caller: String = "Year"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class Quarter(date: Expression) extends DateFormatExpression {

  override protected val format: String = "M"

  override protected val caller: String = "Quarter"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"(Integer.parseInt($c.toString()) - 1) / 3 + 1")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => (s.toString.toInt - 1) / 3 + 1
    }
  }
}

case class Month(date: Expression) extends DateFormatExpression {

  override protected val format: String = "M"

  override protected val caller: String = "Month"

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class Day(date: Expression) extends DateFormatExpression {

  override protected val format: String = "d"

  override protected val caller: String = "Day"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class Hour(date: Expression) extends DateFormatExpression {

  override protected val format: String = "H"

  override protected val caller: String = "Hour"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class Minute(date: Expression) extends DateFormatExpression {

  override protected val format: String = "m"

  override protected val caller: String = "Minute"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class Second(date: Expression) extends DateFormatExpression {

  override protected val format: String = "s"

  override protected val caller: String = "Second"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}

case class WeekOfYear(date: Expression) extends DateFormatExpression {

  override protected val format: String = "w"

  override protected val caller: String = "WeekOfYear"

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"Integer.parseInt($c.toString())")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case s: UTF8String => s.toString.toInt
    }
  }
}
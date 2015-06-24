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
  
  protected val date: Expression
  
  protected val format: Expression

  override def foldable: Boolean = date.foldable && format.foldable

  override def nullable: Boolean = true

  override def children: Seq[Expression] = Seq(date, format)

  override def eval(input: InternalRow): Any = {
    val valueLeft = date.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val valueRight = format.eval(input)
      if (valueRight == null) {
        null
      } else {
        val sdf = new SimpleDateFormat(valueRight.asInstanceOf[UTF8String].toString)
        date.dataType match {
          case TimestampType =>
            UTF8String.fromString(sdf.format(new Date(valueLeft.asInstanceOf[Long] / 10000)))
          case DateType =>
            UTF8String.fromString(sdf.format(DateTimeUtils.toJavaDate(valueLeft.asInstanceOf[Int])))
          case StringType =>
            UTF8String.fromString(
              sdf.format(DateTimeUtils.stringToTime(valueLeft.asInstanceOf[UTF8String].toString)))
        }
      }
    }
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

    val sdf = "java.text.SimpleDateFormat"
    val utf8 = "org.apache.spark.unsafe.types.UTF8String"
    val dtUtils = "org.apache.spark.sql.catalyst.util.DateTimeUtils"

    val eval1 = date.gen(ctx)
    val eval2 = format.gen(ctx)

    val varName = ctx.freshName("resultVar")

    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          $sdf sdf = new $sdf(${eval2.primitive}.toString());
          Object o = ${eval1.primitive};
          $utf8 $varName;
          if (o instanceof ${ctx.boxedType(TimestampType)}) {
            $varName = $utf8.fromString(sdf.format(new java.sql.Date(Long.parseLong(o.toString()) / 10000)));
          } else if (o instanceof ${ctx.boxedType(DateType)}) {
            $varName = $utf8.fromString(sdf.format($dtUtils.toJavaDate(Integer.parseInt(o.toString()))));
          } else {
            $varName = $utf8.fromString(sdf.format(new java.sql.Date($dtUtils.stringToTime(o.toString()).getTime())));
          }
          ${ev.primitive} = ${f(varName)};
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
  
}

case class DateFormatClass(date: Expression, format: Expression) extends DateFormatExpression {

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult =
    (date.dataType, format.dataType) match {
      case (null, _) => TypeCheckResult.TypeCheckSuccess
      case (_, null) => TypeCheckResult.TypeCheckSuccess
      case (_: DateType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case (_: TimestampType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case (_: StringType, _: StringType) => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"DateFormat accepts date types as first argument, " +
          s"and string types as second, not ${date.dataType} and ${format.dataType}")
    }

  override def toString: String = s"DateFormat($date, $format)"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

}

case class Year(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("y")

  override def dataType: DataType = IntegerType

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Year accepts date types as argument, " +
          s" not ${date.dataType}")
    }

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
  
  override protected val format: Expression = Literal("M")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Quarter accepts date types as argument, " +
          s" not ${date.dataType}")
    }

}

case class Month(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("M")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Month accepts date types as argument, " +
          s" not ${date.dataType}")
    }
}

case class Day(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("d")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Day accepts date types as argument, " +
          s" not ${date.dataType}")
    }

}

case class Hour(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("H")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Hour accepts date types as argument, " +
          s" not ${date.dataType}")
    }
}

case class Minute(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("m")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Minute accepts date types as argument, " +
          s" not ${date.dataType}")
    }
}

case class Second(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("s")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"Second accepts date types as argument, " +
          s" not ${date.dataType}")
    }
}

case class WeekOfYear(date: Expression) extends DateFormatExpression {

  override protected val format: Expression = Literal("w")

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

  override def checkInputDataTypes(): TypeCheckResult =
    date.dataType match {
      case null => TypeCheckResult.TypeCheckSuccess
      case _: DateType => TypeCheckResult.TypeCheckSuccess
      case _: TimestampType => TypeCheckResult.TypeCheckSuccess
      case _: StringType => TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(s"WeekOfYear accepts date types as argument, " +
          s" not ${date.dataType}")
    }
}
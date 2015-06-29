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
import java.util.{Calendar, TimeZone, Locale}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class DateFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  private[this] val offset: Int = TimeZone.getDefault.getRawOffset

  protected val format: Int

  override def expectedChildTypes: Seq[DataType] = Seq(TimestampType)

  override def eval(input: InternalRow): Any = {
    val valueLeft = child.eval(input)
    if (valueLeft == null) {
      null
    } else {
      val utcTime: Long = valueLeft.asInstanceOf[Long] / 10000
      val c = Calendar.getInstance()
      c.setTimeInMillis(utcTime)
      c.get(format)
    }
  }

  override protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {

    val cal = classOf[Calendar].getName
    val cVar = ctx.freshName("cal")

    val eval = child.gen(ctx)
    // reuse the previous isNull
    ev.isNull = eval.isNull
    eval.code + s"""
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $cal $cVar = $cal.getInstance();
        $cVar.setTimeInMillis(${eval.primitive} / 10000);
        ${ev.primitive} = ${f(s"""$cVar.get($format)""")};
      }
    """
  }

}

case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
    with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def toString: String = s"DateFormat($left, $right)"

  override def expectedChildTypes: Seq[DataType] = Seq(TimestampType, StringType)

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

case class Year(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.YEAR

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input)
  }

  override def toString: String = s"Year($child)"
}

case class Quarter(child: Expression) extends DateFormatExpression {

  override protected val format: Int = Calendar.MONTH

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c / 3 + 1")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case i: Int => i / 3 + 1
    }
  }

  override def toString: String = s"Quarter($child)"
}

case class Month(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.MONTH

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c + 1)")
  }

  override def eval(input: InternalRow): Any = {
    super.eval(input) match {
      case null => null
      case i: Int => i + 1
    }
  }

  override def toString: String = s"Month($child)"
}

case class Day(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.DAY_OF_MONTH

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def toString: String = s"Day($child)"
}

case class Hour(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.HOUR_OF_DAY

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def toString: String = s"Hour($child)"
}

case class Minute(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.MINUTE

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def toString: String = s"Minute($child)"
}

case class Second(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.SECOND

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def toString: String = s"Second($child)"
}

case class WeekOfYear(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: Int = Calendar.WEEK_OF_YEAR

  override def dataType: DataType = IntegerType

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c")
  }

  override def toString: String = s"WeekOfYear($child)"
}

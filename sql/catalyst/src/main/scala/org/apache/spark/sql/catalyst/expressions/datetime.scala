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

abstract class DateFormatExpression extends UnaryExpression with ExpectsInputTypes {
  self: Product =>

  protected val format: String

  override def expectedChildTypes: Seq[DataType] = Seq(TimestampType)

  override def eval(input: InternalRow): Any = {
    val valueLeft = child.eval(input)
    if (valueLeft == null) {
      null
    } else {
      if (format == null) {
        null
      } else {
        val sdf = new SimpleDateFormat(format)
        UTF8String.fromString(sdf.format(new Date(valueLeft.asInstanceOf[Long] / 10000)))
      }
    }
  }

  override protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {

    val sdf = classOf[SimpleDateFormat].getName
    super.defineCodeGen(ctx, ev, (x) => {
      f(s"""${ctx.stringType}.fromString((new $sdf("$format")).format(new java.sql.Date($x / 10000)))""")
    })
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
      s"""${ctx.stringType}.fromString((new $sdf($y.toString())).format(new java.sql.Date($x / 10000)))"""
    })
  }
}

case class Year(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "y"

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

  override def toString: String = s"Year($child)"
}

case class Quarter(child: Expression) extends DateFormatExpression {

  override protected val format: String = "M"

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

  override def toString: String = s"Quarter($child)"
}

case class Month(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "M"

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

  override def toString: String = s"Month($child)"
}

case class Day(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "d"

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

  override def toString: String = s"Day($child)"
}

case class Hour(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "H"

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

  override def toString: String = s"Hour($child)"
}

case class Minute(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "m"

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

  override def toString: String = s"Minute($child)"
}

case class Second(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "s"

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

  override def toString: String = s"Second($child)"
}

case class WeekOfYear(child: Expression) extends DateFormatExpression with ExpectsInputTypes {

  override protected val format: String = "w"

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

  override def toString: String = s"WeekOfYear($child)"
}
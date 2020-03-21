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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, EmptyBlock, ExprCode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Return the unscaled Long value of a Decimal, assuming it fits in a Long.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 */
case class UnscaledValue(child: Expression) extends UnaryExpression {

  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toUnscaledLong

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c.toUnscaledLong()")
  }
}

/**
 * Create a Decimal from an unscaled Long value.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 */
case class MakeDecimal(
    child: Expression,
    precision: Int,
    scale: Int,
    nullOnOverflow: Boolean) extends UnaryExpression {

  def this(child: Expression, precision: Int, scale: Int) = {
    this(child, precision, scale, !SQLConf.get.ansiEnabled)
  }

  override def dataType: DataType = DecimalType(precision, scale)
  override def nullable: Boolean = child.nullable || nullOnOverflow
  override def toString: String = s"MakeDecimal($child,$precision,$scale)"

  protected override def nullSafeEval(input: Any): Any = {
    val longInput = input.asInstanceOf[Long]
    val result = new Decimal()
    if (nullOnOverflow) {
      result.setOrNull(longInput, precision, scale)
    } else {
      result.set(longInput, precision, scale)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      val setMethod = if (nullOnOverflow) {
        "setOrNull"
      } else {
        "set"
      }
      val setNull = if (nullable) {
        s"${ev.isNull} = ${ev.value} == null;"
      } else {
        ""
      }
      s"""
         |${ev.value} = (new Decimal()).$setMethod($eval, $precision, $scale);
         |$setNull
         |""".stripMargin
    })
  }
}

object MakeDecimal {
  def apply(child: Expression, precision: Int, scale: Int): MakeDecimal = {
    new MakeDecimal(child, precision, scale)
  }
}

/**
 * An expression used to wrap the children when promote the precision of DecimalType to avoid
 * promote multiple times.
 */
case class PromotePrecision(child: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = child.eval(input)
  /** Just a simple pass-through for code generation. */
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    ev.copy(EmptyBlock)
  override def prettyName: String = "promote_precision"
  override def sql: String = child.sql
  override lazy val canonicalized: Expression = child.canonicalized
}

/**
 * Rounds the decimal to given scale and check whether the decimal can fit in provided precision
 * or not. If not, if `nullOnOverflow` is `true`, it returns `null`; otherwise an
 * `ArithmeticException` is thrown.
 */
case class CheckOverflow(
    child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends UnaryExpression {

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toPrecision(
      dataType.precision,
      dataType.scale,
      Decimal.ROUND_HALF_UP,
      nullOnOverflow)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
         |${ev.value} = $eval.toPrecision(
         |  ${dataType.precision}, ${dataType.scale}, Decimal.ROUND_HALF_UP(), $nullOnOverflow);
         |${ev.isNull} = ${ev.value} == null;
       """.stripMargin
    })
  }

  override def toString: String = s"CheckOverflow($child, $dataType, $nullOnOverflow)"

  override def sql: String = child.sql
}

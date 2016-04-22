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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
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
case class MakeDecimal(child: Expression, precision: Int, scale: Int) extends UnaryExpression {

  override def dataType: DataType = DecimalType(precision, scale)
  override def nullable: Boolean = true
  override def toString: String = s"MakeDecimal($child,$precision,$scale)"

  protected override def nullSafeEval(input: Any): Any =
    Decimal(input.asInstanceOf[Long], precision, scale)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        ${ev.value} = (new Decimal()).setOrNull($eval, $precision, $scale);
        ${ev.isNull} = ${ev.value} == null;
      """
    })
  }
}

/**
 * An expression used to wrap the children when promote the precision of DecimalType to avoid
 * promote multiple times.
 */
case class PromotePrecision(child: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = child.eval(input)
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev.copy("")
  override def prettyName: String = "promote_precision"
  override def sql: String = child.sql
}

/**
 * Rounds the decimal to given scale and check whether the decimal can fit in provided precision
 * or not, returns null if not.
 */
case class CheckOverflow(child: Expression, dataType: DecimalType) extends UnaryExpression {

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Decimal].clone()
    if (d.changePrecision(dataType.precision, dataType.scale)) {
      d
    } else {
      null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      val tmp = ctx.freshName("tmp")
      s"""
         | Decimal $tmp = $eval.clone();
         | if ($tmp.changePrecision(${dataType.precision}, ${dataType.scale})) {
         |   ${ev.value} = $tmp;
         | } else {
         |   ${ev.isNull} = true;
         | }
       """.stripMargin
    })
  }

  override def toString: String = s"CheckOverflow($child, $dataType)"

  override def sql: String = child.sql
}

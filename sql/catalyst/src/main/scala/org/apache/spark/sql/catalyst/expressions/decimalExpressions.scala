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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Return the unscaled Long value of a Decimal, assuming it fits in a Long.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 */
case class UnscaledValue(child: Expression) extends UnaryExpression with NullIntolerant {

  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toUnscaledLong

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c.toUnscaledLong()")
  }

  override protected def withNewChildInternal(newChild: Expression): UnscaledValue =
    copy(child = newChild)
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
    nullOnOverflow: Boolean) extends UnaryExpression with NullIntolerant {

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

  override protected def withNewChildInternal(newChild: Expression): MakeDecimal =
    copy(child = newChild)
}

object MakeDecimal {
  def apply(child: Expression, precision: Int, scale: Int): MakeDecimal = {
    new MakeDecimal(child, precision, scale)
  }
}

/**
 * Rounds the decimal to given scale and check whether the decimal can fit in provided precision
 * or not. If not, if `nullOnOverflow` is `true`, it returns `null`; otherwise an
 * `ArithmeticException` is thrown.
 */
case class CheckOverflow(
    child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends UnaryExpression with SupportQueryContext {

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toPrecision(
      dataType.precision,
      dataType.scale,
      Decimal.ROUND_HALF_UP,
      nullOnOverflow,
      queryContext)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val errorContextCode = if (nullOnOverflow) {
      "\"\""
    } else {
      ctx.addReferenceObj("errCtx", queryContext)
    }
    nullSafeCodeGen(ctx, ev, eval => {
      // scalastyle:off line.size.limit
      s"""
         |${ev.value} = $eval.toPrecision(
         |  ${dataType.precision}, ${dataType.scale}, Decimal.ROUND_HALF_UP(), $nullOnOverflow, $errorContextCode);
         |${ev.isNull} = ${ev.value} == null;
       """.stripMargin
      // scalastyle:on line.size.limit
    })
  }

  override def toString: String = s"CheckOverflow($child, $dataType)"

  override def sql: String = child.sql

  override protected def withNewChildInternal(newChild: Expression): CheckOverflow =
    copy(child = newChild)

  override def initQueryContext(): String = if (nullOnOverflow) {
    ""
  } else {
    origin.context
  }
}

// A variant `CheckOverflow`, which treats null as overflow. This is necessary in `Sum`.
case class CheckOverflowInSum(
    child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean,
    queryContext: String = "") extends UnaryExpression {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      if (nullOnOverflow) null
      else throw QueryExecutionErrors.overflowInSumOfDecimalError(queryContext)
    } else {
      value.asInstanceOf[Decimal].toPrecision(
        dataType.precision,
        dataType.scale,
        Decimal.ROUND_HALF_UP,
        nullOnOverflow,
        queryContext)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val errorContextCode = if (nullOnOverflow) {
      "\"\""
    } else {
      ctx.addReferenceObj("errCtx", queryContext)
    }
    val nullHandling = if (nullOnOverflow) {
      ""
    } else {
      s"throw QueryExecutionErrors.overflowInSumOfDecimalError($errorContextCode);"
    }
    // scalastyle:off line.size.limit
    val code = code"""
       |${childGen.code}
       |boolean ${ev.isNull} = ${childGen.isNull};
       |Decimal ${ev.value} = null;
       |if (${childGen.isNull}) {
       |  $nullHandling
       |} else {
       |  ${ev.value} = ${childGen.value}.toPrecision(
       |    ${dataType.precision}, ${dataType.scale}, Decimal.ROUND_HALF_UP(), $nullOnOverflow, $errorContextCode);
       |  ${ev.isNull} = ${ev.value} == null;
       |}
       |""".stripMargin
    // scalastyle:on line.size.limit

    ev.copy(code = code)
  }

  override def toString: String = s"CheckOverflowInSum($child, $dataType, $nullOnOverflow)"

  override def sql: String = child.sql

  override protected def withNewChildInternal(newChild: Expression): CheckOverflowInSum =
    copy(child = newChild)
}

/**
 * An add expression for decimal values which is only used internally by Sum/Avg.
 *
 * Nota that, this expression does not check overflow which is different with `Add`. When
 * aggregating values, Spark writes the aggregation buffer values to `UnsafeRow` via
 * `UnsafeRowWriter`, which already checks decimal overflow, so we don't need to do it again in the
 * add expression used by Sum/Avg.
 */
case class DecimalAddNoOverflowCheck(
    left: Expression,
    right: Expression,
    override val dataType: DataType) extends BinaryOperator {
  require(dataType.isInstanceOf[DecimalType])

  override def inputType: AbstractDataType = DecimalType
  override def symbol: String = "+"
  private def decimalMethod: String = "$plus"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override protected def nullSafeEval(input1: Any, input2: Any): Any =
    numeric.plus(input1, input2)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): DecimalAddNoOverflowCheck =
    copy(left = newLeft, right = newRight)
}

/**
 * A divide expression for decimal values which is only used internally by Avg.
 *
 * It will fail when nullOnOverflow is false follows:
 *   - left (sum in avg) is null due to over the max precision 38,
 *     the right (count in avg) should never be null
 *   - the result of divide is overflow
 */
case class DecimalDivideWithOverflowCheck(
    left: Expression,
    right: Expression,
    override val dataType: DecimalType,
    avgQueryContext: String,
    nullOnOverflow: Boolean)
  extends BinaryExpression with ExpectsInputTypes with SupportQueryContext {
  override def nullable: Boolean = nullOnOverflow
  override def inputTypes: Seq[AbstractDataType] = Seq(DecimalType, DecimalType)
  override def initQueryContext(): String = avgQueryContext
  def decimalMethod: String = "$div"

  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      if (nullOnOverflow)  {
        null
      } else {
        throw QueryExecutionErrors.overflowInSumOfDecimalError(queryContext)
      }
    } else {
      val value2 = right.eval(input)
      dataType.fractional.asInstanceOf[Fractional[Any]].div(value1, value2).asInstanceOf[Decimal]
        .toPrecision(dataType.precision, dataType.scale, Decimal.ROUND_HALF_UP, nullOnOverflow,
          queryContext)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val errorContextCode = if (nullOnOverflow) {
      "\"\""
    } else {
      ctx.addReferenceObj("errCtx", queryContext)
    }
    val nullHandling = if (nullOnOverflow) {
      ""
    } else {
      s"throw QueryExecutionErrors.overflowInSumOfDecimalError($errorContextCode);"
    }

    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // scalastyle:off line.size.limit
    val code =
      code"""
         |${eval1.code}
         |${eval2.code}
         |boolean ${ev.isNull} = ${eval1.isNull};
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (${eval1.isNull}) {
         |  $nullHandling
         |} else {
         |  ${ev.value} = ${eval1.value}.$decimalMethod(${eval2.value}).toPrecision(
         |      ${dataType.precision}, ${dataType.scale}, Decimal.ROUND_HALF_UP(), $nullOnOverflow, $errorContextCode);
         |  ${ev.isNull} = ${ev.value} == null;
         |}
      """.stripMargin
    // scalastyle:on line.size.limit
    ev.copy(code = code)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

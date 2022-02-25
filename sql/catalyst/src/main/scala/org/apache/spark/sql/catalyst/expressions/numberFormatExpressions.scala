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

import java.util.Locale

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.util.NumberFormatter
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that converts string to numeric.
 */
@ExpressionDescription(
  usage = """
     _FUNC_(strExpr, formatExpr) - Convert `strExpr` to a number based on the `formatExpr`.
       The format can consist of the following characters:
         '0' or '9':  digit position
         '.' or 'D':  decimal point (only allowed once)
         ',' or 'G':  group (thousands) separator
         '-' or 'S':  sign anchored to number (only allowed once)
         '$':  value with a leading dollar sign (only allowed once)
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('454', '999');
       454
      > SELECT _FUNC_('454.00', '000D00');
       454.00
      > SELECT _FUNC_('12,454', '99G999');
       12454
      > SELECT _FUNC_('$78.12', '$99.99');
       78.12
      > SELECT _FUNC_('12,454.8-', '99G999D9S');
       -12454.8
  """,
  since = "3.3.0",
  group = "string_funcs")
case class ToNumber(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  private lazy val numberFormat = right.eval().toString.toUpperCase(Locale.ROOT)
  private lazy val numberFormatter = new NumberFormatter(numberFormat)

  override def dataType: DataType = numberFormatter.parsedDecimalType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val inputTypeCheck = super.checkInputDataTypes()
    if (inputTypeCheck.isSuccess) {
      if (right.foldable) {
        numberFormatter.check()
      } else {
        TypeCheckResult.TypeCheckFailure(s"Format expression must be foldable, but got $right")
      }
    } else {
      inputTypeCheck
    }
  }

  override def prettyName: String = "to_number"

  override def nullSafeEval(string: Any, format: Any): Any = {
    val input = string.asInstanceOf[UTF8String]
    numberFormatter.parse(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val builder =
      ctx.addReferenceObj("builder", numberFormatter, classOf[NumberFormatter].getName)
    val eval = left.genCode(ctx)
    ev.copy(code =
      code"""
        |${eval.code}
        |boolean ${ev.isNull} = ${eval.isNull};
        |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        |if (!${ev.isNull}) {
        |  ${ev.value} = $builder.parse(${eval.value});
        |}
      """.stripMargin)
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): ToNumber = copy(left = newLeft, right = newRight)
}


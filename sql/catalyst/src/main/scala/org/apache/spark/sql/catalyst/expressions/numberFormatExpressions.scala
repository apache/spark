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
import org.apache.spark.sql.catalyst.util.ToNumberParser
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that converts strings to decimal values, returning an exception if the input string
 * fails to match the format string.
 */
@ExpressionDescription(
  usage = """
     _FUNC_(expr, fmt) - Convert string 'expr' to a number based on the string format 'fmt'.
       Throws an exception if the conversion fails. The format can consist of the following
       characters, case insensitive:
         '0' or '9':  Specifies an expected digit between 0 and 9. A 0 to the left of the decimal
           point indicates that 'expr' must have at least as many digits. A leading 9 indicates
           that 'expr' may omit these digits. 'expr' must not be larger than the number of digits
           allowed to the left of the decimal point per the format string. Digits in the format
           string to the right of the decimal indicate the most digits 'expr' may have to the right
           of the decimal point.
         '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
         ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be
           a 0 or 9 to the left of the rightmost grouping separator. 'expr' must match the
           grouping separator relevant for the size of the number.
         '$': Specifies the location of the $ currency sign. This character may only be specified
           once.
         'S': Specifies the position of a '+' or '-' sign (optional, only allowed once).
         'MI': Specifies that 'expr' has an optional '-' sign at the end, but no '+'.
         'PR':   Specifies that 'expr' indicates a negative number with wrapping angled brackets
           ('<1>').
  """,
  examples = """
    Examples:
      -- The format expects:
      --  * an optional sign at the beginning,
      --  * followed by a dollar sign,
      --  * followed by a number between 3 and 6 digits long,
      --  * thousands separators,
      --  * up to two digits beyond the decimal point.
      > SELECT _FUNC_('-$12,345.67', 'S$999,099.99');
       -12345.67
      -- The plus sign is optional, and so are fractional digits.
      > SELECT _FUNC_('$345', 'S$999,099.99');
       345.00
      -- The format requires at least three digits.
      > SELECT _FUNC_('$45', 'S$999,099.99');
       <Error>
      -- The format requires at least three digits.
      > SELECT _FUNC_('$045', 'S$999,099.99');
       45.00
      -- Brackets denote negative values.
      > SELECT _FUNC_('<1234>', '999999PR');
       -1234
  """,
  since = "3.3.0",
  group = "string_funcs")
case class ToNumber(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {
  private lazy val numberFormat = right.eval().toString.toUpperCase(Locale.ROOT)
  private lazy val numberFormatter = new ToNumberParser(numberFormat, true)

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
      ctx.addReferenceObj("builder", numberFormatter, classOf[ToNumberParser].getName)
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
      newLeft: Expression, newRight: Expression): ToNumber =
    copy(left = newLeft, right = newRight)
}

/**
 * A function that converts strings to decimal values, returning NULL if the input string fails to
 * match the format string.
 */
@ExpressionDescription(
  usage = """
     _FUNC_(expr, fmt) - Convert string 'expr' to a number based on the string format `fmt`.
       Returns NULL if the string 'expr' does not match the expected format. The format can consist
       of the following characters, case insensitive:
         '0' or '9':  Specifies an expected digit between 0 and 9. A 0 to the left of the decimal
           point indicates that 'expr' must have at least as many digits. A leading 9 indicates
           that 'expr' may omit these digits. 'expr' must not be larger than the number of digits
           allowed to the left of the decimal point per the format string. Digits in the format
           string to the right of the decimal indicate the most digits 'expr' may have to the right
           of the decimal point.
         '.' or 'D': Specifies the position of the decimal point (optional, only allowed once).
         ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be
           a 0 or 9 to the left of the rightmost grouping separator. 'expr' must match the
           grouping separator relevant for the size of the number.
         '$': Specifies the location of the $ currency sign. This character may only be specified
           once.
         'S': Specifies the position of a '+' or '-' sign (optional, only allowed once).
         'MI': Specifies that 'expr' has an optional '-' sign at the end, but no '+'.
         'PR':   Specifies that 'expr' indicates a negative number with wrapping angled brackets
           ('<1>').
  """,
  examples = """
    Examples:
      -- The format expects:
      --  * an optional sign at the beginning,
      --  * followed by a dollar sign,
      --  * followed by a number between 3 and 6 digits long,
      --  * thousands separators,
      --  * up to two digits beyond the decimal point.
      > SELECT _FUNC_('-$12,345.67', 'S$999,099.99');
       -12345.67
      -- The plus sign is optional, and so are fractional digits.
      > SELECT _FUNC_('$345', 'S$999,099.99');
       345.00
      -- The format requires at least three digits.
      > SELECT _FUNC_('$45', 'S$999,099.99');
       <Error>
      -- The format requires at least three digits.
      > SELECT _FUNC_('$045', 'S$999,099.99');
       45.00
      -- Brackets denote negative values.
      > SELECT _FUNC_('<1234>', '999999PR');
       -1234
  """,
  since = "3.3.0",
  group = "string_funcs")
case class TryToNumber(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {
  private lazy val numberFormat = right.eval().toString.toUpperCase(Locale.ROOT)
  private lazy val numberFormatter = new ToNumberParser(numberFormat, false)

  override def dataType: DataType = numberFormatter.parsedDecimalType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def nullable: Boolean = true
  override def checkInputDataTypes(): TypeCheckResult = ToNumber(left, right).checkInputDataTypes()
  override def prettyName: String = "try_to_number"
  override def nullSafeEval(string: Any, format: Any): Any = {
    val input = string.asInstanceOf[UTF8String]
    numberFormatter.parse(input)
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val builder =
      ctx.addReferenceObj("builder", numberFormatter, classOf[ToNumberParser].getName)
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
      newLeft: Expression,
      newRight: Expression): TryToNumber =
    copy(left = newLeft, right = newRight)
}

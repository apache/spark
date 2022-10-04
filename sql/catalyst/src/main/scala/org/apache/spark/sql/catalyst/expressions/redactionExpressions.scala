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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that converts a string representing a credit card number in a form updated to mask the
 * digits in the interest of redacting the information. Returns an error if the format string is
 * invalid or if the input string does not match the format string.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(input) - Convert string 'input' representing a credit card number to an updated version
    applying a transformation to the characters. This can be useful for creating copies of tables
    with sensitive information removed, but retaining the same schema. Returns an error if the
    format string is invalid or if the input string does not match the format string.
    The format can consist of the following characters, case insensitive:
      - Each 'X' represents a digit which will be converted to 'X' in the result.
      - Each digit '0'-'9' represents a digit which will be left unchanged in the result.
      - Each '-' character should match exactly in the input string.
      - Each whitespace character is ignored.
    No other format characters are allowed. Any whitespace in the input string is left unchanged.
    The default is: XXXX-XXXX-XXXX-XXXX.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(ccn) FROM VALUES ("1234-5678-9876-5432") AS tab(ccn);
        XXXX-XXXX-XXXX-XXXX
      > SELECT _FUNC_("  1234 5678 9876 5432", "XXXX XXXX XXXX 1234");
          XXXX XXXX XXXX 5432
      > SELECT _FUNC_("[1234-5678-9876-5432]", "[XXXX-XXXX-XXXX-1234]");
        Error: the format string is invalid
      > SELECT _FUNC_("1234567898765432");
        Error: the input string does not match the format
      > SELECT _FUNC_("1234567898765432", "XXXX-XXXX-XXXX-1234");
        Error: the input string does not match the format
  """,
  since = "3.4.0",
  group = "string_funcs"
)
case class MaskCcn(left: Expression, right: Expression)
  extends MaskDigitSequence(left, right, "mask_ccn", false) {
  def this(left: Expression) = this(left, Literal(MaskCcn.DEFAULT_FORMAT))
  override protected def withNewChildrenInternal(
      newInput: Expression, newFormat: Expression): MaskCcn =
    copy(left = newInput, right = newFormat)
}

/**
 * A function that converts a string representing a credit card number in a form updated to mask the
 * digits in the interest of redacting the information. Returns an error if the format string is
 * invalid or NULL if the input string does not match the format string.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(input) - Convert string 'input' representing a credit card number to an updated version
    applying a transformation to the characters. This can be useful for creating copies of tables
    with sensitive information removed, but retaining the same schema. Returns an error if the
    format string is invalid or NULL if the input string does not match the format string.
    The format can consist of the following characters, case insensitive:
      - Each 'X' represents a digit which will be converted to 'X' in the result.
      - Each digit '0'-'9' represents a digit which will be left unchanged in the result.
      - Each '-' character should match exactly in the input string.
      - Each whitespace character is ignored.
    No other format characters are allowed. Any whitespace in the input string is left unchanged.
    The default is: XXXX-XXXX-XXXX-XXXX.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(ccn) FROM VALUES ("1234-5678-9876-5432") AS tab(ccn);
        XXXX-XXXX-XXXX-XXXX
      > SELECT _FUNC_("  1234 5678 9876 5432", "XXXX XXXX XXXX 1234");
          XXXX XXXX XXXX 5432
      > SELECT _FUNC_("[1234-5678-9876-5432]", "[XXXX-XXXX-XXXX-1234]");
        Error: the format string is invalid
      > SELECT _FUNC_("1234567898765432");
        NULL
      > SELECT _FUNC_("1234567898765432", "XXXX-XXXX-XXXX-1234");
        NULL
  """,
  since = "3.4.0",
  group = "string_funcs"
)
case class TryMaskCcn(left: Expression, right: Expression)
  extends MaskDigitSequence(left, right, "try_mask_ccn", true) {
  def this(left: Expression) = this(left, Literal(MaskCcn.DEFAULT_FORMAT))
  override protected def withNewChildrenInternal(
      newInput: Expression, newFormat: Expression): TryMaskCcn =
    copy(left = newInput, right = newFormat)
  override def nullable: Boolean = true
}

/** Companion object for the MaskCcn and TryMaskCcn classes. */
object MaskCcn {
  val DEFAULT_FORMAT = "XXXX-XXXX-XXXX-XXXX"
}

/** Implementation of an expression to mask digits in a string to replacement characters. */
abstract class MaskDigitSequence(
    left: Expression, right: Expression, functionName: String, nullOnError: Boolean)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {
  private def format: Expression = right

  private lazy val formatString = if (format.foldable) {
    format.eval().toString.toUpperCase(Locale.ROOT)
  } else {
    ""
  }
  private lazy val parser = new MaskDigitSequenceParser(formatString, nullOnError)

  override def prettyName: String = functionName
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def checkInputDataTypes(): TypeCheckResult = {
    val inputTypeCheck = super.checkInputDataTypes()
    if (inputTypeCheck.isSuccess) {
      val formatStringValid = formatString.forall {
        case 'x' | 'X' | '-' => true
        case ch if ch.isDigit || ch.isWhitespace => true
        case _ => false
      }
      if (formatStringValid) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        throw QueryCompilationErrors.maskCcnInvalidFormatError(prettyName)
      }
    } else {
      inputTypeCheck
    }
  }

  override def nullSafeEval(string: Any, format: Any): Any =
    parser.parse(string.asInstanceOf[UTF8String])

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val builder = ctx.addReferenceObj(
      "parser", parser, classOf[MaskDigitSequenceParser].getName)
    val eval = left.genCode(ctx)
    ev.copy(code =
      code"""
        |${eval.code}
        |boolean ${ev.isNull} = ${eval.isNull};
        |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        |if (!${ev.isNull}) {
        |  ${ev.value} = $builder.parse(${eval.value});
        |  if (${ev.value} == null) {
        |    ${ev.isNull} = true;
        |  }
        |}
      """.stripMargin)
  }
}

/** Executes the string parsing steps for the MaskDigitSequence class. */
class MaskDigitSequenceParser(formatString: String, nullOnError: Boolean) extends Serializable {
  def parse(input: UTF8String): UTF8String = {
    val inputString = input.toString
    var formatStringIndex = 0
    var error = false
    def skipFormatWhitespace(): Unit =
      while (formatStringIndex < formatString.length &&
        formatString(formatStringIndex).isWhitespace) {
        formatStringIndex += 1
      }
    // Check and consume each character in the input string, comparing against characters in the
    // format string.
    val result = inputString.map { inputChar =>
      if (error) {
        // If we have encountered an error, leave the input character alone; we will raise an
        // exception or return NULL after this loop has finished.
        inputChar
      } else if (inputChar.isWhitespace) {
        // The input character is whitespace. Ignore it and continue comparing the next input
        // character against the same format string character as this iteration.
        inputChar
      } else if (formatStringIndex >= formatString.length) {
        // We have already consumed all the characters in the format string, but one or more
        // characters exist in the input string. This is an error because the input string does not
        // match the format string.
        error = true
        'X'
      } else {
        // Check the corresponding character in the format string.
        skipFormatWhitespace()
        val newChar = (inputChar, formatString(formatStringIndex)) match {
          // If both the input and format characters are '-', then this is a match, so continue.
          case ('-', '-') =>
            '-'
          // If the input character is a digit and the format character is 'X', this is a match.
          case (inputChar, 'X') if inputChar.isDigit =>
            'X'
          // If both the input and format characters are digits, this is a match.
          case (inputChar, formatChar) if inputChar.isDigit && formatChar.isDigit =>
            inputChar
          // Otherwise, this is an error because the input string does not match the format string.
          case _ =>
            error = true
            'X'
        }
        formatStringIndex += 1
        newChar
      }
    }
    // We have now consumed all the characters in the input string. Check that we have also consumed
    // all the characters in the format string at this point. If not, this is an error because the
    // input string does not match the format string.
    skipFormatWhitespace()
    if (formatStringIndex != formatString.length) {
      error = true
    }
    // If the input string does not match the format string, return NULL for the TRY_MASK_CCN
    // function or throw an exception for the MASK_CCN function. Otherwise, return the formatted
    // result string.
    if (error && nullOnError) {
      null
    } else if (error) {
      throw QueryExecutionErrors.maskCcnFormatMatchError(inputString, formatString)
    } else {
      UTF8String.fromString(result)
    }
  }
}

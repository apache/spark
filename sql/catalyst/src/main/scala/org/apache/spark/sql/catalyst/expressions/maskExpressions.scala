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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_(input[, upperChar, lowerChar, digitChar, otherChar]) - masks the given string value.
       The function replaces characters with 'X' or 'x', and numbers with 'n'.
       This can be useful for creating copies of tables with sensitive information removed.
       Error behavior: null value as replacement argument will throw AnalysisError.
      """,
  arguments = """
    Arguments:
      * input      - string value to mask. Supported types: STRING, VARCHAR, CHAR
      * upperChar  - character to replace upper-case characters with. Specify -1 to retain original character. Default value: 'X'
      * lowerChar  - character to replace lower-case characters with. Specify -1 to retain original character. Default value: 'x'
      * digitChar  - character to replace digit characters with. Specify -1 to retain original character. Default value: 'n'
      * otherChar  - character to replace all other characters with. Specify -1 to retain original character. Default value: -1
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('abcd-EFGH-8765-4321');
        xxxx-XXXX-nnnn-nnnn
      > SELECT _FUNC_('abcd-EFGH-8765-4321', 'Q');
        xxxx-QQQQ-nnnn-nnnn
      > SELECT _FUNC_('AbCD123-@$#', 'Q', 'q');
        QqQQnnn-@$#
      > SELECT _FUNC_('AbCD123-@$#');
        XxXXnnn-@$#
      > SELECT _FUNC_('AbCD123-@$#', 'Q');
        QxQQnnn-@$#
      > SELECT _FUNC_('AbCD123-@$#', 'Q', 'q');
        QqQQnnn-@$#
      > SELECT _FUNC_('AbCD123-@$#', 'Q', 'q', 'd');
        QqQQddd-@$#
      > SELECT _FUNC_('AbCD123-@$#', 'Q', 'q', 'd', 'o');
        QqQQdddoooo
      > SELECT _FUNC_('AbCD123-@$#', -1, 'q', 'd', 'o');
        AqCDdddoooo
      > SELECT _FUNC_('AbCD123-@$#', -1, -1, 'd', 'o');
        AbCDdddoooo
      > SELECT _FUNC_('AbCD123-@$#', -1, -1, -1, 'o');
        AbCD123oooo
      > SELECT _FUNC_(NULL, -1, -1, -1, 'o');
        NULL
      > SELECT _FUNC_(NULL);
        NULL
      > SELECT _FUNC_('AbCD123-@$#', -1, -1, -1, -1);
        AbCD123-@$#
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Mask(
    input: Expression,
    upperChar: Expression,
    lowerChar: Expression,
    digitChar: Expression,
    otherChar: Expression)
    extends QuinaryExpression
    with ImplicitCastInputTypes
    with QueryErrorsBase
    with NullIntolerant {

  def this(input: Expression) =
    this(
      input,
      Literal(Mask.MASKED_UPPERCASE),
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE))

  def this(input: Expression, upperChar: Expression) =
    this(
      input,
      upperChar,
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE))

  def this(input: Expression, upperChar: Expression, lowerChar: Expression) =
    this(input, upperChar, lowerChar, Literal(Mask.MASKED_DIGIT), Literal(Mask.MASKED_IGNORE))

  def this(
      input: Expression,
      upperChar: Expression,
      lowerChar: Expression,
      digitChar: Expression) =
    this(input, upperChar, lowerChar, digitChar, Literal(Mask.MASKED_IGNORE))

  override def checkInputDataTypes(): TypeCheckResult = {

    def checkInputDataType(exp: Expression, message: String): Option[TypeCheckResult] = {
      if (!exp.foldable) {
        Some(
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> message,
              "inputType" -> toSQLType(exp.dataType),
              "inputExpr" -> toSQLExpr(exp))))
      } else {
        val replaceChar = exp.eval()
        if (replaceChar == null) {
          Some(
            DataTypeMismatch(
              errorSubClass = "UNEXPECTED_NULL",
              messageParameters = Map("exprName" -> message)))
        } else if (!replaceChar.asInstanceOf[UTF8String].toString.equals(Mask.MASKED_IGNORE) &&
          replaceChar.asInstanceOf[UTF8String].numChars != 1) {
          Some(
            DataTypeMismatch(
              errorSubClass = "INPUT_SIZE_NOT_ONE",
              messageParameters = Map("exprName" -> message)))
        } else {
          None
        }
      }
    }

    val defaultCheckResult = super.checkInputDataTypes()
    if (defaultCheckResult.isSuccess) {
      Seq(
        (upperChar, "upperChar"),
        (lowerChar, "lowerChar"),
        (digitChar, "digitChar"),
        (otherChar, "otherChar"))
        .flatMap { case (exp: Expression, message: String) =>
          checkInputDataType(exp, message)
        }
        .headOption
        .getOrElse(defaultCheckResult)
    } else {
      defaultCheckResult
    }
  }

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   *   1. a specific data type, e.g. LongType, StringType. 2. a non-leaf abstract data type, e.g.
   *      NumericType, IntegralType, FractionalType.
   */
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, StringType, StringType, StringType)

  /**
   * Called by default [[eval]] implementation. If subclass of QuinaryExpression keep the default
   * nullability, they can override this method to save null-check code. If we need full control
   * of evaluation process, we should override [[eval]].
   */
  override protected def nullSafeEval(
      input: Any,
      upperChar: Any,
      lowerChar: Any,
      digitChar: Any,
      otherChar: Any): Any =
    Mask.transformInput(
      input.asInstanceOf[UTF8String],
      upperChar.asInstanceOf[UTF8String],
      lowerChar.asInstanceOf[UTF8String],
      digitChar.asInstanceOf[UTF8String],
      otherChar.asInstanceOf[UTF8String])

  /**
   * Returns Java source code that can be compiled to evaluate this expression. The default
   * behavior is to call the eval method of the expression. Concrete expression implementations
   * should override this to do actual code generation.
   *
   * @param ctx
   *   a [[CodegenContext]]
   * @param ev
   *   an [[ExprCode]] with unique terms.
   * @return
   *   an [[ExprCode]] containing the Java source code to generate the given expression
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(
      ctx,
      ev,
      (input, upperChar, lowerChar, digitChar, otherChar) => {
        s"org.apache.spark.sql.catalyst.expressions.Mask." +
          s"transformInput($input, $upperChar, $lowerChar, $digitChar, $otherChar);"
      })

  /**
   * Returns the [[DataType]] of the result of evaluating this expression. It is invalid to query
   * the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = StringType

  /**
   * Returns a Seq of the children of this node. Children should not change. Immutability required
   * for containsChild optimization
   */
  override def children: Seq[Expression] =
    Seq(input, upperChar, lowerChar, digitChar, otherChar)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Mask =
    copy(
      input = newChildren(0),
      upperChar = newChildren(1),
      lowerChar = newChildren(2),
      digitChar = newChildren(3),
      otherChar = newChildren(4))
}

case class MaskArgument(maskChar: Char, ignore: Boolean)

object Mask {
  // Default character to replace upper-case characters
  private val MASKED_UPPERCASE = 'X'
  // Default character to replace lower-case characters
  private val MASKED_LOWERCASE = 'x'
  // Default character to replace digits
  private val MASKED_DIGIT = 'n'
  // This value helps to retain original value in the input by ignoring the replacement rules
  private val MASKED_IGNORE = "-1"

  private def createMaskArgument(maskArgument: UTF8String): MaskArgument = {
    val maskArgumentStr = maskArgument.toString
    MaskArgument(maskArgumentStr.toString.charAt(0), MASKED_IGNORE.equals(maskArgumentStr))
  }

  def transformInput(
      input: UTF8String,
      maskUpper: UTF8String,
      maskLower: UTF8String,
      maskDigit: UTF8String,
      maskOther: UTF8String): UTF8String = {
    val maskUpperArg = createMaskArgument(maskUpper)
    val maskLowerArg = createMaskArgument(maskLower)
    val maskDigitArg = createMaskArgument(maskDigit)
    val markOtherArg = createMaskArgument(maskOther)

    val transformedString = input.toString.map {
      transformChar(_, maskUpperArg, maskLowerArg, maskDigitArg, markOtherArg).toChar
    }
    org.apache.spark.unsafe.types.UTF8String.fromString(transformedString)
  }

  private def transformChar(
      c: Int,
      maskUpper: MaskArgument,
      maskLower: MaskArgument,
      maskDigit: MaskArgument,
      maskOther: MaskArgument): Int = {

    Character.getType(c) match {
      case Character.UPPERCASE_LETTER => if (!maskUpper.ignore) maskUpper.maskChar else c
      case Character.LOWERCASE_LETTER => if (!maskLower.ignore) maskLower.maskChar else c
      case Character.DECIMAL_DIGIT_NUMBER => if (!maskDigit.ignore) maskDigit.maskChar else c
      case _ => if (!maskOther.ignore) maskOther.maskChar else c
    }
  }
}

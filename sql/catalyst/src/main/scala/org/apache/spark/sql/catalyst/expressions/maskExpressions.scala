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
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """_FUNC_(input[, upperChar, lowerChar, digitChar, otherChar]) - masks the given string value.
       The function replaces characters with 'X' or 'x', and numbers with 'n'.
       This can be useful for creating copies of tables with sensitive information removed.
      """,
  arguments = """
    Arguments:
      * input      - string value to mask. Supported types: STRING, VARCHAR, CHAR
      * upperChar  - character to replace upper-case characters with. Specify NULL to retain original character. Default value: 'X'
      * lowerChar  - character to replace lower-case characters with. Specify NULL to retain original character. Default value: 'x'
      * digitChar  - character to replace digit characters with. Specify NULL to retain original character. Default value: 'n'
      * otherChar  - character to replace all other characters with. Specify NULL to retain original character. Default value: NULL
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
      > SELECT _FUNC_('AbCD123-@$#', NULL, 'q', 'd', 'o');
        AqCDdddoooo
      > SELECT _FUNC_('AbCD123-@$#', NULL, NULL, 'd', 'o');
        AbCDdddoooo
      > SELECT _FUNC_('AbCD123-@$#', NULL, NULL, NULL, 'o');
        AbCD123oooo
      > SELECT _FUNC_(NULL, NULL, NULL, NULL, 'o');
        NULL
      > SELECT _FUNC_(NULL);
        NULL
      > SELECT _FUNC_('AbCD123-@$#', NULL, NULL, NULL, NULL);
        AbCD123-@$#
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
object MaskExpressionBuilder extends ExpressionBuilder {
  override def functionSignature: Option[FunctionSignature] = {
    val strArg = InputParameter("str")
    val upperCharArg = InputParameter("upperChar",
      Some(Literal.create(Mask.MASKED_UPPERCASE, StringType)))
    val lowerCharArg = InputParameter("lowerChar",
      Some(Literal.create(Mask.MASKED_LOWERCASE, StringType)))
    val digitCharArg = InputParameter("digitChar",
      Some(Literal.create(Mask.MASKED_DIGIT, StringType)))
    val otherCharArg = InputParameter("otherChar",
      Some(Literal.create(Mask.MASKED_IGNORE, StringType)))
    val functionSignature: FunctionSignature = FunctionSignature(Seq(
      strArg, upperCharArg, lowerCharArg, digitCharArg, otherCharArg))
    Some(functionSignature)
  }

  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 5)
    new Mask(expressions(0), expressions(1), expressions(2), expressions(3), expressions(4))
  }
}

case class Mask(
    input: Expression,
    upperChar: Expression,
    lowerChar: Expression,
    digitChar: Expression,
    otherChar: Expression)
    extends QuinaryExpression
    with ExpectsInputTypes
    with QueryErrorsBase {

  def this(input: Expression) =
    this(
      input,
      Literal.create(Mask.MASKED_UPPERCASE, StringType),
      Literal.create(Mask.MASKED_LOWERCASE, StringType),
      Literal.create(Mask.MASKED_DIGIT, StringType),
      Literal.create(Mask.MASKED_IGNORE, input.dataType))

  def this(input: Expression, upperChar: Expression) =
    this(
      input,
      upperChar,
      Literal.create(Mask.MASKED_LOWERCASE, StringType),
      Literal.create(Mask.MASKED_DIGIT, StringType),
      Literal.create(Mask.MASKED_IGNORE, input.dataType))

  def this(input: Expression, upperChar: Expression, lowerChar: Expression) =
    this(
      input,
      upperChar,
      lowerChar,
      Literal.create(Mask.MASKED_DIGIT, StringType),
      Literal.create(Mask.MASKED_IGNORE, input.dataType))

  def this(
      input: Expression,
      upperChar: Expression,
      lowerChar: Expression,
      digitChar: Expression) =
    this(input, upperChar, lowerChar, digitChar,
      Literal.create(Mask.MASKED_IGNORE, input.dataType))

  override def checkInputDataTypes(): TypeCheckResult = {

    def checkInputDataType(exp: Expression, message: String): Option[TypeCheckResult] = {
      if (!exp.foldable) {
        Some(
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId(message),
              "inputType" -> toSQLType(exp.dataType),
              "inputExpr" -> toSQLExpr(exp))))
      } else {
        val replaceChar = exp.eval()
        if (replaceChar != null && replaceChar.asInstanceOf[UTF8String].numChars != 1) {
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
    Seq(
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true))

  override def nullable: Boolean = true

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    Mask.transformInput(
      children(0).eval(input),
      children(1).eval(input),
      children(2).eval(input),
      children(3).eval(input),
      children(4).eval(input))
  }

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
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   *   function that accepts the 5 non-null evaluation result names of children and returns Java
   *   code to compute the output.
   */
  override protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val resultCode =
      f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value, fifthGen.value)
    if (nullable) {
      // this function is somewhat like a `UnaryExpression`, in that only the first child
      // determines whether the result is null
      val nullSafeEval = ctx.nullSafeExec(children(0).nullable, firstGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        boolean ${ev.isNull} = ${firstGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(
        code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral)
    }
  }

  /**
   * Returns the [[DataType]] of the result of evaluating this expression. It is invalid to query
   * the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = input.dataType

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
  val MASKED_UPPERCASE = 'X'
  // Default character to replace lower-case characters
  val MASKED_LOWERCASE = 'x'
  // Default character to replace digits
  val MASKED_DIGIT = 'n'
  // This value helps to retain original value in the input by ignoring the replacement rules
  val MASKED_IGNORE = null

  def transformInput(
      input: Any,
      maskUpper: Any,
      maskLower: Any,
      maskDigit: Any,
      maskOther: Any): UTF8String = {

    val transformedString = if (input == null) {
      null
    } else {
      input.toString.map {
        transformChar(_, maskUpper, maskLower, maskDigit, maskOther).toChar
      }
    }
    org.apache.spark.unsafe.types.UTF8String.fromString(transformedString)
  }

  private def transformChar(
      c: Char,
      maskUpper: Any,
      maskLower: Any,
      maskDigit: Any,
      maskOther: Any): Int = {

    def maskedChar(c: Char, option: Any): Char = {
      if (option != MASKED_IGNORE) option.asInstanceOf[UTF8String].toString.charAt(0) else c
    }

    Character.getType(c) match {
      case Character.UPPERCASE_LETTER => maskedChar(c, maskUpper)
      case Character.LOWERCASE_LETTER => maskedChar(c, maskLower)
      case Character.DECIMAL_DIGIT_NUMBER => maskedChar(c, maskDigit)
      case _ => maskedChar(c, maskOther)
    }
  }
}

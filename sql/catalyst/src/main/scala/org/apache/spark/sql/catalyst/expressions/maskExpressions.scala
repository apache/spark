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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.{AbstractDataType, DataType, IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This trait has common methods, which can be used by Classes implementing Masking functions. For
 * example; Mask, MaskFirstN, etc
 */
private[sql] trait Maskable extends ExpectsInputTypes with QueryErrorsBase {
  protected val upperCharExpr: Expression
  protected val lowerCharExpr: Expression
  protected val digitCharExpr: Expression
  protected val otherCharExpr: Expression

  /**
   * 1) these four are lazy vals in order to cache them for efficiency, and 2) they must all be
   * constant expression trees, which we enforce in each of the derived expressions. If we violate
   * (2), we have a correctness bug.
   */
  @transient
  protected lazy val upperChar = upperCharExpr.eval()
  @transient
  protected lazy val lowerChar = lowerCharExpr.eval()
  @transient
  protected lazy val digitChar = digitCharExpr.eval()
  @transient
  protected lazy val otherChar = otherCharExpr.eval()

  /**
   * Derived expressions shall override this method to support data type validation of additional
   * parameters other than fields validated in method checkInputDataTypes().
   * @return
   *   Seq[Option[TypeCheckResult]]
   */
  protected def validateAdditionalFields(): Seq[Option[TypeCheckResult]] = Seq(None)

  private def validateInputDataTypes(
      typeCheckResult: TypeCheckResult,
      expressions: Seq[(Expression, String)]): TypeCheckResult = {

    if (typeCheckResult.isSuccess) {
      (expressions
        .map { case (exp: Expression, message: String) =>
          validateInputDataType(exp, message)
        } ++ validateAdditionalFields).flatten.headOption
        .getOrElse(typeCheckResult)
    } else {
      typeCheckResult
    }
  }

  private def validateInputDataType(exp: Expression, message: String): Option[TypeCheckResult] =
    exp.foldable match {
      case false =>
        Some(
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> message,
              "inputType" -> toSQLType(exp.dataType),
              "inputExpr" -> toSQLExpr(exp))))
      case _ =>
        exp.eval() match {
          case s: UTF8String if (s.numChars() != 1) =>
            Some(
              DataTypeMismatch(
                errorSubClass = "INPUT_SIZE_NOT_ONE",
                messageParameters = Map("exprName" -> message)))
          case _ => None
        }
    }

  /**
   * Returns the [[DataType]] of the result of evaluating this expression. It is invalid to query
   * the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = StringType

  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult =
    validateInputDataTypes(
      super.checkInputDataTypes(),
      Seq(
        (upperCharExpr, "upperChar"),
        (lowerCharExpr, "lowerChar"),
        (digitCharExpr, "digitChar"),
        (otherCharExpr, "otherChar")))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """_FUNC_(input[, upperChar, lowerChar, digitChar, otherChar]) - masks the given string value.
       The function replaces characters with 'X' or 'x', and numbers with 'n'.
       This can be useful for creating copies of tables with sensitive information removed.
      """,
  arguments = """
    Arguments:
      * input      - string value to mask. Supported types: STRING, VARCHAR, CHAR. Except this parameter all other parameters must be constants.
      * upperChar  - character to replace upper-case characters with. Specify NULL to retain original character. Default value: 'X'.
      * lowerChar  - character to replace lower-case characters with. Specify NULL to retain original character. Default value: 'x'.
      * digitChar  - character to replace digit characters with. Specify NULL to retain original character. Default value: 'n'.
      * otherChar  - character to replace all other characters with. Specify NULL to retain original character. Default value: NULL.

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
case class Mask(
    input: Expression,
    override val upperCharExpr: Expression,
    override val lowerCharExpr: Expression,
    override val digitCharExpr: Expression,
    override val otherCharExpr: Expression)
    extends QuinaryExpression
    with Maskable {

  def this(input: Expression) =
    this(
      input,
      Literal(Mask.MASKED_UPPERCASE),
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(input: Expression, upperCharExpr: Expression) =
    this(
      input,
      upperCharExpr,
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(input: Expression, upperCharExpr: Expression, lowerCharExpr: Expression) =
    this(
      input,
      upperCharExpr,
      lowerCharExpr,
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(
      input: Expression,
      upperCharExpr: Expression,
      lowerCharExpr: Expression,
      digitCharExpr: Expression) =
    this(
      input,
      upperCharExpr,
      lowerCharExpr,
      digitCharExpr,
      Literal(Mask.MASKED_IGNORE, StringType))

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
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    Mask.mask(children(0).eval(input), upperChar, lowerChar, digitChar, otherChar)
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
          s"mask($input, $upperChar, $lowerChar, $digitChar, $otherChar);"
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
   * Returns a Seq of the children of this node. Children should not change. Immutability required
   * for containsChild optimization
   */
  override def children: Seq[Expression] =
    Seq(input, upperCharExpr, lowerCharExpr, digitCharExpr, otherCharExpr)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Mask =
    copy(
      input = newChildren(0),
      upperCharExpr = newChildren(1),
      lowerCharExpr = newChildren(2),
      digitCharExpr = newChildren(3),
      otherCharExpr = newChildren(4))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    """_FUNC_(input[, charCount, upperChar, lowerChar, digitChar, otherChar]) - masks the first n characters of given string value.
       The function masks the first charCount characters of the value with 'X' or 'x', and numbers with 'n'.
       If the length of input string is less than charCount , value of charCount will be replaced with length of input string.
       This can be useful for creating copies of tables with sensitive information removed.
       Error behavior: null value as replacement argument will throw AnalysisError.
      """,
  arguments = """
    Arguments:
      * input      - string value to mask. Supported types: STRING, VARCHAR, CHAR. Except this parameter all other parameters must be constants.
      * charCount  - number of characters to be masked. Default value: 4.
      * upperChar  - character to replace upper-case characters with. Specify NULL to retain original character. Default value: 'X'.
      * lowerChar  - character to replace lower-case characters with. Specify NULL to retain original character. Default value: 'x'.
      * digitChar  - character to replace digit characters with. Specify NULL to retain original character. Default value: 'n'.
      * otherChar  - character to replace all other characters with. Specify NULL to retain original character. Default value: NULL.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('abcd-EFGH-8765-4321');
        xxxx-EFGH-8765-4321
      > SELECT _FUNC_('abcd-EFGH-8765-4321', 9);
        xxxx-XXXX-8765-4321
      > SELECT _FUNC_('abcd-EFGH-8765-@$#', 14);
        xxxx-XXXX-nnnn-@$#
      > SELECT _FUNC_('abcd-EFGH-8765-@$#', 15, 'x', 'X', 'n', 'o');
        XXXXoxxxxonnnno@$#
      > SELECT _FUNC_('abcd-EFGH-8765-@$#', 20, 'x', 'X', 'n', 'o');
        XXXXoxxxxonnnnoooo
      > SELECT _FUNC_('AbCD123-@$#', 10,'Q', 'q', 'd', 'o');
        QqQQdddooo#
      > SELECT _FUNC_('AbCD123-@$#', 10, NULL, 'q', 'd', 'o');
        AqCDdddooo#
      > SELECT _FUNC_('AbCD123-@$#', 10, NULL, NULL, 'd', 'o');
        AbCDdddooo#
      > SELECT _FUNC_('AbCD123-@$#', 10, NULL, NULL, NULL, 'o');
        AbCD123ooo#
      > SELECT _FUNC_(NULL);
        NULL
      > SELECT _FUNC_(NULL, 1, NULL, NULL, 'o');
        NULL
  """,
  since = "3.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class MaskFirstN(
    input: Expression,
    charCountExpr: Expression,
    override val upperCharExpr: Expression,
    override val lowerCharExpr: Expression,
    override val digitCharExpr: Expression,
    override val otherCharExpr: Expression)
    extends SeptenaryExpression
    with Maskable {

  def this(input: Expression) =
    this(
      input,
      Literal(Mask.DEFAULT_CHAR_COUNT),
      Literal(Mask.MASKED_UPPERCASE),
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(input: Expression, charCountExpr: Expression) =
    this(
      input,
      charCountExpr,
      Literal(Mask.MASKED_UPPERCASE),
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(input: Expression, charCountExpr: Expression, upperCharExpr: Expression) =
    this(
      input,
      charCountExpr,
      upperCharExpr,
      Literal(Mask.MASKED_LOWERCASE),
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(
      input: Expression,
      charCountExpr: Expression,
      upperCharExpr: Expression,
      lowerCharExpr: Expression) =
    this(
      input,
      charCountExpr,
      upperCharExpr,
      lowerCharExpr,
      Literal(Mask.MASKED_DIGIT),
      Literal(Mask.MASKED_IGNORE, StringType))

  def this(
      input: Expression,
      charCountExpr: Expression,
      upperCharExpr: Expression,
      lowerCharExpr: Expression,
      digitCharExpr: Expression) =
    this(
      input,
      charCountExpr,
      upperCharExpr,
      lowerCharExpr,
      digitCharExpr,
      Literal(Mask.MASKED_IGNORE, StringType))

  @transient
  private lazy val charCount = charCountExpr.eval().asInstanceOf[Int].max(0)

  override def validateAdditionalFields(): Seq[Option[TypeCheckResult]] = {
    val typeCheckResults = charCountExpr.foldable match {
      case true if charCountExpr.eval() == null =>
        Some(
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_NULL",
            messageParameters = Map("exprName" -> "charCount")))
      case false =>
        Some(
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> "charCount",
              "inputType" -> toSQLType(charCountExpr.dataType),
              "inputExpr" -> toSQLExpr(charCountExpr))))
      case _ => None
    }
    Seq(typeCheckResults)
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
    Seq(StringType, IntegerType, StringType, StringType, StringType, StringType)

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    Mask.mask_first_n(
      children(0).eval(input),
      charCount,
      upperChar,
      lowerChar,
      digitChar,
      otherChar)
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
      (input, count, upperChar, lowerChar, digitChar, otherChar, inputOpt) => {
        s"org.apache.spark.sql.catalyst.expressions.Mask." +
          s"mask_first_n($input, $charCount,$upperChar, $lowerChar, $digitChar, $otherChar);"
      })

  /**
   * Short hand for generating septenary evaluation code. If either of the sub-expressions is
   * null, the result of this computation is assumed to be null.
   *
   * @param f
   *   function that accepts the 7 non-null evaluation result names of children and returns Java
   *   code to compute the output.
   */
  override protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val sixthGen = children(5).genCode(ctx)
    val resultCode =
      f(
        firstGen.value,
        secondGen.value,
        thirdGen.value,
        fourthGen.value,
        fifthGen.value,
        sixthGen.value,
        None)

    if (nullable) {
      // this function is somewhat like a `UnaryExpression`, in that only the first child
      // determines whether the result is null
      val nullSafeEval = ctx.nullSafeExec(children(0).nullable, firstGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${firstGen.code}
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
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral)
    }
  }

  /**
   * Returns a Seq of the children of this node. Children should not change. Immutability required
   * for containsChild optimization
   */
  override def children: Seq[Expression] =
    Seq(input, charCountExpr, upperCharExpr, lowerCharExpr, digitCharExpr, otherCharExpr)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): MaskFirstN =
    copy(
      input = newChildren(0),
      charCountExpr = newChildren(1),
      upperCharExpr = newChildren(2),
      lowerCharExpr = newChildren(3),
      digitCharExpr = newChildren(4),
      otherCharExpr = newChildren(5))
}

object Mask {
  // Default number of characters to be masked
  val DEFAULT_CHAR_COUNT = 4
  // Default character to replace digits
  val MASKED_DIGIT = 'n'
  // This value helps to retain original value in the input by ignoring the replacement rules
  val MASKED_IGNORE = null
  // Default character to replace lower-case characters
  val MASKED_LOWERCASE = 'x'
  // Default character to replace upper-case characters
  val MASKED_UPPERCASE = 'X'

  def mask(
      input: Any,
      maskUpper: Any,
      maskLower: Any,
      maskDigit: Any,
      maskOther: Any): UTF8String =
    transformString(input, maskUpper, maskLower, maskDigit, maskOther)

  def mask_first_n(
      input: Any,
      charCount: Int,
      maskUpper: Any,
      maskLower: Any,
      maskDigit: Any,
      maskOther: Any): UTF8String =
    transformString(input, maskUpper, maskLower, maskDigit, maskOther, _.min(charCount))

  private def transformString(
      input: Any,
      maskUpper: Any,
      maskLower: Any,
      maskDigit: Any,
      maskOther: Any,
      f: Int => Int = x => x) = {
    val transformedString = if (input == null) {
      null
    } else {
      val inputStr = input.asInstanceOf[UTF8String].toString
      val stringSize = inputStr.length
      // Get the actual end index from calling method.
      val endIdx = f(stringSize)
      val chars: IndexedSeq[Char] = for (i <- 0 until stringSize) yield {
        val ch = inputStr.charAt(i)
        if (i < endIdx) {
          transformChar(ch, maskUpper, maskLower, maskDigit, maskOther).toChar
        } else {
          ch
        }
      }
      chars.mkString
    }
    UTF8String.fromString(transformedString)
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

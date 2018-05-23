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

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.MaskExpressionsUtils._
import org.apache.spark.sql.catalyst.expressions.MaskLike._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


trait MaskLike {
  def upper: String
  def lower: String
  def digit: String

  protected lazy val upperReplacement: Int = getReplacementChar(upper, defaultMaskedUppercase)
  protected lazy val lowerReplacement: Int = getReplacementChar(lower, defaultMaskedLowercase)
  protected lazy val digitReplacement: Int = getReplacementChar(digit, defaultMaskedDigit)

  protected val maskUtilsClassName: String = classOf[MaskExpressionsUtils].getName

  def inputStringLengthCode(inputString: String, length: String): String = {
    s"${CodeGenerator.JAVA_INT} $length = $inputString.codePointCount(0, $inputString.length());"
  }

  def appendMaskedToStringBuilderCode(
      ctx: CodegenContext,
      sb: String,
      inputString: String,
      offset: String,
      numChars: String): String = {
    val i = ctx.freshName("i")
    val codePoint = ctx.freshName("codePoint")
    s"""
       |for (${CodeGenerator.JAVA_INT} $i = 0; $i < $numChars; $i++) {
       |  ${CodeGenerator.JAVA_INT} $codePoint = $inputString.codePointAt($offset);
       |  $sb.appendCodePoint($maskUtilsClassName.transformChar($codePoint,
       |    $upperReplacement, $lowerReplacement,
       |    $digitReplacement, $defaultMaskedOther));
       |  $offset += Character.charCount($codePoint);
       |}
     """.stripMargin
  }

  def appendUnchangedToStringBuilderCode(
      ctx: CodegenContext,
      sb: String,
      inputString: String,
      offset: String,
      numChars: String): String = {
    val i = ctx.freshName("i")
    val codePoint = ctx.freshName("codePoint")
    s"""
       |for (${CodeGenerator.JAVA_INT} $i = 0; $i < $numChars; $i++) {
       |  ${CodeGenerator.JAVA_INT} $codePoint = $inputString.codePointAt($offset);
       |  $sb.appendCodePoint($codePoint);
       |  $offset += Character.charCount($codePoint);
       |}
     """.stripMargin
  }

  def appendMaskedToStringBuilder(
      sb: java.lang.StringBuilder,
      inputString: String,
      startOffset: Int,
      numChars: Int): Int = {
    var offset = startOffset
    (1 to numChars) foreach { _ =>
      val codePoint = inputString.codePointAt(offset)
      sb.appendCodePoint(transformChar(
        codePoint,
        upperReplacement,
        lowerReplacement,
        digitReplacement,
        defaultMaskedOther))
      offset += Character.charCount(codePoint)
    }
    offset
  }

  def appendUnchangedToStringBuilder(
      sb: java.lang.StringBuilder,
      inputString: String,
      startOffset: Int,
      numChars: Int): Int = {
    var offset = startOffset
    (1 to numChars) foreach { _ =>
      val codePoint = inputString.codePointAt(offset)
      sb.appendCodePoint(codePoint)
      offset += Character.charCount(codePoint)
    }
    offset
  }
}

trait MaskLikeWithN extends MaskLike {
  def n: Int
  protected lazy val charCount: Int = if (n < 0) 0 else n
}

/**
 * Utils for mask operations.
 */
object MaskLike {
  val defaultCharCount = 4
  val defaultMaskedUppercase: Int = 'X'
  val defaultMaskedLowercase: Int = 'x'
  val defaultMaskedDigit: Int = 'n'
  val defaultMaskedOther: Int = MaskExpressionsUtils.UNMASKED_VAL

  def extractCharCount(e: Expression): Int = e match {
    case Literal(i, IntegerType | NullType) =>
      if (i == null) defaultCharCount else i.asInstanceOf[Int]
    case Literal(_, dt) => throw new AnalysisException("Expected literal expression of type " +
      s"${IntegerType.simpleString}, but got literal of ${dt.simpleString}")
    case other => throw new AnalysisException(s"Expected literal expression, but got ${other.sql}")
  }

  def extractReplacement(e: Expression): String = e match {
    case Literal(s, StringType | NullType) => if (s == null) null else s.toString
    case Literal(_, dt) => throw new AnalysisException("Expected literal expression of type " +
      s"${StringType.simpleString}, but got literal of ${dt.simpleString}")
    case other => throw new AnalysisException(s"Expected literal expression, but got ${other.sql}")
  }
}

/**
 * Masks the input string. Additional parameters can be set to change the masking chars for
 * uppercase letters, lowercase letters and digits.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, upper[, lower[, digit]]]) - Masks str. By default, upper case letters are converted to \"X\", lower case letters are converted to \"x\" and numbers are converted to \"n\". You can override the characters used in the mask by supplying additional arguments: the second argument controls the mask character for upper case letters, the third argument for lower case letters and the fourth argument for numbers.",
  examples = """
    Examples:
      > SELECT _FUNC_("abcd-EFGH-8765-4321", "U", "l", "#");
       llll-UUUU-####-####
  """)
// scalastyle:on line.size.limit
case class Mask(child: Expression, upper: String, lower: String, digit: String)
  extends UnaryExpression with ExpectsInputTypes with MaskLike {

  def this(child: Expression) = this(child, null.asInstanceOf[String], null, null)

  def this(child: Expression, upper: Expression) =
    this(child, extractReplacement(upper), null, null)

  def this(child: Expression, upper: Expression, lower: Expression) =
    this(child, extractReplacement(upper), extractReplacement(lower), null)

  def this(child: Expression, upper: Expression, lower: Expression, digit: Expression) =
    this(child, extractReplacement(upper), extractReplacement(lower), extractReplacement(digit))

  override def nullSafeEval(input: Any): Any = {
    val str = input.asInstanceOf[UTF8String].toString
    val length = str.codePointCount(0, str.length())
    val sb = new java.lang.StringBuilder(length)
    appendMaskedToStringBuilder(sb, str, 0, length)
    UTF8String.fromString(sb.toString)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val sb = ctx.freshName("sb")
      val length = ctx.freshName("length")
      val offset = ctx.freshName("offset")
      val inputString = ctx.freshName("inputString")
      s"""
         |String $inputString = $input.toString();
         |${inputStringLengthCode(inputString, length)}
         |StringBuilder $sb = new StringBuilder($length);
         |${CodeGenerator.JAVA_INT} $offset = 0;
         |${appendMaskedToStringBuilderCode(ctx, sb, inputString, offset, length)}
         |${ev.value} = UTF8String.fromString($sb.toString());
       """.stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
}

/**
 * Masks the first N chars of the input string. N defaults to 4. Additional parameters can be set
 * to change the masking chars for uppercase letters, lowercase letters and digits.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, n[, upper[, lower[, digit]]]]) - Masks the first n values of str. By default, n is 4, upper case letters are converted to \"X\", lower case letters are converted to \"x\" and numbers are converted to \"n\". You can override the characters used in the mask by supplying additional arguments: the second argument controls the mask character for upper case letters, the third argument for lower case letters and the fourth argument for numbers.",
  examples = """
    Examples:
      > SELECT _FUNC_("1234-5678-8765-4321", 4);
       nnnn-5678-8765-4321
  """)
// scalastyle:on line.size.limit
case class MaskFirstN(
    child: Expression,
    n: Int,
    upper: String,
    lower: String,
    digit: String)
  extends UnaryExpression with ExpectsInputTypes with MaskLikeWithN {

  def this(child: Expression) =
    this(child, defaultCharCount, null, null, null)

  def this(child: Expression, n: Expression) =
    this(child, extractCharCount(n), null, null, null)

  def this(child: Expression, n: Expression, upper: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), null, null)

  def this(child: Expression, n: Expression, upper: Expression, lower: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), extractReplacement(lower), null)

  def this(
      child: Expression,
      n: Expression,
      upper: Expression,
      lower: Expression,
      digit: Expression) =
    this(child,
      extractCharCount(n),
      extractReplacement(upper),
      extractReplacement(lower),
      extractReplacement(digit))

  override def nullSafeEval(input: Any): Any = {
    val str = input.asInstanceOf[UTF8String].toString
    val length = str.codePointCount(0, str.length())
    val endOfMask = if (charCount > length) length else charCount
    val sb = new java.lang.StringBuilder(length)
    val offset = appendMaskedToStringBuilder(sb, str, 0, endOfMask)
    appendUnchangedToStringBuilder(sb, str, offset, length - endOfMask)
    UTF8String.fromString(sb.toString)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val sb = ctx.freshName("sb")
      val length = ctx.freshName("length")
      val offset = ctx.freshName("offset")
      val inputString = ctx.freshName("inputString")
      val endOfMask = ctx.freshName("endOfMask")
      s"""
         |String $inputString = $input.toString();
         |${inputStringLengthCode(inputString, length)}
         |${CodeGenerator.JAVA_INT} $endOfMask = $charCount > $length ? $length : $charCount;
         |${CodeGenerator.JAVA_INT} $offset = 0;
         |StringBuilder $sb = new StringBuilder($length);
         |${appendMaskedToStringBuilderCode(ctx, sb, inputString, offset, endOfMask)}
         |${appendUnchangedToStringBuilderCode(
              ctx, sb, inputString, offset, s"$length - $endOfMask")}
         |${ev.value} = UTF8String.fromString($sb.toString());
         |""".stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "mask_first_n"
}

/**
 * Masks the last N chars of the input string. N defaults to 4. Additional parameters can be set
 * to change the masking chars for uppercase letters, lowercase letters and digits.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, n[, upper[, lower[, digit]]]]) - Masks the last n values of str. By default, n is 4, upper case letters are converted to \"X\", lower case letters are converted to \"x\" and numbers are converted to \"n\". You can override the characters used in the mask by supplying additional arguments: the second argument controls the mask character for upper case letters, the third argument for lower case letters and the fourth argument for numbers.",
  examples = """
    Examples:
      > SELECT _FUNC_("1234-5678-8765-4321", 4);
       1234-5678-8765-nnnn
  """, since = "2.4.0")
// scalastyle:on line.size.limit
case class MaskLastN(
    child: Expression,
    n: Int,
    upper: String,
    lower: String,
    digit: String)
  extends UnaryExpression with ExpectsInputTypes with MaskLikeWithN {

  def this(child: Expression) =
    this(child, defaultCharCount, null, null, null)

  def this(child: Expression, n: Expression) =
    this(child, extractCharCount(n), null, null, null)

  def this(child: Expression, n: Expression, upper: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), null, null)

  def this(child: Expression, n: Expression, upper: Expression, lower: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), extractReplacement(lower), null)

  def this(
      child: Expression,
      n: Expression,
      upper: Expression,
      lower: Expression,
      digit: Expression) =
    this(child,
      extractCharCount(n),
      extractReplacement(upper),
      extractReplacement(lower),
      extractReplacement(digit))

  override def nullSafeEval(input: Any): Any = {
    val str = input.asInstanceOf[UTF8String].toString
    val length = str.codePointCount(0, str.length())
    val startOfMask = if (charCount >= length) 0 else length - charCount
    val sb = new java.lang.StringBuilder(length)
    val offset = appendUnchangedToStringBuilder(sb, str, 0, startOfMask)
    appendMaskedToStringBuilder(sb, str, offset, length - startOfMask)
    UTF8String.fromString(sb.toString)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val sb = ctx.freshName("sb")
      val length = ctx.freshName("length")
      val offset = ctx.freshName("offset")
      val inputString = ctx.freshName("inputString")
      val startOfMask = ctx.freshName("startOfMask")
      s"""
         |String $inputString = $input.toString();
         |${inputStringLengthCode(inputString, length)}
         |${CodeGenerator.JAVA_INT} $startOfMask = $charCount >= $length ?
         |  0 : $length - $charCount;
         |${CodeGenerator.JAVA_INT} $offset = 0;
         |StringBuilder $sb = new StringBuilder($length);
         |${appendUnchangedToStringBuilderCode(ctx, sb, inputString, offset, startOfMask)}
         |${appendMaskedToStringBuilderCode(
              ctx, sb, inputString, offset, s"$length - $startOfMask")}
         |${ev.value} = UTF8String.fromString($sb.toString());
         |""".stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "mask_last_n"
}

/**
 * Masks all but the first N chars of the input string. N defaults to 4. Additional parameters can
 * be set to change the masking chars for uppercase letters, lowercase letters and digits.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, n[, upper[, lower[, digit]]]]) - Masks all but the first n values of str. By default, n is 4, upper case letters are converted to \"X\", lower case letters are converted to \"x\" and numbers are converted to \"n\". You can override the characters used in the mask by supplying additional arguments: the second argument controls the mask character for upper case letters, the third argument for lower case letters and the fourth argument for numbers.",
  examples = """
    Examples:
      > SELECT _FUNC_("1234-5678-8765-4321", 4);
       1234-nnnn-nnnn-nnnn
  """, since = "2.4.0")
// scalastyle:on line.size.limit
case class MaskShowFirstN(
    child: Expression,
    n: Int,
    upper: String,
    lower: String,
    digit: String)
  extends UnaryExpression with ExpectsInputTypes with MaskLikeWithN {

  def this(child: Expression) =
    this(child, defaultCharCount, null, null, null)

  def this(child: Expression, n: Expression) =
    this(child, extractCharCount(n), null, null, null)

  def this(child: Expression, n: Expression, upper: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), null, null)

  def this(child: Expression, n: Expression, upper: Expression, lower: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), extractReplacement(lower), null)

  def this(
      child: Expression,
      n: Expression,
      upper: Expression,
      lower: Expression,
      digit: Expression) =
    this(child,
      extractCharCount(n),
      extractReplacement(upper),
      extractReplacement(lower),
      extractReplacement(digit))

  override def nullSafeEval(input: Any): Any = {
    val str = input.asInstanceOf[UTF8String].toString
    val length = str.codePointCount(0, str.length())
    val startOfMask = if (charCount > length) length else charCount
    val sb = new java.lang.StringBuilder(length)
    val offset = appendUnchangedToStringBuilder(sb, str, 0, startOfMask)
    appendMaskedToStringBuilder(sb, str, offset, length - startOfMask)
    UTF8String.fromString(sb.toString)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val sb = ctx.freshName("sb")
      val length = ctx.freshName("length")
      val offset = ctx.freshName("offset")
      val inputString = ctx.freshName("inputString")
      val startOfMask = ctx.freshName("startOfMask")
      s"""
         |String $inputString = $input.toString();
         |${inputStringLengthCode(inputString, length)}
         |${CodeGenerator.JAVA_INT} $startOfMask = $charCount > $length ? $length : $charCount;
         |${CodeGenerator.JAVA_INT} $offset = 0;
         |StringBuilder $sb = new StringBuilder($length);
         |${appendUnchangedToStringBuilderCode(ctx, sb, inputString, offset, startOfMask)}
         |${appendMaskedToStringBuilderCode(
              ctx, sb, inputString, offset, s"$length - $startOfMask")}
         |${ev.value} = UTF8String.fromString($sb.toString());
         |""".stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "mask_show_first_n"
}

/**
 * Masks all but the last N chars of the input string. N defaults to 4. Additional parameters can
 * be set to change the masking chars for uppercase letters, lowercase letters and digits.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str[, n[, upper[, lower[, digit]]]]) - Masks all but the last n values of str. By default, n is 4, upper case letters are converted to \"X\", lower case letters are converted to \"x\" and numbers are converted to \"n\". You can override the characters used in the mask by supplying additional arguments: the second argument controls the mask character for upper case letters, the third argument for lower case letters and the fourth argument for numbers.",
  examples = """
    Examples:
      > SELECT _FUNC_("1234-5678-8765-4321", 4);
       nnnn-nnnn-nnnn-4321
  """, since = "2.4.0")
// scalastyle:on line.size.limit
case class MaskShowLastN(
    child: Expression,
    n: Int,
    upper: String,
    lower: String,
    digit: String)
  extends UnaryExpression with ExpectsInputTypes with MaskLikeWithN {

  def this(child: Expression) =
    this(child, defaultCharCount, null, null, null)

  def this(child: Expression, n: Expression) =
    this(child, extractCharCount(n), null, null, null)

  def this(child: Expression, n: Expression, upper: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), null, null)

  def this(child: Expression, n: Expression, upper: Expression, lower: Expression) =
    this(child, extractCharCount(n), extractReplacement(upper), extractReplacement(lower), null)

  def this(
      child: Expression,
      n: Expression,
      upper: Expression,
      lower: Expression,
      digit: Expression) =
    this(child,
      extractCharCount(n),
      extractReplacement(upper),
      extractReplacement(lower),
      extractReplacement(digit))

  override def nullSafeEval(input: Any): Any = {
    val str = input.asInstanceOf[UTF8String].toString
    val length = str.codePointCount(0, str.length())
    val endOfMask = if (charCount >= length) 0 else length - charCount
    val sb = new java.lang.StringBuilder(length)
    val offset = appendMaskedToStringBuilder(sb, str, 0, endOfMask)
    appendUnchangedToStringBuilder(sb, str, offset, length - endOfMask)
    UTF8String.fromString(sb.toString)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val sb = ctx.freshName("sb")
      val length = ctx.freshName("length")
      val offset = ctx.freshName("offset")
      val inputString = ctx.freshName("inputString")
      val endOfMask = ctx.freshName("endOfMask")
      s"""
         |String $inputString = $input.toString();
         |${inputStringLengthCode(inputString, length)}
         |${CodeGenerator.JAVA_INT} $endOfMask = $charCount >= $length ? 0 : $length - $charCount;
         |${CodeGenerator.JAVA_INT} $offset = 0;
         |StringBuilder $sb = new StringBuilder($length);
         |${appendMaskedToStringBuilderCode(ctx, sb, inputString, offset, endOfMask)}
         |${appendUnchangedToStringBuilderCode(
              ctx, sb, inputString, offset, s"$length - $endOfMask")}
         |${ev.value} = UTF8String.fromString($sb.toString());
         |""".stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "mask_show_last_n"
}

/**
 * Returns a hashed value based on str.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns a hashed value based on str. The hash is consistent and can be used to join masked values together across tables.",
  examples = """
    Examples:
      > SELECT _FUNC_("abcd-EFGH-8765-4321");
       60c713f5ec6912229d2060df1c322776
  """)
// scalastyle:on line.size.limit
case class MaskHash(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def nullSafeEval(input: Any): Any = {
    UTF8String.fromString(DigestUtils.md5Hex(input.asInstanceOf[UTF8String].toString))
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (input: String) => {
      val digestUtilsClass = classOf[DigestUtils].getName.stripSuffix("$")
      s"""
         |${ev.value} = UTF8String.fromString($digestUtilsClass.md5Hex($input.toString()));
         |""".stripMargin
    })
  }

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "mask_hash"
}

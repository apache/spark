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

import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.commons.text.StringEscapeUtils
import org.jcodings.specific.UTF8Encoding
import org.joni.{Option, Regex, Syntax}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIKE_FAMLIY, TreePattern}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


abstract class StringRegexExpressionJoni extends BinaryExpression
  with ImplicitCastInputTypes with NullIntolerant with Predicate {

  def escape(v: Array[Byte]): Array[Byte]
  def matches(regex: Regex, str: Array[Byte]): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache foldable pattern
  private lazy val cache: Regex = right match {
    case p: Expression if p.foldable =>
      compile(p.eval().asInstanceOf[UTF8String].getBytes)
    case _ => null
  }

  protected def compile(pattern: Array[Byte]): Regex = if (pattern == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    try {
      val escapedPattern = escape(pattern)
      new Regex(escapedPattern, 0, escapedPattern.length,
        Option.NONE, UTF8Encoding.INSTANCE, Syntax.Java)
    } catch {
      case e: org.joni.exception.SyntaxException =>
        throw QueryExecutionErrors.invalidPatternError(
          prettyName, new String(pattern, StandardCharsets.UTF_8), e)
    }
  }

  protected def pattern(pattern: Array[Byte]) = if (cache == null) compile(pattern) else cache

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val regex = pattern(input2.asInstanceOf[UTF8String].getBytes)
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].getBytes)
    }
  }

  override def sql: String = s"${left.sql} ${prettyName.toUpperCase(Locale.ROOT)} ${right.sql}"
}

// scalastyle:off line.contains.tab
/**
 * Simple RegEx pattern matching function
 */
@ExpressionDescription(
  usage = "str _FUNC_ pattern[ ESCAPE escape] - Returns true if str matches `pattern` with " +
    "`escape`, null if any arguments are null, false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * pattern - a string expression. The pattern is a string which is matched literally, with
          exception to the following special symbols:
          exception to the following special symbols:<br><br>
            _ matches any one character in the input (similar to . in posix regular expressions)
          % matches zero or more characters in the input (similar to .* in posix regular
          expressions)
          expressions)<br><br>
          Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
          to match "\abc", the pattern should be "\\abc".
          When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it fallbacks
          to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
          enabled, the pattern to match "\abc" should be "\abc".
      * escape - an character added since Spark 3.0. The default escape character is the '\'.
          If an escape character precedes a special symbol or another escape character, the
          following character is matched literally. It is invalid to escape any other character.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', '_park');
      true
      > SET spark.sql.parser.escapedStringLiterals=true;
      spark.sql.parser.escapedStringLiterals	true
      > SELECT '%SystemDrive%\Users\John' _FUNC_ '\%SystemDrive\%\\Users%';
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT '%SystemDrive%\\Users\\John' _FUNC_ '\%SystemDrive\%\\\\Users%';
      true
      > SELECT '%SystemDrive%/Users/John' _FUNC_ '/%SystemDrive/%//Users%' ESCAPE '/';
      true
  """,
  note = """
    Use RLIKE to match with standard regular expressions.
  """,
  since = "1.0.0")
// scalastyle:on line.contains.tab
case class LikeJoni(left: Expression, right: Expression, escapeChar: Char)
  extends StringRegexExpressionJoni {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def escape(v: Array[Byte]): Array[Byte] = StringUtils.escapeLikeJoniRegex(v, escapeChar)

  override def matches(regex: Regex, input: Array[Byte]): Boolean = {
    regex.matcher(input).`match`(0, input.length, Option.DEFAULT) == input.size
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(LIKE_FAMLIY)

  override def toString: String = escapeChar match {
    case '\\' => s"$left LIKEJoni $right"
    case c => s"$left LIKEJoni $right ESCAPE '$c'"
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val regexClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val encodingClass = classOf[UTF8Encoding].getName
    val syntaxClass = classOf[Syntax].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeJoniRegex"
    val regex = ctx.freshName("regex")
    val patternName = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val tmp =
          StringEscapeUtils.escapeJava(
            new String(escape(rVal.asInstanceOf[UTF8String].getBytes), "utf-8"))
        val pattern = ctx.addMutableState(regexClass, regex,
          v => s"""
                      byte[] $patternName = UTF8String.fromString("${tmp}").getBytes();
                      $v = new $regexClass($patternName, 0, $patternName.length, $optionClass.NONE,
                        ${encodingClass}.INSTANCE, ${syntaxClass}.Java);
                    """.stripMargin)

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            byte[] input = ${eval.value}.getBytes();
            ${ev.value} =
              ${pattern}.matcher(input)
                .match(0, input.length, ${optionClass}.DEFAULT) == input.length;
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val pattern = ctx.freshName("pattern")
      // val rightStr = ctx.freshName("rightStr")
      // We need to escape the escapeChar to make sure the generated code is valid.
      // Otherwise we'll hit org.codehaus.commons.compiler.CompileException.
      val escapedEscapeChar = StringEscapeUtils.escapeJava(escapeChar.toString)
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          byte[] $pattern = $escapeFunc(${eval2}.getBytes(), '${escapedEscapeChar}');
          ${regexClass} $regex = new ${regexClass}($pattern, 0, $pattern.length, $optionClass.NONE,
          ${encodingClass}.INSTANCE, ${syntaxClass}.Java);
          byte[] input = ${eval1}.getBytes();
          ${ev.value} =
            $regex.matcher(input).match(0, input.length, ${optionClass}.DEFAULT) == input.length;
        """
      })
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): LikeJoni =
    copy(left = newLeft, right = newRight)
}

sealed abstract class MultiLikeJoniBase
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Predicate {

  protected def patterns: Seq[UTF8String]

  protected def isNotSpecified: Boolean

  override def inputTypes: Seq[DataType] = StringType :: Nil

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(LIKE_FAMLIY)

  protected lazy val hasNull: Boolean = patterns.contains(null)

  protected lazy val cache: Seq[Regex] = patterns.filterNot(_ == null)
    .map(s => {
      val escapedPattern = StringUtils.escapeLikeJoniRegex(s.getBytes, '\\')
      new Regex(escapedPattern, 0, escapedPattern.length,
        Option.NONE, UTF8Encoding.INSTANCE, Syntax.Java)
    })

  protected lazy val matchFunc = if (isNotSpecified) {
    (p: Regex, inputValue: Array[Byte]) =>
      p.matcher(inputValue).`match`(0, inputValue.length, Option.DEFAULT) != inputValue.length
  } else {
    (p: Regex, inputValue: Array[Byte]) =>
      p.matcher(inputValue).`match`(0, inputValue.length, Option.DEFAULT) == inputValue.length
  }

  protected def matches(exprValue: Array[Byte]): Any

  override def eval(input: InternalRow): Any = {
    val exprValue = child.eval(input)
    if (exprValue == null) {
      null
    } else {
      matches(exprValue.toString.getBytes())
    }
  }
}

/**
 * Optimized version of LIKE ALL, when all pattern values are literal.
 */
sealed abstract class LikeAllJoniBase extends MultiLikeJoniBase {

  override def matches(exprValue: Array[Byte]): Any = {
    if (cache.forall(matchFunc(_, exprValue))) {
      if (hasNull) null else true
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val patternClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val javaDataType = CodeGenerator.javaType(child.dataType)
    val pattern = ctx.freshName("pattern")
    val valueArg = ctx.freshName("valueArg")
    val patternCache = ctx.addReferenceObj("patternCache", cache.asJava)

    val checkNotMatchCode = if (isNotSpecified) {
      s"$pattern.matcher($valueArg).match(0, ${valueArg}.length, ${optionClass}.DEFAULT)" +
        s" == ${valueArg}.length"
    } else {
      s"$pattern.matcher($valueArg).match(0, ${valueArg}.length, ${optionClass}.DEFAULT)" +
        s" != ${valueArg}.length"
    }

    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.isNull} = false;
            |boolean ${ev.value} = true;
            |if (${eval.isNull}) {
            |  ${ev.isNull} = true;
            |} else {
            |  byte[] $valueArg = ${eval.value}.getBytes();
            |  for ($patternClass $pattern: $patternCache) {
            |    if ($checkNotMatchCode) {
            |      ${ev.value} = false;
            |      break;
            |    }
            |  }
            |  if (${ev.value} && $hasNull) ${ev.isNull} = true;
            |}
      """.stripMargin)
  }
}

case class LikeAllJoni(child: Expression, patterns: Seq[UTF8String]) extends LikeAllJoniBase {
  override def isNotSpecified: Boolean = false
  override protected def withNewChildInternal(newChild: Expression): LikeAllJoni =
    copy(child = newChild)
}

case class NotLikeAllJoni(child: Expression, patterns: Seq[UTF8String]) extends LikeAllJoniBase {
  override def isNotSpecified: Boolean = true
  override protected def withNewChildInternal(newChild: Expression): NotLikeAllJoni =
    copy(child = newChild)
}

/**
 * Optimized version of LIKE ANY, when all pattern values are literal.
 */
sealed abstract class LikeAnyJoniBase extends MultiLikeJoniBase {

  override def matches(exprValue: Array[Byte]): Any = {
    if (cache.exists(matchFunc(_, exprValue))) {
      true
    } else {
      if (hasNull) null else false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val patternClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val javaDataType = CodeGenerator.javaType(child.dataType)
    val pattern = ctx.freshName("pattern")
    val valueArg = ctx.freshName("valueArg")
    val patternCache = ctx.addReferenceObj("patternCache", cache.asJava)

    val checkMatchCode = if (isNotSpecified) {
      s"$pattern.matcher($valueArg).match(0, ${valueArg}.length, ${optionClass}.DEFAULT)" +
        s" != ${valueArg}.length"
    } else {
      s"$pattern.matcher($valueArg).match(0, ${valueArg}.length, ${optionClass}.DEFAULT)" +
        s" == ${valueArg}.length"
    }

    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.isNull} = false;
            |boolean ${ev.value} = false;
            |if (${eval.isNull}) {
            |  ${ev.isNull} = true;
            |} else {
            |  byte[] $valueArg = ${eval.value}.getBytes();
            |  for ($patternClass $pattern: $patternCache) {
            |    if ($checkMatchCode) {
            |      ${ev.value} = true;
            |      break;
            |    }
            |  }
            |  if (!${ev.value} && $hasNull) ${ev.isNull} = true;
            |}
      """.stripMargin)
  }
}

case class LikeAnyJoni(child: Expression, patterns: Seq[UTF8String]) extends LikeAnyJoniBase {
  override def isNotSpecified: Boolean = false
  override protected def withNewChildInternal(newChild: Expression): LikeAnyJoni =
    copy(child = newChild)
}

case class NotLikeAnyJoni(child: Expression, patterns: Seq[UTF8String]) extends LikeAnyJoniBase {
  override def isNotSpecified: Boolean = true
  override protected def withNewChildInternal(newChild: Expression): NotLikeAnyJoni =
    copy(child = newChild)
}


// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "str _FUNC_ regexp - Returns true if `str` matches `regexp`, or false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * regexp - a string expression. The regex string should be a Java regular expression.

          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".

          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
  """,
  examples = """
    Examples:
      > SET spark.sql.parser.escapedStringLiterals=true;
      spark.sql.parser.escapedStringLiterals	true
      > SELECT '%SystemDrive%\Users\John' _FUNC_ '%SystemDrive%\\Users.*';
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT '%SystemDrive%\\Users\\John' _FUNC_ '%SystemDrive%\\\\Users.*';
      true
  """,
  note = """
    Use LIKE to match with simple string pattern.
  """,
  since = "1.0.0")
// scalastyle:on line.contains.tab
case class RLikeJoni(left: Expression, right: Expression) extends StringRegexExpressionJoni {

  override def escape(v: Array[Byte]): Array[Byte] = v
  override def matches(regex: Regex, input: Array[Byte]): Boolean = {
    regex.matcher(input).search(0, input.length, Option.DEFAULT) > -1
  }
  override def toString: String = s"$left RLIKE_JONI $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val regexClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val encodingClass = classOf[UTF8Encoding].getName
    val syntaxClass = classOf[Syntax].getName
    val regex = ctx.freshName("regex")
    val patternName = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val tmp =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        val pattern = ctx.addMutableState(regexClass, regex,
          v => s"""
                    byte[] $patternName = UTF8String.fromString("${tmp}").getBytes();
                    $v = new $regexClass($patternName, 0, $patternName.length, $optionClass.NONE,
                      $encodingClass.INSTANCE, $syntaxClass.Java);
                  """.stripMargin)

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            byte[] input = ${eval.value}.getBytes();
            ${ev.value} =
              $pattern.matcher(input).search(0, input.length, ${optionClass}.DEFAULT) > -1;
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val pattern = ctx.freshName("pattern")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          byte[] $pattern = ${eval2}.getBytes();
          ${regexClass} $regex = new ${regexClass}($pattern, 0, $pattern.length, $optionClass.NONE,
          ${encodingClass}.INSTANCE, ${syntaxClass}.Java);
          byte[] input = ${eval1}.getBytes();
          ${ev.value} =
            $regex.matcher(input).search(0, input.length, ${optionClass}.DEFAULT) > -1;
        """
      })
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): RLikeJoni =
    copy(left = newLeft, right = newRight)
}

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
import java.util.regex.{Matcher, MatchResult, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIKE_FAMLIY, REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE, TreePattern}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, StringUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


abstract class StringRegexExpression extends BinaryExpression
  with ImplicitCastInputTypes with NullIntolerant {

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache foldable pattern
  private lazy val cache: Pattern = right match {
    case p: Expression if p.foldable =>
      compile(p.eval().asInstanceOf[UTF8String].toString)
    case _ => null
  }

  protected def compile(str: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if (cache == null) compile(str) else cache

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val regex = pattern(input2.asInstanceOf[UTF8String].toString)
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].toString)
    }
  }
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
          exception to the following special symbols:<br><br>
          _ matches any one character in the input (similar to . in posix regular expressions)\
          % matches zero or more characters in the input (similar to .* in posix regular
          expressions)<br><br>
          Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
          to match "\abc", the pattern should be "\\abc".<br><br>
          When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it falls back
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
  since = "1.0.0",
  group = "predicate_funcs")
// scalastyle:on line.contains.tab
case class Like(left: Expression, right: Expression, escapeChar: Char)
  extends StringRegexExpression {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v, escapeChar)

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  final override val nodePatterns: Seq[TreePattern] = Seq(LIKE_FAMLIY)

  override def toString: String = escapeChar match {
    case '\\' => s"$left LIKE $right"
    case c => s"$left LIKE $right ESCAPE '$c'"
  }

  override def sql: String = s"${left.sql} ${prettyName.toUpperCase(Locale.ROOT)} ${right.sql}"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        val pattern = ctx.addMutableState(patternClass, "patternLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).matches();
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
      val rightStr = ctx.freshName("rightStr")
      // We need to escape the escapeChar to make sure the generated code is valid.
      // Otherwise we'll hit org.codehaus.commons.compiler.CompileException.
      val escapedEscapeChar = StringEscapeUtils.escapeJava(escapeChar.toString)
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile(
            $escapeFunc($rightStr, '$escapedEscapeChar'));
          ${ev.value} = $pattern.matcher($eval1.toString()).matches();
        """
      })
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Like =
    copy(left = newLeft, right = newRight)
}

// scalastyle:off line.contains.tab line.size.limit
/**
 * Simple RegEx case-insensitive pattern matching function
 */
@ExpressionDescription(
  usage = "str _FUNC_ pattern[ ESCAPE escape] - Returns true if str matches `pattern` with " +
    "`escape` case-insensitively, null if any arguments are null, false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * pattern - a string expression. The pattern is a string which is matched literally and
          case-insensitively, with exception to the following special symbols:<br><br>
          _ matches any one character in the input (similar to . in posix regular expressions)<br><br>
          % matches zero or more characters in the input (similar to .* in posix regular
          expressions)<br><br>
          Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
          to match "\abc", the pattern should be "\\abc".<br><br>
          When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it falls back
          to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
          enabled, the pattern to match "\abc" should be "\abc".
      * escape - an character added since Spark 3.0. The default escape character is the '\'.
          If an escape character precedes a special symbol or another escape character, the
          following character is matched literally. It is invalid to escape any other character.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', '_Park');
      true
      > SET spark.sql.parser.escapedStringLiterals=true;
      spark.sql.parser.escapedStringLiterals	true
      > SELECT '%SystemDrive%\Users\John' _FUNC_ '\%SystemDrive\%\\users%';
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT '%SystemDrive%\\USERS\\John' _FUNC_ '\%SystemDrive\%\\\\Users%';
      true
      > SELECT '%SystemDrive%/Users/John' _FUNC_ '/%SYSTEMDrive/%//Users%' ESCAPE '/';
      true
  """,
  note = """
    Use RLIKE to match with standard regular expressions.
  """,
  since = "3.3.0",
  group = "predicate_funcs")
// scalastyle:on line.contains.tab line.size.limit
case class ILike(
    left: Expression,
    right: Expression,
    escapeChar: Char) extends RuntimeReplaceable
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  override lazy val replacement: Expression = Like(Lower(left), Lower(right), escapeChar)

  def this(left: Expression, right: Expression) =
    this(left, right, '\\')

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

sealed abstract class MultiLikeBase
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  protected def patterns: Seq[UTF8String]

  protected def isNotSpecified: Boolean

  override def inputTypes: Seq[DataType] = StringType :: Nil

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(LIKE_FAMLIY)

  protected lazy val hasNull: Boolean = patterns.contains(null)

  protected lazy val cache = patterns.filterNot(_ == null)
    .map(s => Pattern.compile(StringUtils.escapeLikeRegex(s.toString, '\\')))

  protected lazy val matchFunc = if (isNotSpecified) {
    (p: Pattern, inputValue: String) => !p.matcher(inputValue).matches()
  } else {
    (p: Pattern, inputValue: String) => p.matcher(inputValue).matches()
  }

  protected def matches(exprValue: String): Any

  override def eval(input: InternalRow): Any = {
    val exprValue = child.eval(input)
    if (exprValue == null) {
      null
    } else {
      matches(exprValue.toString)
    }
  }
}

/**
 * Optimized version of LIKE ALL, when all pattern values are literal.
 */
sealed abstract class LikeAllBase extends MultiLikeBase {

  override def matches(exprValue: String): Any = {
    if (cache.forall(matchFunc(_, exprValue))) {
      if (hasNull) null else true
    } else {
      false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val patternClass = classOf[Pattern].getName
    val javaDataType = CodeGenerator.javaType(child.dataType)
    val pattern = ctx.freshName("pattern")
    val valueArg = ctx.freshName("valueArg")
    val patternCache = ctx.addReferenceObj("patternCache", cache.asJava)

    val checkNotMatchCode = if (isNotSpecified) {
      s"$pattern.matcher($valueArg.toString()).matches()"
    } else {
      s"!$pattern.matcher($valueArg.toString()).matches()"
    }

    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.isNull} = false;
            |boolean ${ev.value} = true;
            |if (${eval.isNull}) {
            |  ${ev.isNull} = true;
            |} else {
            |  $javaDataType $valueArg = ${eval.value};
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

case class LikeAll(child: Expression, patterns: Seq[UTF8String]) extends LikeAllBase {
  override def isNotSpecified: Boolean = false
  override protected def withNewChildInternal(newChild: Expression): LikeAll =
    copy(child = newChild)
}

case class NotLikeAll(child: Expression, patterns: Seq[UTF8String]) extends LikeAllBase {
  override def isNotSpecified: Boolean = true
  override protected def withNewChildInternal(newChild: Expression): NotLikeAll =
    copy(child = newChild)
}

/**
 * Optimized version of LIKE ANY, when all pattern values are literal.
 */
sealed abstract class LikeAnyBase extends MultiLikeBase {

  override def matches(exprValue: String): Any = {
    if (cache.exists(matchFunc(_, exprValue))) {
      true
    } else {
      if (hasNull) null else false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val patternClass = classOf[Pattern].getName
    val javaDataType = CodeGenerator.javaType(child.dataType)
    val pattern = ctx.freshName("pattern")
    val valueArg = ctx.freshName("valueArg")
    val patternCache = ctx.addReferenceObj("patternCache", cache.asJava)

    val checkMatchCode = if (isNotSpecified) {
      s"!$pattern.matcher($valueArg.toString()).matches()"
    } else {
      s"$pattern.matcher($valueArg.toString()).matches()"
    }

    ev.copy(code =
      code"""
            |${eval.code}
            |boolean ${ev.isNull} = false;
            |boolean ${ev.value} = false;
            |if (${eval.isNull}) {
            |  ${ev.isNull} = true;
            |} else {
            |  $javaDataType $valueArg = ${eval.value};
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

case class LikeAny(child: Expression, patterns: Seq[UTF8String]) extends LikeAnyBase {
  override def isNotSpecified: Boolean = false
  override protected def withNewChildInternal(newChild: Expression): LikeAny =
    copy(child = newChild)
}

case class NotLikeAny(child: Expression, patterns: Seq[UTF8String]) extends LikeAnyBase {
  override def isNotSpecified: Boolean = true
  override protected def withNewChildInternal(newChild: Expression): NotLikeAny =
    copy(child = newChild)
}

// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(str, regexp) - Returns true if `str` matches `regexp`, or false otherwise.",
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
      > SELECT _FUNC_('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*');
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT _FUNC_('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*');
      true
  """,
  note = """
    Use LIKE to match with simple string pattern.
  """,
  since = "1.0.0",
  group = "predicate_funcs")
// scalastyle:on line.contains.tab
case class RLike(left: Expression, right: Expression) extends StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"RLIKE($left, $right)"
  override def sql: String = s"${prettyName.toUpperCase(Locale.ROOT)}(${left.sql}, ${right.sql})"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        val pattern = ctx.addMutableState(patternClass, "patternRLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).find(0);
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val rightStr = ctx.freshName("rightStr")
      val pattern = ctx.freshName("pattern")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile($rightStr);
          ${ev.value} = $pattern.matcher($eval1.toString()).find(0);
        """
      })
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): RLike =
    copy(left = newLeft, right = newRight)
}


/**
 * Splits str around matches of the given regex.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regex, limit) - Splits `str` around occurrences that match `regex`" +
    " and returns an array with a length of at most `limit`",
  arguments = """
    Arguments:
      * str - a string expression to split.
      * regex - a string representing a regular expression. The regex string should be a
        Java regular expression.
      * limit - an integer expression which controls the number of times the regex is applied.
          * limit > 0: The resulting array's length will not be more than `limit`,
            and the resulting array's last entry will contain all input
            beyond the last matched regex.
          * limit <= 0: `regex` will be applied as many times as possible, and
            the resulting array can be of any size.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]');
       ["one","two","three",""]
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]', -1);
       ["one","two","three",""]
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]', 2);
       ["one","twoBthreeC"]
  """,
  since = "1.5.0",
  group = "string_funcs")
case class StringSplit(str: Expression, regex: Expression, limit: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = ArrayType(StringType, containsNull = false)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = str
  override def second: Expression = regex
  override def third: Expression = limit

  def this(exp: Expression, regex: Expression) = this(exp, regex, Literal(-1));

  override def nullSafeEval(string: Any, regex: Any, limit: Any): Any = {
    val strings = string.asInstanceOf[UTF8String].split(
      regex.asInstanceOf[UTF8String], limit.asInstanceOf[Int])
    new GenericArrayData(strings.asInstanceOf[Array[Any]])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, regex, limit) => {
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"""${ev.value} = new $arrayClass($str.split($regex,$limit));""".stripMargin
    })
  }

  override def prettyName: String = "split"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringSplit =
    copy(str = newFirst, regex = newSecond, limit = newThird)
}


/**
 * Replace all substrings of str that match regexp with rep.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, regexp, rep[, position]) - Replaces all substrings of `str` that match `regexp` with `rep`.",
  arguments = """
    Arguments:
      * str - a string expression to search for a regular expression pattern match.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.<br><br>
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".<br><br>
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
      * rep - a string expression to replace matched substrings.
      * position - a positive integer literal that indicates the position within `str` to begin searching.
          The default is 1. If position is greater than the number of characters in `str`, the result is `str`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('100-200', '(\\d+)', 'num');
       num-num
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression, pos: Expression)
  extends QuaternaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(subject: Expression, regexp: Expression, rep: Expression) =
    this(subject, regexp, rep, Literal(1))

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    if (!pos.foldable) {
      return TypeCheckFailure(s"Position expression must be foldable, but got $pos")
    }

    val posEval = pos.eval()
    if (posEval == null || posEval.asInstanceOf[Int] > 0) {
      TypeCheckSuccess
    } else {
      TypeCheckFailure(s"Position expression must be positive, but got: $posEval")
    }
  }

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacement: String = _
  @transient private var lastReplacementInUTF8: UTF8String = _
  // result buffer write by Matcher
  @transient private lazy val result: StringBuffer = new StringBuffer
  final override val nodePatterns: Seq[TreePattern] = Seq(REGEXP_REPLACE)

  override def nullSafeEval(s: Any, p: Any, r: Any, i: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    if (!r.equals(lastReplacementInUTF8)) {
      // replacement string changed
      lastReplacementInUTF8 = r.asInstanceOf[UTF8String].clone()
      lastReplacement = lastReplacementInUTF8.toString
    }
    val source = s.toString()
    val position = i.asInstanceOf[Int] - 1
    if (position == 0 || position < source.length) {
      val m = pattern.matcher(source)
      m.region(position, source.length)
      result.delete(0, result.length())
      while (m.find) {
        m.appendReplacement(result, lastReplacement)
      }
      m.appendTail(result)
      UTF8String.fromString(result.toString)
    } else {
      s
    }
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, StringType, IntegerType)
  override def prettyName: String = "regexp_replace"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val termResult = ctx.freshName("termResult")

    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameStringBuffer = classOf[java.lang.StringBuffer].getCanonicalName

    val matcher = ctx.freshName("matcher")
    val source = ctx.freshName("source")
    val position = ctx.freshName("position")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")
    val termLastReplacement = ctx.addMutableState("String", "lastReplacement")
    val termLastReplacementInUTF8 = ctx.addMutableState("UTF8String", "lastReplacementInUTF8")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep, pos) => {
    s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp.clone();
        $termPattern = $classNamePattern.compile($termLastRegex.toString());
      }
      if (!$rep.equals($termLastReplacementInUTF8)) {
        // replacement string changed
        $termLastReplacementInUTF8 = $rep.clone();
        $termLastReplacement = $termLastReplacementInUTF8.toString();
      }
      String $source = $subject.toString();
      int $position = $pos - 1;
      if ($position == 0 || $position < $source.length()) {
        $classNameStringBuffer $termResult = new $classNameStringBuffer();
        java.util.regex.Matcher $matcher = $termPattern.matcher($source);
        $matcher.region($position, $source.length());

        while ($matcher.find()) {
          $matcher.appendReplacement($termResult, $termLastReplacement);
        }
        $matcher.appendTail($termResult);
        ${ev.value} = UTF8String.fromString($termResult.toString());
        $termResult = null;
      } else {
        ${ev.value} = $subject;
      }
      $setEvNotNull
    """
    })
  }

  override def first: Expression = subject
  override def second: Expression = regexp
  override def third: Expression = rep
  override def fourth: Expression = pos

  override protected def withNewChildrenInternal(
      first: Expression, second: Expression, third: Expression, fourth: Expression): RegExpReplace =
    copy(subject = first, regexp = second, rep = third, pos = fourth)
}

object RegExpReplace {
  def apply(subject: Expression, regexp: Expression, rep: Expression): RegExpReplace =
    new RegExpReplace(subject, regexp, rep)
}

object RegExpExtractBase {
  def checkGroupIndex(groupCount: Int, groupIndex: Int): Unit = {
    if (groupIndex < 0) {
      throw QueryExecutionErrors.regexGroupIndexLessThanZeroError
    } else if (groupCount < groupIndex) {
      throw QueryExecutionErrors.regexGroupIndexExceedGroupCountError(
        groupCount, groupIndex)
    }
  }
}

abstract class RegExpExtractBase
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {
  def subject: Expression
  def regexp: Expression
  def idx: Expression

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  final override val nodePatterns: Seq[TreePattern] = Seq(REGEXP_EXTRACT_FAMILY)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = subject
  override def second: Expression = regexp
  override def third: Expression = idx

  protected def getLastMatcher(s: Any, p: Any): Matcher = {
    if (p != lastRegex) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    pattern.matcher(s.toString)
  }
}

/**
 * Extract a specific(idx) group identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp[, idx]) - Extract the first string in the `str` that match the `regexp`
    expression and corresponding to the regex group index.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.<br><br>
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".<br><br>
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
      * idx - an integer expression that representing the group index. The regex maybe contains
          multiple groups. `idx` indicates which regex group to extract. The group index should
          be non-negative. The minimum value of `idx` is 0, which means matching the entire
          regular expression. If `idx` is not specified, the default group index value is 1. The
          `idx` parameter is the Java regex Matcher group() method index.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('100-200', '(\\d+)-(\\d+)', 1);
       100
  """,
  since = "1.5.0",
  group = "string_funcs")
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    val m = getLastMatcher(s, p)
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      val index = r.asInstanceOf[Int]
      RegExpExtractBase.checkGroupIndex(mr.groupCount, index)
      val group = mr.group(index)
      if (group == null) { // Pattern matched, but it's an optional group
        UTF8String.EMPTY_UTF8
      } else {
        UTF8String.fromString(group)
      }
    } else {
      UTF8String.EMPTY_UTF8
    }
  }

  override def dataType: DataType = StringType
  override def prettyName: String = "regexp_extract"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameRegExpExtractBase = classOf[RegExpExtractBase].getCanonicalName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp.clone();
        $termPattern = $classNamePattern.compile($termLastRegex.toString());
      }
      java.util.regex.Matcher $matcher =
        $termPattern.matcher($subject.toString());
      if ($matcher.find()) {
        java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
        $classNameRegExpExtractBase.checkGroupIndex($matchResult.groupCount(), $idx);
        if ($matchResult.group($idx) == null) {
          ${ev.value} = UTF8String.EMPTY_UTF8;
        } else {
          ${ev.value} = UTF8String.fromString($matchResult.group($idx));
        }
        $setEvNotNull
      } else {
        ${ev.value} = UTF8String.EMPTY_UTF8;
        $setEvNotNull
      }"""
    })
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): RegExpExtract =
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
}

/**
 * Extract all specific(idx) groups identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp[, idx]) - Extract all strings in the `str` that match the `regexp`
    expression and corresponding to the regex group index.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.<br><br>
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".<br><br>
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
      * idx - an integer expression that representing the group index. The regex may contains
          multiple groups. `idx` indicates which regex group to extract. The group index should
          be non-negative. The minimum value of `idx` is 0, which means matching the entire
          regular expression. If `idx` is not specified, the default group index value is 1. The
          `idx` parameter is the Java regex Matcher group() method index.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('100-200, 300-400', '(\\d+)-(\\d+)', 1);
       ["100","300"]
  """,
  since = "3.1.0",
  group = "string_funcs")
case class RegExpExtractAll(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    val m = getLastMatcher(s, p)
    val matchResults = new ArrayBuffer[UTF8String]()
    while(m.find) {
      val mr: MatchResult = m.toMatchResult
      val index = r.asInstanceOf[Int]
      RegExpExtractBase.checkGroupIndex(mr.groupCount, index)
      val group = mr.group(index)
      if (group == null) { // Pattern matched, but it's an optional group
        matchResults += UTF8String.EMPTY_UTF8
      } else {
        matchResults += UTF8String.fromString(group)
      }
    }

    new GenericArrayData(matchResults.toArray.asInstanceOf[Array[Any]])
  }

  override def dataType: DataType = ArrayType(StringType)
  override def prettyName: String = "regexp_extract_all"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameRegExpExtractBase = classOf[RegExpExtractBase].getCanonicalName
    val arrayClass = classOf[GenericArrayData].getName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")
    val matchResults = ctx.freshName("matchResults")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }
    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
         | if (!$regexp.equals($termLastRegex)) {
         |   // regex value changed
         |   $termLastRegex = $regexp.clone();
         |   $termPattern = $classNamePattern.compile($termLastRegex.toString());
         | }
         | java.util.regex.Matcher $matcher = $termPattern.matcher($subject.toString());
         | java.util.ArrayList $matchResults = new java.util.ArrayList<UTF8String>();
         | while ($matcher.find()) {
         |   java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
         |   $classNameRegExpExtractBase.checkGroupIndex($matchResult.groupCount(), $idx);
         |   if ($matchResult.group($idx) == null) {
         |     $matchResults.add(UTF8String.EMPTY_UTF8);
         |   } else {
         |     $matchResults.add(UTF8String.fromString($matchResult.group($idx)));
         |   }
         | }
         | ${ev.value} =
         |   new $arrayClass($matchResults.toArray(new UTF8String[$matchResults.size()]));
         | $setEvNotNull
         """
    })
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): RegExpExtractAll =
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp) - Returns a count of the number of times that the regular expression pattern `regexp` is matched in the string `str`.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en');
       2
      > SELECT _FUNC_('abcdefghijklmnopqrstuvwxyz', '[a-z]{3}');
       8
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class RegExpCount(left: Expression, right: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression =
    Size(RegExpExtractAll(left, right, Literal(0)), legacySizeOfNull = false)

  override def prettyName: String = "regexp_count"

  override def children: Seq[Expression] = Seq(left, right)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RegExpCount =
    copy(left = newChildren(0), right = newChildren(1))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp) - Returns the substring that matches the regular expression `regexp` within the string `str`. If the regular expression is not found, the result is null.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a Java regular expression.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en');
       Steven
      > SELECT _FUNC_('Steven Jones and Stephen Smith are the best players', 'Jeck');
       NULL
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class RegExpSubStr(left: Expression, right: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression =
    new NullIf(
      RegExpExtract(subject = left, regexp = right, idx = Literal(0)),
      Literal(""))

  override def prettyName: String = "regexp_substr"

  override def children: Seq[Expression] = Seq(left, right)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RegExpSubStr =
    copy(left = newChildren(0), right = newChildren(1))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, regexp) - Searches a string for a regular expression and returns an integer that indicates the beginning position of the matched substring. Positions are 1-based, not 0-based. If no match is found, returns 0.
  """,
  arguments = """
    Arguments:
      * str - a string expression.
      * regexp - a string representing a regular expression. The regex string should be a
          Java regular expression.<br><br>
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".<br><br>
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('user@spark.apache.org', '@[^.]*');
       5
  """,
  since = "3.4.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class RegExpInStr(subject: Expression, regexp: Expression, idx: Expression)
  extends RegExpExtractBase {
  def this(s: Expression, r: Expression) = this(s, r, Literal(0))

  override def nullSafeEval(s: Any, r: Any, i: Any): Any = {
    try {
      val m = getLastMatcher(s, r)
      if (m.find) {
        m.toMatchResult.start() + 1
      } else {
        0
      }
    } catch {
      case _: IllegalStateException => 0
    }
  }

  override def dataType: DataType = IntegerType
  override def prettyName: String = "regexp_instr"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val matcher = ctx.freshName("matcher")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, _) => {
      s"""
         |try {
         |  $setEvNotNull
         |  if (!$regexp.equals($termLastRegex)) {
         |    // regex value changed
         |    $termLastRegex = $regexp.clone();
         |    $termPattern = $classNamePattern.compile($termLastRegex.toString());
         |  }
         |  java.util.regex.Matcher $matcher = $termPattern.matcher($subject.toString());
         |  if ($matcher.find()) {
         |    ${ev.value} = $matcher.toMatchResult().start() + 1;
         |  } else {
         |    ${ev.value} = 0;
         |  }
         |} catch (IllegalStateException e) {
         |  ${ev.value} = 0;
         |}
         |""".stripMargin
    })
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): RegExpInStr =
    copy(subject = newFirst, regexp = newSecond, idx = newThird)
}

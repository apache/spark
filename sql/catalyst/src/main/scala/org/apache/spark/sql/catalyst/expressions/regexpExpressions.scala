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

import java.util.regex.{MatchResult, Pattern}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{GenericArrayData, StringUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


trait StringRegexExpression extends ImplicitCastInputTypes {
  self: BinaryExpression =>

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
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

  override def sql: String = s"${left.sql} ${prettyName.toUpperCase} ${right.sql}"
}


/**
 * Simple RegEx pattern matching function
 */
@ExpressionDescription(
  usage = "str _FUNC_ pattern - Returns true if str matches pattern and false otherwise.")
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v)

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  override def toString: String = s"$left LIKE $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).matches();
          }
        """)
      } else {
        ev.copy(code = s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """)
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile($escapeFunc(rightStr));
          ${ev.value} = $pattern.matcher(${eval1}.toString()).matches();
        """
      })
    }
  }
}

@ExpressionDescription(
  usage = "str _FUNC_ regexp - Returns true if str matches regexp and false otherwise.")
case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).find(0);
          }
        """)
      } else {
        ev.copy(code = s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        """)
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile(rightStr);
          ${ev.value} = $pattern.matcher(${eval1}.toString()).find(0);
        """
      })
    }
  }
}


/**
 * Splits str around pat (pattern is a regular expression).
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regex) - Splits str around occurrences that match regex",
  extended = "> SELECT _FUNC_('oneAtwoBthreeC', '[ABC]');\n ['one', 'two', 'three']")
case class StringSplit(str: Expression, pattern: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = pattern
  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def nullSafeEval(string: Any, regex: Any): Any = {
    val strings = string.asInstanceOf[UTF8String].split(regex.asInstanceOf[UTF8String], -1)
    new GenericArrayData(strings.asInstanceOf[Array[Any]])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, pattern) =>
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"""${ev.value} = new $arrayClass($str.split($pattern, -1));""")
  }

  override def prettyName: String = "split"
}


/**
 * Replace all substrings of str that match regexp with rep.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regexp, rep) - replace all substrings of str that match regexp with rep.",
  extended = "> SELECT _FUNC_('100-200', '(\\d+)', 'num');\n 'num-num'")
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacement: String = _
  @transient private var lastReplacementInUTF8: UTF8String = _
  // result buffer write by Matcher
  @transient private val result: StringBuffer = new StringBuffer

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
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
    val m = pattern.matcher(s.toString())
    result.delete(0, result.length())

    while (m.find) {
      m.appendReplacement(result, lastReplacement)
    }
    m.appendTail(result)

    UTF8String.fromString(result.toString)
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = subject :: regexp :: rep :: Nil
  override def prettyName: String = "regexp_replace"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val termLastRegex = ctx.freshName("lastRegex")
    val termPattern = ctx.freshName("pattern")

    val termLastReplacement = ctx.freshName("lastReplacement")
    val termLastReplacementInUTF8 = ctx.freshName("lastReplacementInUTF8")

    val termResult = ctx.freshName("result")

    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameStringBuffer = classOf[java.lang.StringBuffer].getCanonicalName

    ctx.addMutableState("UTF8String", termLastRegex, s"${termLastRegex} = null;")
    ctx.addMutableState(classNamePattern, termPattern, s"${termPattern} = null;")
    ctx.addMutableState("String", termLastReplacement, s"${termLastReplacement} = null;")
    ctx.addMutableState("UTF8String",
      termLastReplacementInUTF8, s"${termLastReplacementInUTF8} = null;")
    ctx.addMutableState(classNameStringBuffer,
      termResult, s"${termResult} = new $classNameStringBuffer();")

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep) => {
    s"""
      if (!$regexp.equals(${termLastRegex})) {
        // regex value changed
        ${termLastRegex} = $regexp.clone();
        ${termPattern} = ${classNamePattern}.compile(${termLastRegex}.toString());
      }
      if (!$rep.equals(${termLastReplacementInUTF8})) {
        // replacement string changed
        ${termLastReplacementInUTF8} = $rep.clone();
        ${termLastReplacement} = ${termLastReplacementInUTF8}.toString();
      }
      ${termResult}.delete(0, ${termResult}.length());
      java.util.regex.Matcher m = ${termPattern}.matcher($subject.toString());

      while (m.find()) {
        m.appendReplacement(${termResult}, ${termLastReplacement});
      }
      m.appendTail(${termResult});
      ${ev.value} = UTF8String.fromString(${termResult}.toString());
      ${ev.isNull} = false;
    """
    })
  }
}

/**
 * Extract a specific(idx) group identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regexp[, idx]) - extracts a group that matches regexp.",
  extended = "> SELECT _FUNC_('100-200', '(\\d+)-(\\d+)', 1);\n '100'")
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    val m = pattern.matcher(s.toString)
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      UTF8String.fromString(mr.group(r.asInstanceOf[Int]))
    } else {
      UTF8String.EMPTY_UTF8
    }
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil
  override def prettyName: String = "regexp_extract"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val termLastRegex = ctx.freshName("lastRegex")
    val termPattern = ctx.freshName("pattern")
    val classNamePattern = classOf[Pattern].getCanonicalName

    ctx.addMutableState("UTF8String", termLastRegex, s"${termLastRegex} = null;")
    ctx.addMutableState(classNamePattern, termPattern, s"${termPattern} = null;")

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!$regexp.equals(${termLastRegex})) {
        // regex value changed
        ${termLastRegex} = $regexp.clone();
        ${termPattern} = ${classNamePattern}.compile(${termLastRegex}.toString());
      }
      java.util.regex.Matcher m =
        ${termPattern}.matcher($subject.toString());
      if (m.find()) {
        java.util.regex.MatchResult mr = m.toMatchResult();
        ${ev.value} = UTF8String.fromString(mr.group($idx));
        ${ev.isNull} = false;
      } else {
        ${ev.value} = UTF8String.EMPTY_UTF8;
        ${ev.isNull} = false;
      }"""
    })
  }
}

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

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{RegexUtils, StringUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.jcodings.specific.UTF8Encoding
import org.joni.{Regex, Option, Matcher}


trait StringRegexExpression extends ImplicitCastInputTypes {
  self: BinaryExpression =>

  def escape(v: Array[Byte]): Array[Byte]
  def matches(regex: Regex, input: Array[Byte]): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache the pattern for Literal
  private lazy val cache: Regex = right match {
    case x @ Literal(pattern: Array[Byte], BinaryType) => compile(pattern)
    case _ => null
  }

  protected def compile(pattern: Array[Byte]): Regex = if (pattern == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    val escapedPattern = escape(pattern)
    new Regex(escapedPattern, 0, escapedPattern.length, Option.NONE, UTF8Encoding.INSTANCE)
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
}


/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: Array[Byte]): Array[Byte] = StringUtils.escapeLikeRegex(v)

  override def matches(regex: Regex, input: Array[Byte]): Boolean =
    regex.matcher(input).`match`(0, input.length, Option.DEFAULT) > -1

  override def toString: String = s"$left LIKE $right"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val regexClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val encodingClass = classOf[UTF8Encoding].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"
    val regex = ctx.freshName("regex")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val tmp =
          StringEscapeUtils.escapeJava(
            new String(escape(rVal.asInstanceOf[UTF8String].getBytes), "utf-8"))
        ctx.addMutableState(regexClass, regex,
          s"""
            byte[] pattern = UTF8String.fromString("${tmp}").getBytes();
            $regex = new ${regexClass}(pattern, 0, pattern.length, ${optionClass}.NONE,
              ${encodingClass}.INSTANCE);
          """.stripMargin)

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            byte[] input = ${eval.primitive}.getBytes();
            ${ev.primitive} =
              $regex.matcher(input).match(0, input.length, ${optionClass}.DEFAULT) > -1;
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          byte[] pattern = $escapeFunc(${eval2}.getBytes());
          ${regexClass} $regex = new ${regexClass}(pattern, 0, pattern.length, ${optionClass}.NONE,
          ${encodingClass}.INSTANCE);
          byte[] input = ${eval1}.getBytes();
          ${ev.primitive} =
            $regex.matcher(input).match(0, input.length, ${optionClass}.DEFAULT) > -1;
        """
      })
    }
  }
}


case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: Array[Byte]): Array[Byte] = v
  override def matches(regex: Regex, input: Array[Byte]): Boolean =
    regex.matcher(input).search(0, input.length, Option.DEFAULT) > -1
  override def toString: String = s"$left RLIKE $right"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val regexClass = classOf[Regex].getName
    val optionClass = classOf[Option].getName
    val encodingClass = classOf[UTF8Encoding].getName
    val regex = ctx.freshName("regex")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val tmp =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        ctx.addMutableState(regexClass, regex,
          s"""
            byte[] pattern = UTF8String.fromString("${tmp}").getBytes();
            $regex = new ${regexClass}(pattern, 0, pattern.length, ${optionClass}.NONE,
              ${encodingClass}.INSTANCE);
          """.stripMargin)

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            byte[] input = ${eval.primitive}.getBytes();
            ${ev.primitive} =
              $regex.matcher(input).search(0, input.length, ${optionClass}.DEFAULT) > -1;
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          byte[] pattern = ${eval2}.getBytes();
          ${regexClass} $regex = new ${regexClass}(pattern, 0, pattern.length, ${optionClass}.NONE,
            ${encodingClass}.INSTANCE);
          byte[] input = ${eval1}.getBytes();
          ${ev.primitive} =
            $regex.matcher(input).search(0, input.length, ${optionClass}.DEFAULT) > -1;
        """
      })
    }
  }
}


/**
 * Splits str around pat (pattern is a regular expression).
 */
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

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, pattern) =>
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"""${ev.primitive} = new $arrayClass($str.split($pattern, -1));""")
  }

  override def prettyName: String = "split"
}


/**
 * Replace all substrings of str that match regexp with rep.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  @transient private var lastRegex: Array[Byte] = _
  // last regex in string, we will update the pattern if regexp value changed.
  @transient private var lastRegexInUTF8: UTF8String = _
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacementInUTF8: UTF8String = _
  @transient private var lastReplacement: Array[Byte] = _

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegexInUTF8)) {
      // regex value changed
      lastRegexInUTF8 = p.asInstanceOf[UTF8String].clone()
      lastRegex = lastRegexInUTF8.getBytes
    }
    if (!r.equals(lastReplacementInUTF8)) {
      // replacement string changed
      lastReplacementInUTF8 = r.asInstanceOf[UTF8String].clone()
      lastReplacement = lastReplacementInUTF8.getBytes
    }

    val input = s.asInstanceOf[UTF8String].getBytes
    val result = RegexUtils.replaceAll(input, lastRegex, lastReplacement)
    UTF8String.fromBytes(result)
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = subject :: regexp :: rep :: Nil
  override def prettyName: String = "regexp_replace"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val lastRegexInUTF8 = ctx.freshName("lastRegexInUTF8")
    val lastRegex = ctx.freshName("lastRegex")
    val lastReplacementInUTF8 = ctx.freshName("lastReplacementInUTF8")
    val lastReplacement = ctx.freshName("lastReplacement")

    val replaceAllFunc = RegexUtils.getClass.getName.stripSuffix("$") + ".replaceAll"

    ctx.addMutableState("UTF8String", lastRegexInUTF8, s"${lastRegexInUTF8} = null;")
    ctx.addMutableState("byte[]", lastRegex, s"${lastRegex} = null;")
    ctx.addMutableState("UTF8String", lastReplacementInUTF8, s"${lastReplacementInUTF8} = null;")
    ctx.addMutableState("byte[]", lastReplacement, s"${lastReplacement} = null;")

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep) => {
      s"""
      if (!${regexp}.equals(${lastRegexInUTF8})) {
        // regex value changed
        ${lastRegexInUTF8} = ${regexp}.clone();
        ${lastRegex} = ${lastRegexInUTF8}.getBytes();
      }
      if (!$rep.equals(${lastReplacementInUTF8})) {
        // replacement string changed
        ${lastReplacementInUTF8} = $rep.clone();
        ${lastReplacement} = ${lastReplacementInUTF8}.getBytes();
      }

      byte[] input = ${subject}.getBytes();
      byte[] result = ${replaceAllFunc}(input, ${lastRegex}, ${lastReplacement});
      ${ev.primitive} = UTF8String.fromBytes(result);
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
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegexInUTF8: UTF8String = _
  @transient private var lastRegex: Array[Byte] = _
  // last regex pattern, we cache it for performance concern
  @transient private var regex: Regex = _

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegexInUTF8)) {
      // regex value changed
      lastRegexInUTF8 = p.asInstanceOf[UTF8String].clone()
      lastRegex = lastRegexInUTF8.getBytes
      regex = new Regex(lastRegex, 0, lastRegex.length, Option.NONE, UTF8Encoding.INSTANCE)
    }
    val input = s.asInstanceOf[UTF8String].getBytes
    val m = regex.matcher(input)
    if (m.search(0, input.length, Option.DEFAULT) > -1) {
      UTF8String.fromBytes(RegexUtils.group(m, r.asInstanceOf[Int], input))
    } else {
      UTF8String.EMPTY_UTF8
    }
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil
  override def prettyName: String = "regexp_extract"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val regexClass = classOf[Regex].getName
    val matcherClass = classOf[Matcher].getName
    val optionClass = classOf[Option].getName
    val encodingClass = classOf[UTF8Encoding].getName
    val lastRegexInUTF8 = ctx.freshName("lastRegexInUTF8")
    val lastRegex = ctx.freshName("lastRegex")
    val regex = ctx.freshName("regex")
    val groupFunc = RegexUtils.getClass.getName.stripSuffix("$") + ".group"

    ctx.addMutableState("UTF8String", lastRegexInUTF8, s"${lastRegexInUTF8} = null;")
    ctx.addMutableState("byte[]", lastRegex, s"${lastRegex} = null;")
    ctx.addMutableState(regexClass, regex, s"${regex} = null;")

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!${regexp}.equals(${lastRegexInUTF8})) {
        // regex value changed
        ${lastRegexInUTF8} = ${regexp}.clone();
        ${lastRegex} = ${lastRegexInUTF8}.getBytes();
        ${regex} = new ${regexClass}(${lastRegex}, 0, ${lastRegex}.length, ${optionClass}.NONE,
          ${encodingClass}.INSTANCE);
      }
      byte[] input = ${subject}.getBytes();
      ${matcherClass} m = ${regex}.matcher(input);
      if (m.search(0, input.length, ${optionClass}.DEFAULT) > -1) {
        ${ev.primitive} = UTF8String.fromBytes(${groupFunc}(m, $idx, input));
        ${ev.isNull} = false;
      } else {
        ${ev.primitive} = UTF8String.EMPTY_UTF8;
        ${ev.isNull} = false;
      }"""
    })
  }
}

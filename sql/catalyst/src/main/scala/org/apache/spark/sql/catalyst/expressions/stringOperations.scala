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
import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.codegen._
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
    val regex = pattern(input2.asInstanceOf[UTF8String].toString())
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].toString())
    }
  }
}

/**
 * Simple RegEx pattern matching function
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  override def escape(v: String): String =
    if (!v.isEmpty) {
      "(?s)" + (' ' +: v.init).zip(v).flatMap {
        case (prev, '\\') => ""
        case ('\\', c) =>
          c match {
            case '_' => "_"
            case '%' => "%"
            case _ => Pattern.quote("\\" + c)
          }
        case (prev, c) =>
          c match {
            case '_' => "."
            case '%' => ".*"
            case _ => Pattern.quote(Character.toString(c))
          }
      }.mkString
    } else {
      v
    }

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  override def toString: String = s"$left LIKE $right"
}

case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"
}

trait String2StringExpression extends ImplicitCastInputTypes {
  self: UnaryExpression =>

  def convert(v: UTF8String): UTF8String

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(input: Any): Any =
    convert(input.asInstanceOf[UTF8String])
}

/**
 * A function that converts the characters of a string to uppercase.
 */
case class Upper(child: Expression) extends UnaryExpression with String2StringExpression {

  override def convert(v: UTF8String): UTF8String = v.toUpperCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toUpperCase()")
  }
}

/**
 * A function that converts the characters of a string to lowercase.
 */
case class Lower(child: Expression) extends UnaryExpression with String2StringExpression {

  override def convert(v: UTF8String): UTF8String = v.toLowerCase

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).toLowerCase()")
  }
}

/** A base trait for functions that compare two strings, returning a boolean. */
trait StringComparison extends ImplicitCastInputTypes {
  self: BinaryExpression =>

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[UTF8String], input2.asInstanceOf[UTF8String])

  override def toString: String = s"$nodeName($left, $right)"
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.contains(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).contains($c2)")
  }
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.startsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).startsWith($c2)")
  }
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression)
    extends BinaryExpression with Predicate with StringComparison {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.endsWith(r)
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).endsWith($c2)")
  }
}

/**
 * A function that trim the spaces from both ends for the specified string.
 */
case class StringTrim(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trim()

  override def prettyName: String = "trim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trim()")
  }
}

/**
 * A function that trim the spaces from left end for given string.
 */
case class StringTrimLeft(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimLeft()

  override def prettyName: String = "ltrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trimLeft()")
  }
}

/**
 * A function that trim the spaces from right end for given string.
 */
case class StringTrimRight(child: Expression)
  extends UnaryExpression with String2StringExpression {

  def convert(v: UTF8String): UTF8String = v.trimRight()

  override def prettyName: String = "rtrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).trimRight()")
  }
}

/**
 * A function that returns the position of the first occurrence of substr in the given string.
 * Returns null if either of the arguments are null and
 * returns 0 if substr could not be found in str.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
case class StringInstr(str: Expression, substr: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = substr
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  override def nullSafeEval(string: Any, sub: Any): Any = {
    string.asInstanceOf[UTF8String].indexOf(sub.asInstanceOf[UTF8String], 0) + 1
  }

  override def prettyName: String = "instr"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (l, r) =>
      s"($l).indexOf($r, 0) + 1")
  }
}

/**
 * A function that returns the position of the first occurrence of substr
 * in given string after position pos.
 */
case class StringLocate(substr: Expression, str: Expression, start: Expression)
  extends Expression with ImplicitCastInputTypes {

  def this(substr: Expression, str: Expression) = {
    this(substr, str, Literal(0))
  }

  override def children: Seq[Expression] = substr :: str :: start :: Nil
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = substr.nullable || str.nullable
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def eval(input: InternalRow): Any = {
    val s = start.eval(input)
    if (s == null) {
      // if the start position is null, we need to return 0, (conform to Hive)
      0
    } else {
      val r = substr.eval(input)
      if (r == null) {
        null
      } else {
        val l = str.eval(input)
        if (l == null) {
          null
        } else {
          l.asInstanceOf[UTF8String].indexOf(
            r.asInstanceOf[UTF8String],
            s.asInstanceOf[Int]) + 1
        }
      }
    }
  }

  override def prettyName: String = "locate"
}

/**
 * Returns str, left-padded with pad to a length of len.
 */
case class StringLPad(str: Expression, len: Expression, pad: Expression)
  extends Expression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children.exists(_.nullable)
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def eval(input: InternalRow): Any = {
    val s = str.eval(input)
    if (s == null) {
      null
    } else {
      val l = len.eval(input)
      if (l == null) {
        null
      } else {
        val p = pad.eval(input)
        if (p == null) {
          null
        } else {
          val len = l.asInstanceOf[Int]
          val str = s.asInstanceOf[UTF8String]
          val pad = p.asInstanceOf[UTF8String]

          str.lpad(len, pad)
        }
      }
    }
  }

  override def prettyName: String = "lpad"
}

/**
 * Returns str, right-padded with pad to a length of len.
 */
case class StringRPad(str: Expression, len: Expression, pad: Expression)
  extends Expression with ImplicitCastInputTypes {

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children.exists(_.nullable)
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def eval(input: InternalRow): Any = {
    val s = str.eval(input)
    if (s == null) {
      null
    } else {
      val l = len.eval(input)
      if (l == null) {
        null
      } else {
        val p = pad.eval(input)
        if (p == null) {
          null
        } else {
          val len = l.asInstanceOf[Int]
          val str = s.asInstanceOf[UTF8String]
          val pad = p.asInstanceOf[UTF8String]

          str.rpad(len, pad)
        }
      }
    }
  }

  override def prettyName: String = "rpad"
}

/**
 * Returns the input formatted according do printf-style format strings
 */
case class StringFormat(children: Expression*) extends Expression {

  require(children.length >=1, "printf() should take at least 1 argument")

  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children(0).nullable
  override def dataType: DataType = StringType
  private def format: Expression = children(0)
  private def args: Seq[Expression] = children.tail

  override def eval(input: InternalRow): Any = {
    val pattern = format.eval(input)
    if (pattern == null) {
      null
    } else {
      val sb = new StringBuffer()
      val formatter = new java.util.Formatter(sb, Locale.US)

      val arglist = args.map(_.eval(input).asInstanceOf[AnyRef])
      formatter.format(pattern.asInstanceOf[UTF8String].toString(), arglist: _*)

      UTF8String.fromString(sb.toString)
    }
  }

  override def prettyName: String = "printf"
}

/**
 * Returns the string which repeat the given string value n times.
 */
case class StringRepeat(str: Expression, times: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = times
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType)

  override def nullSafeEval(string: Any, n: Any): Any = {
    string.asInstanceOf[UTF8String].repeat(n.asInstanceOf[Integer])
  }

  override def prettyName: String = "repeat"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, (l, r) => s"($l).repeat($r)")
  }
}

/**
 * Returns the reversed given string.
 */
case class StringReverse(child: Expression) extends UnaryExpression with String2StringExpression {
  override def convert(v: UTF8String): UTF8String = v.reverse()

  override def prettyName: String = "reverse"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).reverse()")
  }
}

/**
 * Returns a n spaces string.
 */
case class StringSpace(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def nullSafeEval(s: Any): Any = {
    val length = s.asInstanceOf[Integer]

    val spaces = new Array[Byte](if (length < 0) 0 else length)
    java.util.Arrays.fill(spaces, ' '.asInstanceOf[Byte])
    UTF8String.fromBytes(spaces)
  }

  override def prettyName: String = "space"
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
    val splits =
      string.asInstanceOf[UTF8String].toString.split(regex.asInstanceOf[UTF8String].toString, -1)
    splits.toSeq.map(UTF8String.fromString)
  }

  override def prettyName: String = "split"
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 */
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends Expression with ImplicitCastInputTypes {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def foldable: Boolean = str.foldable && pos.foldable && len.foldable
  override def nullable: Boolean = str.nullable || pos.nullable || len.nullable

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this, s"Cannot resolve since $children are not resolved")
    }
    if (str.dataType == BinaryType) str.dataType else StringType
  }

  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, IntegerType)

  override def children: Seq[Expression] = str :: pos :: len :: Nil

  @inline
  def slicePos(startPos: Int, sliceLen: Int, length: () => Int): (Int, Int) = {
    // Hive and SQL use one-based indexing for SUBSTR arguments but also accept zero and
    // negative indices for start positions. If a start index i is greater than 0, it
    // refers to element i-1 in the sequence. If a start index i is less than 0, it refers
    // to the -ith element before the end of the sequence. If a start index i is 0, it
    // refers to the first element.

    val start = startPos match {
      case pos if pos > 0 => pos - 1
      case neg if neg < 0 => length() + neg
      case _ => 0
    }

    val end = sliceLen match {
      case max if max == Integer.MAX_VALUE => max
      case x => start + x
    }

    (start, end)
  }

  override def eval(input: InternalRow): Any = {
    val string = str.eval(input)
    val po = pos.eval(input)
    val ln = len.eval(input)

    if ((string == null) || (po == null) || (ln == null)) {
      null
    } else {
      val start = po.asInstanceOf[Int]
      val length = ln.asInstanceOf[Int]
      string match {
        case ba: Array[Byte] =>
          val (st, end) = slicePos(start, length, () => ba.length)
          ba.slice(st, end)
        case s: UTF8String =>
          val (st, end) = slicePos(start, length, () => s.numChars())
          s.substring(st, end)
      }
    }
  }
}

/**
 * A function that return the length of the given string expression.
 */
case class StringLength(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any =
    string.asInstanceOf[UTF8String].numChars

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"($c).numChars()")
  }

  override def prettyName: String = "length"
}

/**
 * A function that return the Levenshtein distance between the two given strings.
 */
case class Levenshtein(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = IntegerType

  protected override def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    leftValue.asInstanceOf[UTF8String].levenshteinDistance(rightValue.asInstanceOf[UTF8String])

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (left, right) =>
      s"${ev.primitive} = $left.levenshteinDistance($right);")
  }
}

/**
 * Returns the numeric value of the first character of str.
 */
case class Ascii(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any = {
    val bytes = string.asInstanceOf[UTF8String].getBytes
    if (bytes.length > 0) {
      bytes(0).asInstanceOf[Int]
    } else {
      0
    }
  }
}

/**
 * Converts the argument from binary to a base 64 string.
 */
case class Base64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(bytes: Any): Any = {
    UTF8String.fromBytes(
      org.apache.commons.codec.binary.Base64.encodeBase64(
        bytes.asInstanceOf[Array[Byte]]))
  }
}

/**
 * Converts the argument from a base 64 string to BINARY.
 */
case class UnBase64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any =
    org.apache.commons.codec.binary.Base64.decodeBase64(string.asInstanceOf[UTF8String].toString)
}

/**
 * Decodes the first argument into a String using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
 */
case class Decode(bin: Expression, charset: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = bin
  override def right: Expression = charset
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val fromCharset = input2.asInstanceOf[UTF8String].toString
    UTF8String.fromString(new String(input1.asInstanceOf[Array[Byte]], fromCharset))
  }
}

/**
 * Encodes the first argument into a BINARY using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
*/
case class Encode(value: Expression, charset: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = value
  override def right: Expression = charset
  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val toCharset = input2.asInstanceOf[UTF8String].toString
    input1.asInstanceOf[UTF8String].toString.getBytes(toCharset)
  }
}


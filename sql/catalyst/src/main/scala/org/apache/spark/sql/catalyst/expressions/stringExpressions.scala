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

import java.net.{URI, URISyntaxException}
import java.text.{BreakIterator, DecimalFormat, DecimalFormatSymbols}
import java.util.{HashMap, Locale, Map => JMap}
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.codec.binary.{Base64 => CommonsBase64}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for string operations.
////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * An expression that concatenates multiple input strings or array of strings into a single string,
 * using a given separator (the first child).
 *
 * Returns null if the separator is null. Otherwise, concat_ws skips all null values.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(sep, [str | array(str)]+) - Returns the concatenation of the strings separated by `sep`.",
  examples = """
    Examples:
      > SELECT _FUNC_(' ', 'Spark', 'SQL');
        Spark SQL
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class ConcatWs(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  require(children.nonEmpty, s"$prettyName requires at least one argument.")

  override def prettyName: String = "concat_ws"

  /** The 1st child (separator) is str, and rest are either str or array of str. */
  override def inputTypes: Seq[AbstractDataType] = {
    val arrayOrStr = TypeCollection(ArrayType(StringType), StringType)
    StringType +: Seq.fill(children.size - 1)(arrayOrStr)
  }

  override def dataType: DataType = StringType

  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val flatInputs = children.flatMap { child =>
      child.eval(input) match {
        case s: UTF8String => Iterator(s)
        case arr: ArrayData => arr.toArray[UTF8String](StringType)
        case null => Iterator(null.asInstanceOf[UTF8String])
      }
    }
    UTF8String.concatWs(flatInputs.head, flatInputs.tail : _*)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (children.forall(_.dataType == StringType)) {
      // All children are strings. In that case we can construct a fixed size array.
      val evals = children.map(_.genCode(ctx))
      val separator = evals.head
      val strings = evals.tail
      val numArgs = strings.length
      val args = ctx.freshName("args")

      val inputs = strings.zipWithIndex.map { case (eval, index) =>
        if (eval.isNull != TrueLiteral) {
          s"""
             ${eval.code}
             if (!${eval.isNull}) {
               $args[$index] = ${eval.value};
             }
           """
        } else {
          ""
        }
      }
      val codes = ctx.splitExpressionsWithCurrentInputs(
          expressions = inputs,
          funcName = "valueConcatWs",
          extraArguments = ("UTF8String[]", args) :: Nil)
      ev.copy(code"""
        UTF8String[] $args = new UTF8String[$numArgs];
        ${separator.code}
        $codes
        UTF8String ${ev.value} = UTF8String.concatWs(${separator.value}, $args);
        boolean ${ev.isNull} = ${ev.value} == null;
      """)
    } else {
      val array = ctx.freshName("array")
      val varargNum = ctx.freshName("varargNum")
      val idxVararg = ctx.freshName("idxInVararg")

      val evals = children.map(_.genCode(ctx))
      val (varargCount, varargBuild) = children.tail.zip(evals.tail).map { case (child, eval) =>
        child.dataType match {
          case StringType =>
            ("", // we count all the StringType arguments num at once below.
             if (eval.isNull == TrueLiteral) {
               ""
             } else {
               s"$array[$idxVararg ++] = ${eval.isNull} ? (UTF8String) null : ${eval.value};"
             })
          case _: ArrayType =>
            val size = ctx.freshName("n")
            if (eval.isNull == TrueLiteral) {
              ("", "")
            } else {
              (s"""
                if (!${eval.isNull}) {
                  $varargNum += ${eval.value}.numElements();
                }
                """,
               s"""
                if (!${eval.isNull}) {
                  final int $size = ${eval.value}.numElements();
                  for (int j = 0; j < $size; j ++) {
                    $array[$idxVararg ++] = ${CodeGenerator.getValue(eval.value, StringType, "j")};
                  }
                }
                """)
            }
        }
      }.unzip

      val codes = ctx.splitExpressionsWithCurrentInputs(evals.map(_.code.toString))

      val varargCounts = ctx.splitExpressionsWithCurrentInputs(
        expressions = varargCount,
        funcName = "varargCountsConcatWs",
        returnType = "int",
        makeSplitFunction = body =>
          s"""
             |int $varargNum = 0;
             |$body
             |return $varargNum;
           """.stripMargin,
        foldFunctions = _.map(funcCall => s"$varargNum += $funcCall;").mkString("\n"))

      val varargBuilds = ctx.splitExpressionsWithCurrentInputs(
        expressions = varargBuild,
        funcName = "varargBuildsConcatWs",
        extraArguments = ("UTF8String []", array) :: ("int", idxVararg) :: Nil,
        returnType = "int",
        makeSplitFunction = body =>
          s"""
             |$body
             |return $idxVararg;
           """.stripMargin,
        foldFunctions = _.map(funcCall => s"$idxVararg = $funcCall;").mkString("\n"))

      ev.copy(
        code"""
        $codes
        int $varargNum = ${children.count(_.dataType == StringType) - 1};
        int $idxVararg = 0;
        $varargCounts
        UTF8String[] $array = new UTF8String[$varargNum];
        $varargBuilds
        UTF8String ${ev.value} = UTF8String.concatWs(${evals.head.value}, $array);
        boolean ${ev.isNull} = ${ev.value} == null;
      """)
    }
  }
}

/**
 * An expression that returns the `n`-th input in given inputs.
 * If all inputs are binary, `elt` returns an output as binary. Otherwise, it returns as string.
 * If any input is null, `elt` returns null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(n, input1, input2, ...) - Returns the `n`-th input, e.g., returns `input2` when `n` is 2.",
  examples = """
    Examples:
      > SELECT _FUNC_(1, 'scala', 'java');
       scala
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit
case class Elt(children: Seq[Expression]) extends Expression {

  private lazy val indexExpr = children.head
  private lazy val inputExprs = children.tail.toArray

  /** This expression is always nullable because it returns null if index is out of range. */
  override def nullable: Boolean = true

  override def dataType: DataType = inputExprs.map(_.dataType).headOption.getOrElse(StringType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 2) {
      TypeCheckResult.TypeCheckFailure("elt function requires at least two arguments")
    } else {
      val (indexType, inputTypes) = (indexExpr.dataType, inputExprs.map(_.dataType))
      if (indexType != IntegerType) {
        return TypeCheckResult.TypeCheckFailure(s"first input to function $prettyName should " +
          s"have ${IntegerType.catalogString}, but it's ${indexType.catalogString}")
      }
      if (inputTypes.exists(tpe => !Seq(StringType, BinaryType).contains(tpe))) {
        return TypeCheckResult.TypeCheckFailure(
          s"input to function $prettyName should have ${StringType.catalogString} or " +
            s"${BinaryType.catalogString}, but it's " +
            inputTypes.map(_.catalogString).mkString("[", ", ", "]"))
      }
      TypeUtils.checkForSameTypeInputExpr(inputTypes, s"function $prettyName")
    }
  }

  override def eval(input: InternalRow): Any = {
    val indexObj = indexExpr.eval(input)
    if (indexObj == null) {
      null
    } else {
      val index = indexObj.asInstanceOf[Int]
      if (index <= 0 || index > inputExprs.length) {
        null
      } else {
        inputExprs(index - 1).eval(input)
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val index = indexExpr.genCode(ctx)
    val inputs = inputExprs.map(_.genCode(ctx))
    val indexVal = ctx.freshName("index")
    val indexMatched = ctx.freshName("eltIndexMatched")

    val inputVal = ctx.addMutableState(CodeGenerator.javaType(dataType), "inputVal")

    val assignInputValue = inputs.zipWithIndex.map { case (eval, index) =>
      s"""
         |if ($indexVal == ${index + 1}) {
         |  ${eval.code}
         |  $inputVal = ${eval.isNull} ? null : ${eval.value};
         |  $indexMatched = true;
         |  continue;
         |}
      """.stripMargin
    }

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = assignInputValue,
      funcName = "eltFunc",
      extraArguments = ("int", indexVal) :: Nil,
      returnType = CodeGenerator.JAVA_BOOLEAN,
      makeSplitFunction = body =>
        s"""
           |${CodeGenerator.JAVA_BOOLEAN} $indexMatched = false;
           |do {
           |  $body
           |} while (false);
           |return $indexMatched;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$indexMatched = $funcCall;
           |if ($indexMatched) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)

    ev.copy(
      code"""
         |${index.code}
         |final int $indexVal = ${index.value};
         |${CodeGenerator.JAVA_BOOLEAN} $indexMatched = false;
         |$inputVal = null;
         |do {
         |  $codes
         |} while (false);
         |final ${CodeGenerator.javaType(dataType)} ${ev.value} = $inputVal;
         |final boolean ${ev.isNull} = ${ev.value} == null;
       """.stripMargin)
  }
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
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns `str` with all characters changed to uppercase.",
  examples = """
    Examples:
      > SELECT _FUNC_('SparkSql');
       SPARKSQL
  """,
  since = "1.0.1")
case class Upper(child: Expression)
  extends UnaryExpression with String2StringExpression {

  // scalastyle:off caselocale
  override def convert(v: UTF8String): UTF8String = v.toUpperCase
  // scalastyle:on caselocale

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"($c).toUpperCase()")
  }
}

/**
 * A function that converts the characters of a string to lowercase.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns `str` with all characters changed to lowercase.",
  examples = """
    Examples:
      > SELECT _FUNC_('SparkSql');
       sparksql
  """,
  since = "1.0.1")
case class Lower(child: Expression) extends UnaryExpression with String2StringExpression {

  // scalastyle:off caselocale
  override def convert(v: UTF8String): UTF8String = v.toLowerCase
  // scalastyle:on caselocale

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"($c).toLowerCase()")
  }
}

/** A base trait for functions that compare two strings, returning a boolean. */
abstract class StringPredicate extends BinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[UTF8String], input2.asInstanceOf[UTF8String])

  override def toString: String = s"$nodeName($left, $right)"
}

/**
 * A function that returns true if the string `left` contains the string `right`.
 */
case class Contains(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.contains(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).contains($c2)")
  }
}

/**
 * A function that returns true if the string `left` starts with the string `right`.
 */
case class StartsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.startsWith(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).startsWith($c2)")
  }
}

/**
 * A function that returns true if the string `left` ends with the string `right`.
 */
case class EndsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = l.endsWith(r)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => s"($c1).endsWith($c2)")
  }
}

/**
 * Replace all occurrences with string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, search[, replace]) - Replaces all occurrences of `search` with `replace`.",
  arguments = """
    Arguments:
      * str - a string expression
      * search - a string expression. If `search` is not found in `str`, `str` is returned unchanged.
      * replace - a string expression. If `replace` is not specified or is an empty string, nothing replaces
          the string that is removed from `str`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('ABCabc', 'abc', 'DEF');
       ABCDEF
  """,
  since = "2.3.0")
// scalastyle:on line.size.limit
case class StringReplace(srcExpr: Expression, searchExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(srcExpr: Expression, searchExpr: Expression) = {
    this(srcExpr, searchExpr, Literal(""))
  }

  override def nullSafeEval(srcEval: Any, searchEval: Any, replaceEval: Any): Any = {
    srcEval.asInstanceOf[UTF8String].replace(
      searchEval.asInstanceOf[UTF8String], replaceEval.asInstanceOf[UTF8String])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (src, search, replace) => {
      s"""${ev.value} = $src.replace($search, $replace);"""
    })
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = srcExpr :: searchExpr :: replaceExpr :: Nil
  override def prettyName: String = "replace"
}

object StringTranslate {

  def buildDict(matchingString: UTF8String, replaceString: UTF8String)
    : JMap[Character, Character] = {
    val matching = matchingString.toString()
    val replace = replaceString.toString()
    val dict = new HashMap[Character, Character]()
    var i = 0
    while (i < matching.length()) {
      val rep = if (i < replace.length()) replace.charAt(i) else '\u0000'
      if (null == dict.get(matching.charAt(i))) {
        dict.put(matching.charAt(i), rep)
      }
      i += 1
    }
    dict
  }
}

/**
 * A function translate any character in the `srcExpr` by a character in `replaceExpr`.
 * The characters in `replaceExpr` is corresponding to the characters in `matchingExpr`.
 * The translate will happen when any character in the string matching with the character
 * in the `matchingExpr`.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(input, from, to) - Translates the `input` string by replacing the characters present in the `from` string with the corresponding characters in the `to` string.",
  examples = """
    Examples:
      > SELECT _FUNC_('AaBbCc', 'abc', '123');
       A1B2C3
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class StringTranslate(srcExpr: Expression, matchingExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  @transient private var lastMatching: UTF8String = _
  @transient private var lastReplace: UTF8String = _
  @transient private var dict: JMap[Character, Character] = _

  override def nullSafeEval(srcEval: Any, matchingEval: Any, replaceEval: Any): Any = {
    if (matchingEval != lastMatching || replaceEval != lastReplace) {
      lastMatching = matchingEval.asInstanceOf[UTF8String].clone()
      lastReplace = replaceEval.asInstanceOf[UTF8String].clone()
      dict = StringTranslate.buildDict(lastMatching, lastReplace)
    }
    srcEval.asInstanceOf[UTF8String].translate(dict)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNameDict = classOf[JMap[Character, Character]].getCanonicalName

    val termLastMatching = ctx.addMutableState("UTF8String", "lastMatching")
    val termLastReplace = ctx.addMutableState("UTF8String", "lastReplace")
    val termDict = ctx.addMutableState(classNameDict, "dict")

    nullSafeCodeGen(ctx, ev, (src, matching, replace) => {
      val check = if (matchingExpr.foldable && replaceExpr.foldable) {
        s"$termDict == null"
      } else {
        s"!$matching.equals($termLastMatching) || !$replace.equals($termLastReplace)"
      }
      s"""if ($check) {
        // Not all of them is literal or matching or replace value changed
        $termLastMatching = $matching.clone();
        $termLastReplace = $replace.clone();
        $termDict = org.apache.spark.sql.catalyst.expressions.StringTranslate
          .buildDict($termLastMatching, $termLastReplace);
      }
      ${ev.value} = $src.translate($termDict);
      """
    })
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = srcExpr :: matchingExpr :: replaceExpr :: Nil
  override def prettyName: String = "translate"
}

/**
 * A function that returns the index (1-based) of the given string (left) in the comma-
 * delimited list (right). Returns 0, if the string wasn't found or if the given
 * string (left) contains a comma.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, str_array) - Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`str_array`).
      Returns 0, if the string was not found or if the given string (`str`) contains a comma.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('ab','abc,b,ab,c,def');
       3
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class FindInSet(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def nullSafeEval(word: Any, set: Any): Any =
    set.asInstanceOf[UTF8String].findInSet(word.asInstanceOf[UTF8String])

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (word, set) =>
      s"${ev.value} = $set.findInSet($word);"
    )
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "find_in_set"
}

trait String2TrimExpression extends Expression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)
}

object StringTrim {
  def apply(str: Expression, trimStr: Expression) : StringTrim = StringTrim(str, Some(trimStr))
  def apply(str: Expression) : StringTrim = StringTrim(str, None)
}

/**
 * A function that takes a character string, removes the leading and trailing characters matching
 * with any character in the trim string, returns the new string.
 * If BOTH and trimStr keywords are not specified, it defaults to remove space character from both
 * ends. The trim function will have one argument, which contains the source string.
 * If BOTH and trimStr keywords are specified, it trims the characters from both ends, and the trim
 * function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: A character string to be trimmed from the source string, if it has multiple characters,
 * the function searches for each character in the source string, removes the characters from the
 * source string until it encounters the first non-match character.
 * BOTH: removes any character from both ends of the source string that matches characters in the
 * trim string.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Removes the leading and trailing space characters from `str`.

    _FUNC_(BOTH FROM str) - Removes the leading and trailing space characters from `str`.

    _FUNC_(LEADING FROM str) - Removes the leading space characters from `str`.

    _FUNC_(TRAILING FROM str) - Removes the trailing space characters from `str`.

    _FUNC_(trimStr, str) - Remove the leading and trailing `trimStr` characters from `str`.

    _FUNC_(trimStr FROM str) - Remove the leading and trailing `trimStr` characters from `str`.

    _FUNC_(BOTH trimStr FROM str) - Remove the leading and trailing `trimStr` characters from `str`.

    _FUNC_(LEADING trimStr FROM str) - Remove the leading `trimStr` characters from `str`.

    _FUNC_(TRAILING trimStr FROM str) - Remove the trailing `trimStr` characters from `str`.
  """,
  arguments = """
    Arguments:
      * str - a string expression
      * trimStr - the trim string characters to trim, the default value is a single space
      * BOTH, FROM - these are keywords to specify trimming string characters from both ends of
          the string
      * LEADING, FROM - these are keywords to specify trimming string characters from the left
          end of the string
      * TRAILING, FROM - these are keywords to specify trimming string characters from the right
          end of the string
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('    SparkSQL   ');
       SparkSQL
      > SELECT _FUNC_(BOTH FROM '    SparkSQL   ');
       SparkSQL
      > SELECT _FUNC_(LEADING FROM '    SparkSQL   ');
       SparkSQL
      > SELECT _FUNC_(TRAILING FROM '    SparkSQL   ');
           SparkSQL
      > SELECT _FUNC_('SL', 'SSparkSQLS');
       parkSQ
      > SELECT _FUNC_('SL' FROM '    SparkSQL   ');
       parkSQ
      > SELECT _FUNC_(BOTH 'SL' FROM 'SSparkSQLS');
       parkSQ
      > SELECT _FUNC_(LEADING 'SL' FROM 'SSparkSQLS');
       parkSQLS
      > SELECT _FUNC_(TRAILING 'SL' FROM 'SSparkSQLS');
       SSparkSQ
  """,
  since = "1.5.0")
case class StringTrim(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(srcStr: Expression, trimStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "trim"

  override def children: Seq[Expression] = if (trimStr.isDefined) {
    srcStr :: trimStr.get :: Nil
  } else {
    srcStr :: Nil
  }
  override def eval(input: InternalRow): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[UTF8String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        srcString.trim(trimStr.get.eval(input).asInstanceOf[UTF8String])
      } else {
        srcString.trim()
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trim();
        }""")
    } else {
      val trimString = evals(1)
      val getTrimFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trim(${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimFunction
        }""")
    }
  }
}

object StringTrimLeft {
  def apply(str: Expression, trimStr: Expression): StringTrimLeft =
    StringTrimLeft(str, Some(trimStr))
  def apply(str: Expression): StringTrimLeft = StringTrimLeft(str, None)
}

/**
 * A function that trims the characters from left end for a given string.
 * If LEADING and trimStr keywords are not specified, it defaults to remove space character from
 * the left end. The ltrim function will have one argument, which contains the source string.
 * If LEADING and trimStr keywords are not specified, it trims the characters from left end. The
 * ltrim function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: the function removes any character from the left end of the source string which matches
 * with the characters from trimStr, it stops at the first non-match character.
 * LEADING: removes any character from the left end of the source string that matches characters in
 * the trim string.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Removes the leading space characters from `str`.

    _FUNC_(trimStr, str) - Removes the leading string contains the characters from the trim string
  """,
  arguments = """
    Arguments:
      * str - a string expression
      * trimStr - the trim string characters to trim, the default value is a single space
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('    SparkSQL   ');
       SparkSQL
      > SELECT _FUNC_('Sp', 'SSparkSQLS');
       arkSQLS
  """,
  since = "1.5.0")
case class StringTrimLeft(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(srcStr: Expression, trimStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "ltrim"

  override def children: Seq[Expression] = if (trimStr.isDefined) {
    srcStr :: trimStr.get :: Nil
  } else {
    srcStr :: Nil
  }

  override def eval(input: InternalRow): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[UTF8String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        srcString.trimLeft(trimStr.get.eval(input).asInstanceOf[UTF8String])
      } else {
        srcString.trimLeft()
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trimLeft();
        }""")
    } else {
      val trimString = evals(1)
      val getTrimLeftFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trimLeft(${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimLeftFunction
        }""")
    }
  }
}

object StringTrimRight {
  def apply(str: Expression, trimStr: Expression): StringTrimRight =
    StringTrimRight(str, Some(trimStr))
  def apply(str: Expression) : StringTrimRight = StringTrimRight(str, None)
}

/**
 * A function that trims the characters from right end for a given string.
 * If TRAILING and trimStr keywords are not specified, it defaults to remove space character
 * from the right end. The rtrim function will have one argument, which contains the source string.
 * If TRAILING and trimStr keywords are specified, it trims the characters from right end. The
 * rtrim function will have two arguments, the first argument contains trimStr, the second argument
 * contains the source string.
 * trimStr: the function removes any character from the right end of source string which matches
 * with the characters from trimStr, it stops at the first non-match character.
 * TRAILING: removes any character from the right end of the source string that matches characters
 * in the trim string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Removes the trailing space characters from `str`.

    _FUNC_(trimStr, str) - Removes the trailing string which contains the characters from the trim string from the `str`
  """,
  arguments = """
    Arguments:
      * str - a string expression
      * trimStr - the trim string characters to trim, the default value is a single space
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('    SparkSQL   ');
       SparkSQL
      > SELECT _FUNC_('LQSa', 'SSparkSQLS');
       SSpark
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class StringTrimRight(
    srcStr: Expression,
    trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(srcStr: Expression, trimStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "rtrim"

  override def children: Seq[Expression] = if (trimStr.isDefined) {
    srcStr :: trimStr.get :: Nil
  } else {
    srcStr :: Nil
  }

  override def eval(input: InternalRow): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[UTF8String]
    if (srcString == null) {
      null
    } else {
      if (trimStr.isDefined) {
        srcString.trimRight(trimStr.get.eval(input).asInstanceOf[UTF8String])
      } else {
        srcString.trimRight()
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals(0)

    if (evals.length == 1) {
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trimRight();
        }""")
    } else {
      val trimString = evals(1)
      val getTrimRightFunction =
        s"""
        if (${trimString.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${srcString.value}.trimRight(${trimString.value});
        }"""
      ev.copy(evals.map(_.code) :+ code"""
        boolean ${ev.isNull} = false;
        UTF8String ${ev.value} = null;
        if (${srcString.isNull}) {
          ${ev.isNull} = true;
        } else {
          $getTrimRightFunction
        }""")
    }
  }
}

/**
 * A function that returns the position of the first occurrence of substr in the given string.
 * Returns null if either of the arguments are null and
 * returns 0 if substr could not be found in str.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, substr) - Returns the (1-based) index of the first occurrence of `substr` in `str`.",
  examples = """
    Examples:
      > SELECT _FUNC_('SparkSQL', 'SQL');
       6
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (l, r) =>
      s"($l).indexOf($r, 0) + 1")
  }
}

/**
 * Returns the substring from string str before count occurrences of the delimiter delim.
 * If count is positive, everything the left of the final delimiter (counting from left) is
 * returned. If count is negative, every to the right of the final delimiter (counting from the
 * right) is returned. substring_index performs a case-sensitive match when searching for delim.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, delim, count) - Returns the substring from `str` before `count` occurrences of the delimiter `delim`.
      If `count` is positive, everything to the left of the final delimiter (counting from the
      left) is returned. If `count` is negative, everything to the right of the final delimiter
      (counting from the right) is returned. The function substring_index performs a case-sensitive match
      when searching for `delim`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('www.apache.org', '.', 2);
       www.apache
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class SubstringIndex(strExpr: Expression, delimExpr: Expression, countExpr: Expression)
 extends TernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = Seq(strExpr, delimExpr, countExpr)
  override def prettyName: String = "substring_index"

  override def nullSafeEval(str: Any, delim: Any, count: Any): Any = {
    str.asInstanceOf[UTF8String].subStringIndex(
      delim.asInstanceOf[UTF8String],
      count.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (str, delim, count) => s"$str.subStringIndex($delim, $count)")
  }
}

/**
 * A function that returns the position of the first occurrence of substr
 * in given string after position pos.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(substr, str[, pos]) - Returns the position of the first occurrence of `substr` in `str` after position `pos`.
      The given `pos` and return value are 1-based.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('bar', 'foobarbar');
       4
      > SELECT _FUNC_('bar', 'foobarbar', 5);
       7
      > SELECT POSITION('bar' IN 'foobarbar');
       4
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class StringLocate(substr: Expression, str: Expression, start: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(substr: Expression, str: Expression) = {
    this(substr, str, Literal(1))
  }

  override def children: Seq[Expression] = substr :: str :: start :: Nil
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
          val sVal = s.asInstanceOf[Int]
          if (sVal < 1) {
            0
          } else {
            l.asInstanceOf[UTF8String].indexOf(
              r.asInstanceOf[UTF8String],
              s.asInstanceOf[Int] - 1) + 1
          }
        }
      }
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val substrGen = substr.genCode(ctx)
    val strGen = str.genCode(ctx)
    val startGen = start.genCode(ctx)
    ev.copy(code = code"""
      int ${ev.value} = 0;
      boolean ${ev.isNull} = false;
      ${startGen.code}
      if (!${startGen.isNull}) {
        ${substrGen.code}
        if (!${substrGen.isNull}) {
          ${strGen.code}
          if (!${strGen.isNull}) {
            if (${startGen.value} > 0) {
              ${ev.value} = ${strGen.value}.indexOf(${substrGen.value},
                ${startGen.value} - 1) + 1;
            }
          } else {
            ${ev.isNull} = true;
          }
        } else {
          ${ev.isNull} = true;
        }
      }
     """)
  }

  override def prettyName: String = "locate"
}

/**
 * Returns str, left-padded with pad to a length of len.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str, len[, pad]) - Returns `str`, left-padded with `pad` to a length of `len`.
      If `str` is longer than `len`, the return value is shortened to `len` characters.
      If `pad` is not specified, `str` will be padded to the left with space characters.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('hi', 5, '??');
       ???hi
      > SELECT _FUNC_('hi', 1, '??');
       h
      > SELECT _FUNC_('hi', 5);
          hi
  """,
  since = "1.5.0")
case class StringLPad(str: Expression, len: Expression, pad: Expression = Literal(" "))
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(str: Expression, len: Expression) = {
    this(str, len, Literal(" "))
  }

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].lpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (str, len, pad) => s"$str.lpad($len, $pad)")
  }

  override def prettyName: String = "lpad"
}

/**
 * Returns str, right-padded with pad to a length of len.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str, len[, pad]) - Returns `str`, right-padded with `pad` to a length of `len`.
      If `str` is longer than `len`, the return value is shortened to `len` characters.
      If `pad` is not specified, `str` will be padded to the right with space characters.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('hi', 5, '??');
       hi???
      > SELECT _FUNC_('hi', 1, '??');
       h
      > SELECT _FUNC_('hi', 5);
       hi
  """,
  since = "1.5.0")
case class StringRPad(str: Expression, len: Expression, pad: Expression = Literal(" "))
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(str: Expression, len: Expression) = {
    this(str, len, Literal(" "))
  }

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def nullSafeEval(str: Any, len: Any, pad: Any): Any = {
    str.asInstanceOf[UTF8String].rpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (str, len, pad) => s"$str.rpad($len, $pad)")
  }

  override def prettyName: String = "rpad"
}

object ParseUrl {
  private val HOST = UTF8String.fromString("HOST")
  private val PATH = UTF8String.fromString("PATH")
  private val QUERY = UTF8String.fromString("QUERY")
  private val REF = UTF8String.fromString("REF")
  private val PROTOCOL = UTF8String.fromString("PROTOCOL")
  private val FILE = UTF8String.fromString("FILE")
  private val AUTHORITY = UTF8String.fromString("AUTHORITY")
  private val USERINFO = UTF8String.fromString("USERINFO")
  private val REGEXPREFIX = "(&|^)"
  private val REGEXSUBFIX = "=([^&]*)"
}

/**
 * Extracts a part from a URL
 */
@ExpressionDescription(
  usage = "_FUNC_(url, partToExtract[, key]) - Extracts a part from a URL.",
  examples = """
    Examples:
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'HOST')
       spark.apache.org
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'QUERY')
       query=1
      > SELECT _FUNC_('http://spark.apache.org/path?query=1', 'QUERY', 'query')
       1
  """,
  since = "2.0.0")
case class ParseUrl(children: Seq[Expression])
  extends Expression with ExpectsInputTypes with CodegenFallback {

  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "parse_url"

  // If the url is a constant, cache the URL object so that we don't need to convert url
  // from UTF8String to String to URL for every row.
  @transient private lazy val cachedUrl = children(0) match {
    case Literal(url: UTF8String, _) if url ne null => getUrl(url)
    case _ => null
  }

  // If the key is a constant, cache the Pattern object so that we don't need to convert key
  // from UTF8String to String to StringBuilder to String to Pattern for every row.
  @transient private lazy val cachedPattern = children(2) match {
    case Literal(key: UTF8String, _) if key ne null => getPattern(key)
    case _ => null
  }

  // If the partToExtract is a constant, cache the Extract part function so that we don't need
  // to check the partToExtract for every row.
  @transient private lazy val cachedExtractPartFunc = children(1) match {
    case Literal(part: UTF8String, _) => getExtractPartFunc(part)
    case _ => null
  }

  import ParseUrl._

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size > 3 || children.size < 2) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName function requires two or three arguments")
    } else {
      super[ExpectsInputTypes].checkInputDataTypes()
    }
  }

  private def getPattern(key: UTF8String): Pattern = {
    Pattern.compile(REGEXPREFIX + key.toString + REGEXSUBFIX)
  }

  private def getUrl(url: UTF8String): URI = {
    try {
      new URI(url.toString)
    } catch {
      case e: URISyntaxException => null
    }
  }

  private def getExtractPartFunc(partToExtract: UTF8String): URI => String = {

    // partToExtract match {
    //   case HOST => _.toURL().getHost
    //   case PATH => _.toURL().getPath
    //   case QUERY => _.toURL().getQuery
    //   case REF => _.toURL().getRef
    //   case PROTOCOL => _.toURL().getProtocol
    //   case FILE => _.toURL().getFile
    //   case AUTHORITY => _.toURL().getAuthority
    //   case USERINFO => _.toURL().getUserInfo
    //   case _ => (url: URI) => null
    // }

    partToExtract match {
      case HOST => _.getHost
      case PATH => _.getRawPath
      case QUERY => _.getRawQuery
      case REF => _.getRawFragment
      case PROTOCOL => _.getScheme
      case FILE =>
        (url: URI) =>
          if (url.getRawQuery ne null) {
            url.getRawPath + "?" + url.getRawQuery
          } else {
            url.getRawPath
          }
      case AUTHORITY => _.getRawAuthority
      case USERINFO => _.getRawUserInfo
      case _ => (url: URI) => null
    }
  }

  private def extractValueFromQuery(query: UTF8String, pattern: Pattern): UTF8String = {
    val m = pattern.matcher(query.toString)
    if (m.find()) {
      UTF8String.fromString(m.group(2))
    } else {
      null
    }
  }

  private def extractFromUrl(url: URI, partToExtract: UTF8String): UTF8String = {
    if (cachedExtractPartFunc ne null) {
      UTF8String.fromString(cachedExtractPartFunc.apply(url))
    } else {
      UTF8String.fromString(getExtractPartFunc(partToExtract).apply(url))
    }
  }

  private def parseUrlWithoutKey(url: UTF8String, partToExtract: UTF8String): UTF8String = {
    if (cachedUrl ne null) {
      extractFromUrl(cachedUrl, partToExtract)
    } else {
      val currentUrl = getUrl(url)
      if (currentUrl ne null) {
        extractFromUrl(currentUrl, partToExtract)
      } else {
        null
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    val evaluated = children.map{e => e.eval(input).asInstanceOf[UTF8String]}
    if (evaluated.contains(null)) return null
    if (evaluated.size == 2) {
      parseUrlWithoutKey(evaluated(0), evaluated(1))
    } else {
      // 3-arg, i.e. QUERY with key
      assert(evaluated.size == 3)
      if (evaluated(1) != QUERY) {
        return null
      }

      val query = parseUrlWithoutKey(evaluated(0), evaluated(1))
      if (query eq null) {
        return null
      }

      if (cachedPattern ne null) {
        extractValueFromQuery(query, cachedPattern)
      } else {
        extractValueFromQuery(query, getPattern(evaluated(2)))
      }
    }
  }
}

/**
 * Returns the input formatted according do printf-style format strings
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(strfmt, obj, ...) - Returns a formatted string from printf-style format strings.",
  examples = """
    Examples:
      > SELECT _FUNC_("Hello World %d %s", 100, "days");
       Hello World 100 days
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class FormatString(children: Expression*) extends Expression with ImplicitCastInputTypes {

  require(children.nonEmpty, "format_string() should take at least 1 argument")

  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children(0).nullable
  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] =
    StringType :: List.fill(children.size - 1)(AnyDataType)

  override def eval(input: InternalRow): Any = {
    val pattern = children(0).eval(input)
    if (pattern == null) {
      null
    } else {
      val sb = new StringBuffer()
      val formatter = new java.util.Formatter(sb, Locale.US)

      val arglist = children.tail.map(_.eval(input).asInstanceOf[AnyRef])
      formatter.format(pattern.asInstanceOf[UTF8String].toString, arglist: _*)

      UTF8String.fromString(sb.toString)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pattern = children.head.genCode(ctx)

    val argListGen = children.tail.map(x => (x.dataType, x.genCode(ctx)))
    val argList = ctx.freshName("argLists")
    val numArgLists = argListGen.length
    val argListCode = argListGen.zipWithIndex.map { case(v, index) =>
      val value =
        if (CodeGenerator.boxedType(v._1) != CodeGenerator.javaType(v._1)) {
          // Java primitives get boxed in order to allow null values.
          s"(${v._2.isNull}) ? (${CodeGenerator.boxedType(v._1)}) null : " +
            s"new ${CodeGenerator.boxedType(v._1)}(${v._2.value})"
        } else {
          s"(${v._2.isNull}) ? null : ${v._2.value}"
        }
      s"""
         ${v._2.code}
         $argList[$index] = $value;
       """
    }
    val argListCodes = ctx.splitExpressionsWithCurrentInputs(
      expressions = argListCode,
      funcName = "valueFormatString",
      extraArguments = ("Object[]", argList) :: Nil)

    val form = ctx.freshName("formatter")
    val formatter = classOf[java.util.Formatter].getName
    val sb = ctx.freshName("sb")
    val stringBuffer = classOf[StringBuffer].getName
    ev.copy(code = code"""
      ${pattern.code}
      boolean ${ev.isNull} = ${pattern.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $stringBuffer $sb = new $stringBuffer();
        $formatter $form = new $formatter($sb, ${classOf[Locale].getName}.US);
        Object[] $argList = new Object[$numArgLists];
        $argListCodes
        $form.format(${pattern.value}.toString(), $argList);
        ${ev.value} = UTF8String.fromString($sb.toString());
      }""")
  }

  override def prettyName: String = "format_string"
}

/**
 * Returns string, with the first letter of each word in uppercase, all other letters in lowercase.
 * Words are delimited by whitespace.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Returns `str` with the first letter of each word in uppercase.
      All other letters are in lowercase. Words are delimited by white space.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('sPark sql');
       Spark Sql
  """,
  since = "1.5.0")
case class InitCap(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType

  override def nullSafeEval(string: Any): Any = {
    // scalastyle:off caselocale
    string.asInstanceOf[UTF8String].toLowerCase.toTitleCase
    // scalastyle:on caselocale
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, str => s"$str.toLowerCase().toTitleCase()")
  }
}

/**
 * Returns the string which repeat the given string value n times.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, n) - Returns the string which repeats the given string value n times.",
  examples = """
    Examples:
      > SELECT _FUNC_('123', 2);
       123123
  """,
  since = "1.5.0")
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (l, r) => s"($l).repeat($r)")
  }
}

/**
 * Returns a string consisting of n spaces.
 */
@ExpressionDescription(
  usage = "_FUNC_(n) - Returns a string consisting of `n` spaces.",
  examples = """
    Examples:
      > SELECT concat(_FUNC_(2), '1');
         1
  """,
  since = "1.5.0")
case class StringSpace(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def nullSafeEval(s: Any): Any = {
    val length = s.asInstanceOf[Int]
    UTF8String.blankString(if (length < 0) 0 else length)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (length) =>
      s"""${ev.value} = UTF8String.blankString(($length < 0) ? 0 : $length);""")
  }

  override def prettyName: String = "space"
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, pos[, len]) - Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of byte array that starts at `pos` and is of length `len`.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 5);
       k SQL
      > SELECT _FUNC_('Spark SQL', -3);
       SQL
      > SELECT _FUNC_('Spark SQL', 5, 1);
       k
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def children: Seq[Expression] = str :: pos :: len :: Nil

  override def nullSafeEval(string: Any, pos: Any, len: Any): Any = {
    str.dataType match {
      case StringType => string.asInstanceOf[UTF8String]
        .substringSQL(pos.asInstanceOf[Int], len.asInstanceOf[Int])
      case BinaryType => ByteArray.subStringSQL(string.asInstanceOf[Array[Byte]],
        pos.asInstanceOf[Int], len.asInstanceOf[Int])
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    defineCodeGen(ctx, ev, (string, pos, len) => {
      str.dataType match {
        case StringType => s"$string.substringSQL($pos, $len)"
        case BinaryType => s"${classOf[ByteArray].getName}.subStringSQL($string, $pos, $len)"
      }
    })
  }
}

/**
 * Returns the rightmost n characters from the string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, len) - Returns the rightmost `len`(`len` can be string type) characters from the string `str`,if `len` is less or equal than 0 the result is an empty string.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 3);
       SQL
  """,
  since = "2.3.0")
// scalastyle:on line.size.limit
case class Right(str: Expression, len: Expression, child: Expression) extends RuntimeReplaceable {
  def this(str: Expression, len: Expression) = {
    this(str, len, If(IsNull(str), Literal(null, StringType), If(LessThanOrEqual(len, Literal(0)),
      Literal(UTF8String.EMPTY_UTF8, StringType), new Substring(str, UnaryMinus(len)))))
  }

  override def flatArguments: Iterator[Any] = Iterator(str, len)
  override def sql: String = s"$prettyName(${str.sql}, ${len.sql})"
}

/**
 * Returns the leftmost n characters from the string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, len) - Returns the leftmost `len`(`len` can be string type) characters from the string `str`,if `len` is less or equal than 0 the result is an empty string.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 3);
       Spa
  """,
  since = "2.3.0")
// scalastyle:on line.size.limit
case class Left(str: Expression, len: Expression, child: Expression) extends RuntimeReplaceable {
  def this(str: Expression, len: Expression) = {
    this(str, len, Substring(str, Literal(1), len))
  }

  override def flatArguments: Iterator[Any] = Iterator(str, len)
  override def sql: String = s"$prettyName(${str.sql}, ${len.sql})"
}

/**
 * A function that returns the char length of the given string expression or
 * number of bytes of the given binary expression.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL ');
       10
      > SELECT CHAR_LENGTH('Spark SQL ');
       10
      > SELECT CHARACTER_LENGTH('Spark SQL ');
       10
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class Length(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[UTF8String].numChars
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, c => s"($c).numChars()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }
}

/**
 * A function that returns the bit length of the given string or binary expression.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the bit length of string data or number of bits of binary data.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       72
  """,
  since = "2.3.0")
case class BitLength(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[UTF8String].numBytes * 8
    case BinaryType => value.asInstanceOf[Array[Byte]].length * 8
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, c => s"($c).numBytes() * 8")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length * 8")
    }
  }

  override def prettyName: String = "bit_length"
}

/**
 * A function that returns the byte length of the given string or binary expression.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the byte length of string data or number of bytes of binary " +
    "data.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       9
  """,
  since = "2.3.0")
case class OctetLength(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case StringType => value.asInstanceOf[UTF8String].numBytes
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case StringType => defineCodeGen(ctx, ev, c => s"($c).numBytes()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }

  override def prettyName: String = "octet_length"
}

/**
 * A function that return the Levenshtein distance between the two given strings.
 */
@ExpressionDescription(
  usage = "_FUNC_(str1, str2) - Returns the Levenshtein distance between the two given strings.",
  examples = """
    Examples:
      > SELECT _FUNC_('kitten', 'sitting');
       3
  """,
  since = "1.5.0")
case class Levenshtein(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = IntegerType
  protected override def nullSafeEval(leftValue: Any, rightValue: Any): Any =
    leftValue.asInstanceOf[UTF8String].levenshteinDistance(rightValue.asInstanceOf[UTF8String])

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (left, right) =>
      s"${ev.value} = $left.levenshteinDistance($right);")
  }
}

/**
 * A function that return Soundex code of the given string expression.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns Soundex code of the string.",
  examples = """
    Examples:
      > SELECT _FUNC_('Miller');
       M460
  """,
  since = "1.5.0")
case class SoundEx(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def nullSafeEval(input: Any): Any = input.asInstanceOf[UTF8String].soundex()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c.soundex()")
  }
}

/**
 * Returns the numeric value of the first character of str.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns the numeric value of the first character of `str`.",
  examples = """
    Examples:
      > SELECT _FUNC_('222');
       50
      > SELECT _FUNC_(2);
       50
  """,
  since = "1.5.0")
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (child) => {
      val bytes = ctx.freshName("bytes")
      s"""
        byte[] $bytes = $child.getBytes();
        if ($bytes.length > 0) {
          ${ev.value} = (int) $bytes[0];
        } else {
          ${ev.value} = 0;
        }
       """})
  }
}

/**
 * Returns the ASCII character having the binary equivalent to n.
 * If n is larger than 256 the result is equivalent to chr(n % 256)
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the ASCII character having the binary equivalent to `expr`. If n is larger than 256 the result is equivalent to chr(n % 256)",
  examples = """
    Examples:
      > SELECT _FUNC_(65);
       A
  """,
  since = "2.3.0")
// scalastyle:on line.size.limit
case class Chr(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(LongType)

  protected override def nullSafeEval(lon: Any): Any = {
    val longVal = lon.asInstanceOf[Long]
    if (longVal < 0) {
      UTF8String.EMPTY_UTF8
    } else if ((longVal & 0xFF) == 0) {
      UTF8String.fromString(Character.MIN_VALUE.toString)
    } else {
      UTF8String.fromString((longVal & 0xFF).toChar.toString)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, lon => {
      s"""
        if ($lon < 0) {
          ${ev.value} = UTF8String.EMPTY_UTF8;
        } else if (($lon & 0xFF) == 0) {
          ${ev.value} = UTF8String.fromString(String.valueOf(Character.MIN_VALUE));
        } else {
          char c = (char)($lon & 0xFF);
          ${ev.value} = UTF8String.fromString(String.valueOf(c));
        }
      """
    })
  }
}

/**
 * Converts the argument from binary to a base 64 string.
 */
@ExpressionDescription(
  usage = "_FUNC_(bin) - Converts the argument from a binary `bin` to a base 64 string.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL');
       U3BhcmsgU1FM
  """,
  since = "1.5.0")
case class Base64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(bytes: Any): Any = {
    UTF8String.fromBytes(CommonsBase64.encodeBase64(bytes.asInstanceOf[Array[Byte]]))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""${ev.value} = UTF8String.fromBytes(
            ${classOf[CommonsBase64].getName}.encodeBase64($child));
       """})
  }
}

/**
 * Converts the argument from a base 64 string to BINARY.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Converts the argument from a base 64 string `str` to a binary.",
  examples = """
    Examples:
      > SELECT _FUNC_('U3BhcmsgU1FM');
       Spark SQL
  """,
  since = "1.5.0")
case class UnBase64(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[DataType] = Seq(StringType)

  protected override def nullSafeEval(string: Any): Any =
    CommonsBase64.decodeBase64(string.asInstanceOf[UTF8String].toString)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (child) => {
      s"""
         ${ev.value} = ${classOf[CommonsBase64].getName}.decodeBase64($child.toString());
       """})
  }
}

/**
 * Decodes the first argument into a String using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(bin, charset) - Decodes the first argument using the second argument character set.",
  examples = """
    Examples:
      > SELECT _FUNC_(encode('abc', 'utf-8'), 'utf-8');
       abc
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (bytes, charset) =>
      s"""
        try {
          ${ev.value} = UTF8String.fromString(new String($bytes, $charset.toString()));
        } catch (java.io.UnsupportedEncodingException e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }
      """)
  }
}

/**
 * Encodes the first argument into a BINARY using the provided character set
 * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 * If either argument is null, the result will also be null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, charset) - Encodes the first argument using the second argument character set.",
  examples = """
    Examples:
      > SELECT _FUNC_('abc', 'utf-8');
       abc
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
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

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (string, charset) =>
      s"""
        try {
          ${ev.value} = $string.toString().getBytes($charset.toString());
        } catch (java.io.UnsupportedEncodingException e) {
          org.apache.spark.unsafe.Platform.throwException(e);
        }""")
  }
}

/**
 * Formats the number X to a format like '#,###,###.##', rounded to D decimal places,
 * and returns the result as a string. If D is 0, the result has no decimal point or
 * fractional part.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Formats the number `expr1` like '#,###,###.##', rounded to `expr2`
      decimal places. If `expr2` is 0, the result has no decimal point or fractional part.
      `expr2` also accept a user specified format.
      This is supposed to function like MySQL's FORMAT.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(12332.123456, 4);
       12,332.1235
      > SELECT _FUNC_(12332.123456, '##################.###');
       12332.123
  """,
  since = "1.5.0")
case class FormatNumber(x: Expression, d: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def left: Expression = x
  override def right: Expression = d
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq(NumericType, TypeCollection(IntegerType, StringType))

  private val defaultFormat = "#,###,###,###,###,###,##0"

  // Associated with the pattern, for the last d value, and we will update the
  // pattern (DecimalFormat) once the new coming d value differ with the last one.
  // This is an Option to distinguish between 0 (numberFormat is valid) and uninitialized after
  // serialization (numberFormat has not been updated for dValue = 0).
  @transient
  private var lastDIntValue: Option[Int] = None

  @transient
  private var lastDStringValue: Option[String] = None

  // A cached DecimalFormat, for performance concern, we will change it
  // only if the d value changed.
  @transient
  private lazy val pattern: StringBuffer = new StringBuffer()

  // SPARK-13515: US Locale configures the DecimalFormat object to use a dot ('.')
  // as a decimal separator.
  @transient
  private lazy val numberFormat = new DecimalFormat("", new DecimalFormatSymbols(Locale.US))

  override protected def nullSafeEval(xObject: Any, dObject: Any): Any = {
    right.dataType match {
      case IntegerType =>
        val dValue = dObject.asInstanceOf[Int]
        if (dValue < 0) {
          return null
        }

        lastDIntValue match {
          case Some(last) if last == dValue =>
          // use the current pattern
          case _ =>
            // construct a new DecimalFormat only if a new dValue
            pattern.delete(0, pattern.length)
            pattern.append(defaultFormat)

            // decimal place
            if (dValue > 0) {
              pattern.append(".")

              var i = 0
              while (i < dValue) {
                i += 1
                pattern.append("0")
              }
            }

            lastDIntValue = Some(dValue)

            numberFormat.applyLocalizedPattern(pattern.toString)
        }
      case StringType =>
        val dValue = dObject.asInstanceOf[UTF8String].toString
        lastDStringValue match {
          case Some(last) if last == dValue =>
          case _ =>
            pattern.delete(0, pattern.length)
            lastDStringValue = Some(dValue)
            if (dValue.isEmpty) {
              numberFormat.applyLocalizedPattern(defaultFormat)
            } else {
              numberFormat.applyLocalizedPattern(dValue)
            }
        }
    }

    x.dataType match {
      case ByteType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Byte]))
      case ShortType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Short]))
      case FloatType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Float]))
      case IntegerType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Int]))
      case LongType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Long]))
      case DoubleType => UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Double]))
      case _: DecimalType =>
        UTF8String.fromString(numberFormat.format(xObject.asInstanceOf[Decimal].toJavaBigDecimal))
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (num, d) => {

      def typeHelper(p: String): String = {
        x.dataType match {
          case _ : DecimalType => s"""$p.toJavaBigDecimal()"""
          case _ => s"$p"
        }
      }

      val sb = classOf[StringBuffer].getName
      val df = classOf[DecimalFormat].getName
      val dfs = classOf[DecimalFormatSymbols].getName
      val l = classOf[Locale].getName
      // SPARK-13515: US Locale configures the DecimalFormat object to use a dot ('.')
      // as a decimal separator.
      val usLocale = "US"
      val numberFormat = ctx.addMutableState(df, "numberFormat",
        v => s"""$v = new $df("", new $dfs($l.$usLocale));""")

      right.dataType match {
        case IntegerType =>
          val pattern = ctx.addMutableState(sb, "pattern", v => s"$v = new $sb();")
          val i = ctx.freshName("i")
          val lastDValue =
            ctx.addMutableState(CodeGenerator.JAVA_INT, "lastDValue", v => s"$v = -100;")
          s"""
            if ($d >= 0) {
              $pattern.delete(0, $pattern.length());
              if ($d != $lastDValue) {
                $pattern.append("$defaultFormat");

                if ($d > 0) {
                  $pattern.append(".");
                  for (int $i = 0; $i < $d; $i++) {
                    $pattern.append("0");
                  }
                }
                $lastDValue = $d;
                $numberFormat.applyLocalizedPattern($pattern.toString());
              }
              ${ev.value} = UTF8String.fromString($numberFormat.format(${typeHelper(num)}));
            } else {
              ${ev.value} = null;
              ${ev.isNull} = true;
            }
           """
        case StringType =>
          val lastDValue = ctx.addMutableState("String", "lastDValue", v => s"""$v = null;""")
          val dValue = ctx.freshName("dValue")
          s"""
            String $dValue = $d.toString();
            if (!$dValue.equals($lastDValue)) {
              $lastDValue = $dValue;
              if ($dValue.isEmpty()) {
                $numberFormat.applyLocalizedPattern("$defaultFormat");
              } else {
                $numberFormat.applyLocalizedPattern($dValue);
              }
            }
            ${ev.value} = UTF8String.fromString($numberFormat.format(${typeHelper(num)}));
           """
      }
    })
  }

  override def prettyName: String = "format_number"
}

/**
 * Splits a string into arrays of sentences, where each sentence is an array of words.
 * The 'lang' and 'country' arguments are optional, and if omitted, the default locale is used.
 */
@ExpressionDescription(
  usage = "_FUNC_(str[, lang, country]) - Splits `str` into an array of array of words.",
  examples = """
    Examples:
      > SELECT _FUNC_('Hi there! Good morning.');
       [["Hi","there"],["Good","morning"]]
  """,
  since = "2.0.0")
case class Sentences(
    str: Expression,
    language: Expression = Literal(""),
    country: Expression = Literal(""))
  extends Expression with ImplicitCastInputTypes with CodegenFallback {

  def this(str: Expression) = this(str, Literal(""), Literal(""))
  def this(str: Expression, language: Expression) = this(str, language, Literal(""))

  override def nullable: Boolean = true
  override def dataType: DataType =
    ArrayType(ArrayType(StringType, containsNull = false), containsNull = false)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = str :: language :: country :: Nil

  override def eval(input: InternalRow): Any = {
    val string = str.eval(input)
    if (string == null) {
      null
    } else {
      val languageStr = language.eval(input).asInstanceOf[UTF8String]
      val countryStr = country.eval(input).asInstanceOf[UTF8String]
      val locale = if (languageStr != null && countryStr != null) {
        new Locale(languageStr.toString, countryStr.toString)
      } else {
        Locale.US
      }
      getSentences(string.asInstanceOf[UTF8String].toString, locale)
    }
  }

  private def getSentences(sentences: String, locale: Locale) = {
    val bi = BreakIterator.getSentenceInstance(locale)
    bi.setText(sentences)
    var idx = 0
    val result = new ArrayBuffer[GenericArrayData]
    while (bi.next != BreakIterator.DONE) {
      val sentence = sentences.substring(idx, bi.current)
      idx = bi.current

      val wi = BreakIterator.getWordInstance(locale)
      var widx = 0
      wi.setText(sentence)
      val words = new ArrayBuffer[UTF8String]
      while (wi.next != BreakIterator.DONE) {
        val word = sentence.substring(widx, wi.current)
        widx = wi.current
        if (Character.isLetterOrDigit(word.charAt(0))) words += UTF8String.fromString(word)
      }
      result += new GenericArrayData(words)
    }
    new GenericArrayData(result)
  }
}

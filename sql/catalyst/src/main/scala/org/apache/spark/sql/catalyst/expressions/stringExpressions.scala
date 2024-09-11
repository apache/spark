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

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.CharacterCodingException
import java.text.{BreakIterator, DecimalFormat, DecimalFormatSymbols}
import java.util.{Base64 => JBase64, HashMap, Locale, Map => JMap}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.QueryContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UPPER_OR_LOWER}
import org.apache.spark.sql.catalyst.util.{ArrayData, CharsetProvider, CollationFactory, CollationSupport, GenericArrayData, TypeUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.{AbstractArrayType, StringTypeAnyCollation}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.UTF8StringBuilder
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
import org.apache.spark.util.ArrayImplicits._

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
  usage = "_FUNC_(sep[, str | array(str)]+) - Returns the concatenation of the strings separated by `sep`, skipping null values.",
  examples = """
    Examples:
      > SELECT _FUNC_(' ', 'Spark', 'SQL');
        Spark SQL
      > SELECT _FUNC_('s');

      > SELECT _FUNC_('/', 'foo', null, 'bar');
        foo/bar
      > SELECT _FUNC_(null, 'Spark', 'SQL');
        NULL
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class ConcatWs(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def prettyName: String = "concat_ws"

  /** The 1st child (separator) is str, and rest are either str or array of str. */
  override def inputTypes: Seq[AbstractDataType] = {
    val arrayOrStr =
      TypeCollection(AbstractArrayType(StringTypeAnyCollation), StringTypeAnyCollation)
    StringTypeAnyCollation +: Seq.fill(children.size - 1)(arrayOrStr)
  }

  override def dataType: DataType = children.head.dataType

  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 0"), children.length
      )
    } else {
      super.checkInputDataTypes()
    }
  }

  override def eval(input: InternalRow): Any = {
    val flatInputs = children.flatMap { child =>
      child.eval(input) match {
        case s: UTF8String => Iterator(s)
        case arr: ArrayData =>
          arr.toArray[UTF8String](child.dataType.asInstanceOf[ArrayType].elementType)
        case null => Iterator(null.asInstanceOf[UTF8String])
      }
    }
    UTF8String.concatWs(flatInputs.head, flatInputs.tail : _*)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (children.forall(_.dataType.isInstanceOf[StringType])) {
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
      val isNullArgs = ctx.freshName("isNullArgs")
      val valueArgs = ctx.freshName("valueArgs")

      val array = ctx.freshName("array")
      val varargNum = ctx.freshName("varargNum")
      val idxVararg = ctx.freshName("idxInVararg")

      val evals = children.map(_.genCode(ctx))
      val (argBuild, varargCount, varargBuild) = children.tail.zip(evals.tail)
        .zipWithIndex.map { case ((child, eval), idx) =>
        val reprForIsNull = s"$isNullArgs[$idx]"
        val reprForValue = s"$valueArgs[$idx]"

        val arg =
          s"""
           ${eval.code}
           $reprForIsNull = ${eval.isNull};
           $reprForValue = ${eval.value};
           """

        val (varCount, varBuild) = child.dataType match {
          case _: StringType =>
            val reprForValueCast = s"((UTF8String) $reprForValue)"
            ("", // we count all the StringType arguments num at once below.
              if (eval.isNull == TrueLiteral) {
                ""
              } else {
                s"$array[$idxVararg ++] = $reprForIsNull ? (UTF8String) null : $reprForValueCast;"
              })
          case arr: ArrayType =>
            val reprForValueCast = s"((ArrayData) $reprForValue)"
            val size = ctx.freshName("n")
            if (eval.isNull == TrueLiteral) {
              ("", "")
            } else {
              // scalastyle:off line.size.limit
              (s"""
                if (!$reprForIsNull) {
                  $varargNum += $reprForValueCast.numElements();
                }
                """,
                s"""
                if (!$reprForIsNull) {
                  final int $size = $reprForValueCast.numElements();
                  for (int j = 0; j < $size; j ++) {
                    $array[$idxVararg ++] = ${CodeGenerator.getValue(reprForValueCast, arr.elementType, "j")};
                  }
                }
                """)
              // scalastyle:on line.size.limit
            }
        }

        (arg, varCount, varBuild)
      }.unzip3

      val argBuilds = ctx.splitExpressionsWithCurrentInputs(
        expressions = argBuild,
        funcName = "initializeArgsArrays",
        extraArguments = ("boolean []", isNullArgs) :: ("Object []", valueArgs) :: Nil
      )

      val varargCounts = ctx.splitExpressionsWithCurrentInputs(
        expressions = varargCount,
        funcName = "varargCountsConcatWs",
        extraArguments = ("boolean []", isNullArgs) :: ("Object []", valueArgs) :: Nil,
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
        extraArguments = ("UTF8String []", array) :: ("int", idxVararg) ::
          ("boolean []", isNullArgs) :: ("Object []", valueArgs) :: Nil,
        returnType = "int",
        makeSplitFunction = body =>
          s"""
             |$body
             |return $idxVararg;
           """.stripMargin,
        foldFunctions = _.map(funcCall => s"$idxVararg = $funcCall;").mkString("\n"))

      ev.copy(
        code"""
        boolean[] $isNullArgs = new boolean[${children.length - 1}];
        Object[] $valueArgs = new Object[${children.length - 1}];
        $argBuilds
        int $varargNum = ${children.count(_.dataType.isInstanceOf[StringType]) - 1};
        int $idxVararg = 0;
        $varargCounts
        UTF8String[] $array = new UTF8String[$varargNum];
        $varargBuilds
        ${evals.head.code}
        UTF8String ${ev.value} = UTF8String.concatWs(${evals.head.value}, $array);
        boolean ${ev.isNull} = ${ev.value} == null;
      """)
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ConcatWs =
    copy(children = newChildren)
}

/**
 * An expression that returns the `n`-th input in given inputs.
 * If all inputs are binary, `elt` returns an output as binary. Otherwise, it returns as string.
 * If any input is null, `elt` returns null.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(n, input1, input2, ...) - Returns the `n`-th input, e.g., returns `input2` when `n` is 2.
    The function returns NULL if the index exceeds the length of the array
    and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true,
    it throws ArrayIndexOutOfBoundsException for invalid indices.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(1, 'scala', 'java');
       scala
      > SELECT _FUNC_(2, 'a', 1);
       1
  """,
  since = "2.0.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Elt(
    children: Seq[Expression],
    failOnError: Boolean = SQLConf.get.ansiEnabled) extends Expression
  with SupportQueryContext {

  def this(children: Seq[Expression]) = this(children, SQLConf.get.ansiEnabled)

  private lazy val indexExpr = children.head
  private lazy val inputExprs = children.tail.toArray

  /** This expression is always nullable because it returns null if index is out of range. */
  override def nullable: Boolean = true

  override def dataType: DataType =
    inputExprs.map(_.dataType).headOption.getOrElse(SQLConf.get.defaultStringType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 2) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq("> 1"), children.length
      )
    } else {
      val (indexType, inputTypes) = (indexExpr.dataType, inputExprs.map(_.dataType))
      if (indexType != IntegerType) {
        return DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(IntegerType),
            "inputSql" -> toSQLExpr(indexExpr),
            "inputType" -> toSQLType(indexType)))
      }
      if (inputTypes.exists(tpe => !tpe.isInstanceOf[StringType] && tpe != BinaryType)) {
        return DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> (ordinalNumber(1) + "..."),
            "requiredType" -> (toSQLType(StringType) + " or " + toSQLType(BinaryType)),
            "inputSql" -> inputExprs.map(toSQLExpr).mkString(","),
            "inputType" -> inputTypes.map(toSQLType).mkString(",")
          )
        )
      }
      TypeUtils.checkForSameTypeInputExpr(inputTypes.toImmutableArraySeq, prettyName)
    }
  }

  override def eval(input: InternalRow): Any = {
    val indexObj = indexExpr.eval(input)
    if (indexObj == null) {
      null
    } else {
      val index = indexObj.asInstanceOf[Int]
      if (index <= 0 || index > inputExprs.length) {
        if (failOnError) {
          throw QueryExecutionErrors.invalidArrayIndexError(
            index, inputExprs.length, getContextOrNull())
        } else {
          null
        }
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
      expressions = assignInputValue.toImmutableArraySeq,
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

    val indexOutOfBoundBranch = if (failOnError) {
      val errorContext = getContextOrNullCode(ctx)
      // scalastyle:off line.size.limit
      s"""
         |if (!$indexMatched) {
         |  throw QueryExecutionErrors.invalidArrayIndexError(${index.value}, ${inputExprs.length}, $errorContext);
         |}
       """.stripMargin
      // scalastyle:on line.size.limit
    } else {
      ""
    }

    ev.copy(
      code"""
         |${index.code}
         |boolean ${ev.isNull} = ${index.isNull};
         |${CodeGenerator.javaType(dataType)} ${ev.value} = null;
         |if (!${index.isNull}) {
         |  final int $indexVal = ${index.value};
         |  ${CodeGenerator.JAVA_BOOLEAN} $indexMatched = false;
         |  $inputVal = null;
         |  do {
         |    $codes
         |  } while (false);
         |  $indexOutOfBoundBranch
         |  ${ev.value} = $inputVal;
         |  ${ev.isNull} = ${ev.value} == null;
         |}
       """.stripMargin)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Elt =
    copy(children = newChildren)

  override def initQueryContext(): Option[QueryContext] = if (failOnError) {
    Some(origin.context)
  } else {
    None
  }
}


trait String2StringExpression extends ImplicitCastInputTypes {
  self: UnaryExpression =>

  def convert(v: UTF8String): UTF8String

  override def dataType: DataType = child.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

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
  since = "1.0.1",
  group = "string_funcs")
case class Upper(child: Expression)
  extends UnaryExpression with String2StringExpression with NullIntolerant {

  final lazy val collationId: Int = child.dataType.asInstanceOf[StringType].collationId

  // Flag to indicate whether to use ICU instead of JVM case mappings for UTF8_BINARY collation.
  private final lazy val useICU = SQLConf.get.getConf(SQLConf.ICU_CASE_MAPPINGS_ENABLED)

  override def convert(v: UTF8String): UTF8String =
    CollationSupport.Upper.exec(v, collationId, useICU)

  final override val nodePatterns: Seq[TreePattern] = Seq(UPPER_OR_LOWER)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => CollationSupport.Upper.genCode(c, collationId, useICU))
  }

  override protected def withNewChildInternal(newChild: Expression): Upper = copy(child = newChild)
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
  since = "1.0.1",
  group = "string_funcs")
case class Lower(child: Expression)
  extends UnaryExpression with String2StringExpression with NullIntolerant {

  final lazy val collationId: Int = child.dataType.asInstanceOf[StringType].collationId

  // Flag to indicate whether to use ICU instead of JVM case mappings for UTF8_BINARY collation.
  private final lazy val useICU = SQLConf.get.getConf(SQLConf.ICU_CASE_MAPPINGS_ENABLED)

  override def convert(v: UTF8String): UTF8String =
    CollationSupport.Lower.exec(v, collationId, useICU)

  final override val nodePatterns: Seq[TreePattern] = Seq(UPPER_OR_LOWER)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => CollationSupport.Lower.genCode(c, collationId, useICU))
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("lower")

  override protected def withNewChildInternal(newChild: Expression): Lower = copy(child = newChild)
}

/** A base trait for functions that compare two strings, returning a boolean. */
abstract class StringPredicate extends BinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = left.dataType.asInstanceOf[StringType].collationId

  def compare(l: UTF8String, r: UTF8String): Boolean

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation)

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    compare(input1.asInstanceOf[UTF8String], input2.asInstanceOf[UTF8String])

  override def toString: String = s"$nodeName($left, $right)"
}

trait StringBinaryPredicateExpressionBuilderBase extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 2) {
      if (expressions(0).dataType == BinaryType && expressions(1).dataType == BinaryType) {
        BinaryPredicate(funcName, expressions(0), expressions(1))
      } else {
        createStringPredicate(expressions(0), expressions(1))
      }
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2), numArgs)
    }
  }

  protected def createStringPredicate(left: Expression, right: Expression): Expression
}

object BinaryPredicate {
  def unapply(expr: Expression): Option[StaticInvoke] = expr match {
    case s @ StaticInvoke(
        clz, _, "contains" | "startsWith" | "endsWith", Seq(_, _), _, _, _, _, _)
      if clz == classOf[ByteArrayMethods] => Some(s)
    case _ => None
  }
}

case class BinaryPredicate(override val prettyName: String, left: Expression, right: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes with BinaryLike[Expression] {

  private lazy val realFuncName = prettyName match {
    case "startswith" => "startsWith"
    case "endswith" => "endsWith"
    case name => name
  }

  override lazy val replacement =
    StaticInvoke(
      classOf[ByteArrayMethods],
      BooleanType,
      realFuncName,
      Seq(left, right),
      Seq(BinaryType, BinaryType))

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Returns a boolean. The value is True if right is found inside left.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both left or right must be of STRING or BINARY type.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 'Spark');
       true
      > SELECT _FUNC_('Spark SQL', 'SPARK');
       false
      > SELECT _FUNC_('Spark SQL', null);
       NULL
      > SELECT _FUNC_(x'537061726b2053514c', x'537061726b');
       true
  """,
  since = "3.3.0",
  group = "string_funcs"
)
object ContainsExpressionBuilder extends StringBinaryPredicateExpressionBuilderBase {
  override protected def createStringPredicate(left: Expression, right: Expression): Expression = {
    Contains(left, right)
  }
}

case class Contains(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = {
    CollationSupport.Contains.exec(l, r, collationId)
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) =>
      CollationSupport.Contains.genCode(c1, c2, collationId))
  }
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Contains = copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Returns a boolean. The value is True if left starts with right.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both left or right must be of STRING or BINARY type.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 'Spark');
       true
      > SELECT _FUNC_('Spark SQL', 'SQL');
       false
      > SELECT _FUNC_('Spark SQL', null);
       NULL
      > SELECT _FUNC_(x'537061726b2053514c', x'537061726b');
       true
      > SELECT _FUNC_(x'537061726b2053514c', x'53514c');
       false
  """,
  since = "3.3.0",
  group = "string_funcs"
)
object StartsWithExpressionBuilder extends StringBinaryPredicateExpressionBuilderBase {
  override protected def createStringPredicate(left: Expression, right: Expression): Expression = {
    StartsWith(left, right)
  }
}

case class StartsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = {
    CollationSupport.StartsWith.exec(l, r, collationId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) =>
      CollationSupport.StartsWith.genCode(c1, c2, collationId))
  }
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): StartsWith = copy(left = newLeft, right = newRight)
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Returns a boolean. The value is True if left ends with right.
    Returns NULL if either input expression is NULL. Otherwise, returns False.
    Both left or right must be of STRING or BINARY type.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 'SQL');
       true
      > SELECT _FUNC_('Spark SQL', 'Spark');
       false
      > SELECT _FUNC_('Spark SQL', null);
       NULL
      > SELECT _FUNC_(x'537061726b2053514c', x'537061726b');
       false
      > SELECT _FUNC_(x'537061726b2053514c', x'53514c');
       true
  """,
  since = "3.3.0",
  group = "string_funcs"
)
object EndsWithExpressionBuilder extends StringBinaryPredicateExpressionBuilderBase {
  override protected def createStringPredicate(left: Expression, right: Expression): Expression = {
    EndsWith(left, right)
  }
}

case class EndsWith(left: Expression, right: Expression) extends StringPredicate {
  override def compare(l: UTF8String, r: UTF8String): Boolean = {
    CollationSupport.EndsWith.exec(l, r, collationId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) =>
      CollationSupport.EndsWith.genCode(c1, c2, collationId))
  }
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): EndsWith = copy(left = newLeft, right = newRight)
}

/**
 * A function that checks if a UTF8 string is valid.
 */
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns true if `str` is a valid UTF-8 string, otherwise returns false.",
  arguments = """
    Arguments:
      * str - a string expression
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       true
      > SELECT _FUNC_(x'61');
       true
      > SELECT _FUNC_(x'80');
       false
      > SELECT _FUNC_(x'61C262');
       false
  """,
  since = "4.0.0",
  group = "string_funcs")
case class IsValidUTF8(input: Expression) extends RuntimeReplaceable with ImplicitCastInputTypes
  with UnaryLike[Expression] with NullIntolerant {

  override lazy val replacement: Expression = Invoke(input, "isValid", BooleanType)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def nodeName: String = "is_valid_utf8"

  override def nullable: Boolean = true

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): IsValidUTF8 = {
    copy(input = newChild)
  }

}

/**
 * A function that converts an invalid UTF8 string to a valid UTF8 string by replacing invalid
 * UTF-8 byte sequences with the Unicode replacement character (U+FFFD), according to the UNICODE
 * standard rules (Section 3.9, Paragraph D86, Table 3-7). Valid strings remain unchanged.
 */
// scalastyle:off
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns the original string if `str` is a valid UTF-8 string, " +
    "otherwise returns a new string whose invalid UTF8 byte sequences are replaced using the " +
    "UNICODE replacement character U+FFFD.",
  arguments = """
    Arguments:
      * str - a string expression
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       Spark
      > SELECT _FUNC_(x'61');
       a
      > SELECT _FUNC_(x'80');
       �
      > SELECT _FUNC_(x'61C262');
       a�b
  """,
  since = "4.0.0",
  group = "string_funcs")
// scalastyle:on
case class MakeValidUTF8(input: Expression) extends RuntimeReplaceable with ImplicitCastInputTypes
  with UnaryLike[Expression] with NullIntolerant {

  override lazy val replacement: Expression = Invoke(input, "makeValid", input.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def nodeName: String = "make_valid_utf8"

  override def nullable: Boolean = true

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): MakeValidUTF8 = {
    copy(input = newChild)
  }

}

/**
 * A function that validates a UTF8 string, throwing an exception if the string is invalid.
 */
// scalastyle:off
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns the original string if `str` is a valid UTF-8 string, " +
    "otherwise throws an exception.",
  arguments = """
    Arguments:
      * str - a string expression
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       Spark
      > SELECT _FUNC_(x'61');
       a
  """,
  since = "4.0.0",
  group = "string_funcs")
// scalastyle:on
case class ValidateUTF8(input: Expression) extends RuntimeReplaceable with ImplicitCastInputTypes
  with UnaryLike[Expression] with NullIntolerant {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    input.dataType,
    "validateUTF8String",
    Seq(input),
    inputTypes)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def nodeName: String = "validate_utf8"

  override def nullable: Boolean = true

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): ValidateUTF8 = {
    copy(input = newChild)
  }

}

/**
 * A function that tries to validate a UTF8 string, returning NULL if the string is invalid.
 */
// scalastyle:off
@ExpressionDescription(
  usage = "_FUNC_(str) - Returns the original string if `str` is a valid UTF-8 string, " +
    "otherwise returns NULL.",
  arguments = """
    Arguments:
      * str - a string expression
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark');
       Spark
      > SELECT _FUNC_(x'61');
       a
      > SELECT _FUNC_(x'80');
       NULL
      > SELECT _FUNC_(x'61C262');
       NULL
  """,
  since = "4.0.0",
  group = "string_funcs")
// scalastyle:on
case class TryValidateUTF8(input: Expression) extends RuntimeReplaceable with ImplicitCastInputTypes
  with UnaryLike[Expression] with NullIntolerant {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    input.dataType,
    "tryValidateUTF8String",
    Seq(input),
    inputTypes)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def nodeName: String = "try_validate_utf8"

  override def nullable: Boolean = true

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): TryValidateUTF8 = {
    copy(input = newChild)
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
  since = "2.3.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class StringReplace(srcExpr: Expression, searchExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = first.dataType.asInstanceOf[StringType].collationId

  def this(srcExpr: Expression, searchExpr: Expression) = {
    this(srcExpr, searchExpr, Literal(""))
  }

  override def nullSafeEval(srcEval: Any, searchEval: Any, replaceEval: Any): Any = {
    CollationSupport.StringReplace.exec(srcEval.asInstanceOf[UTF8String],
      searchEval.asInstanceOf[UTF8String], replaceEval.asInstanceOf[UTF8String], collationId);
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (src, search, replace) =>
      CollationSupport.StringReplace.genCode(src, search, replace, collationId))
  }

  override def dataType: DataType = srcExpr.dataType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, StringTypeAnyCollation)
  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

  override def prettyName: String = "replace"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringReplace =
    copy(srcExpr = newFirst, searchExpr = newSecond, replaceExpr = newThird)
}

object Overlay {

  def calculate(input: UTF8String, replace: UTF8String, pos: Int, len: Int): UTF8String = {
    val builder = new UTF8StringBuilder
    builder.append(input.substringSQL(1, pos - 1))
    builder.append(replace)
    // If you specify length, it must be a positive whole number or zero.
    // Otherwise it will be ignored.
    // The default value for length is the length of replace.
    val length = if (len >= 0) {
      len
    } else {
      replace.numChars
    }
    builder.append(input.substringSQL(pos + length, Int.MaxValue))
    builder.build()
  }

  def calculate(input: Array[Byte], replace: Array[Byte], pos: Int, len: Int): Array[Byte] = {
    // If you specify length, it must be a positive whole number or zero.
    // Otherwise it will be ignored.
    // The default value for length is the length of replace.
    val length = if (len >= 0) {
      len
    } else {
      replace.length
    }
    ByteArray.concat(ByteArray.subStringSQL(input, 1, pos - 1),
      replace, ByteArray.subStringSQL(input, pos + length, Int.MaxValue))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(input, replace, pos[, len]) - Replace `input` with `replace` that starts at `pos` and is of length `len`.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL' PLACING '_' FROM 6);
       Spark_SQL
      > SELECT _FUNC_('Spark SQL' PLACING 'CORE' FROM 7);
       Spark CORE
      > SELECT _FUNC_('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0);
       Spark ANSI SQL
      > SELECT _FUNC_('Spark SQL' PLACING 'tructured' FROM 2 FOR 4);
       Structured SQL
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8') PLACING encode('_', 'utf-8') FROM 6);
       Spark_SQL
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8') PLACING encode('CORE', 'utf-8') FROM 7);
       Spark CORE
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8') PLACING encode('ANSI ', 'utf-8') FROM 7 FOR 0);
       Spark ANSI SQL
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8') PLACING encode('tructured', 'utf-8') FROM 2 FOR 4);
       Structured SQL
  """,
  since = "3.0.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Overlay(input: Expression, replace: Expression, pos: Expression, len: Expression)
  extends QuaternaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(str: Expression, replace: Expression, pos: Expression) = {
    this(str, replace, pos, Literal.create(-1, IntegerType))
  }

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(
    TypeCollection(StringTypeAnyCollation, BinaryType),
    TypeCollection(StringTypeAnyCollation, BinaryType), IntegerType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val inputTypeCheck = super.checkInputDataTypes()
    if (inputTypeCheck.isSuccess) {
      TypeUtils.checkForSameTypeInputExpr(
        input.dataType :: replace.dataType :: Nil, prettyName)
    } else {
      inputTypeCheck
    }
  }

  private lazy val replaceFunc = input.dataType match {
    case _: StringType =>
      (inputEval: Any, replaceEval: Any, posEval: Int, lenEval: Int) => {
        Overlay.calculate(
          inputEval.asInstanceOf[UTF8String],
          replaceEval.asInstanceOf[UTF8String],
          posEval, lenEval)
      }
    case BinaryType =>
      (inputEval: Any, replaceEval: Any, posEval: Int, lenEval: Int) => {
        Overlay.calculate(
          inputEval.asInstanceOf[Array[Byte]],
          replaceEval.asInstanceOf[Array[Byte]],
          posEval, lenEval)
      }
  }

  override def nullSafeEval(inputEval: Any, replaceEval: Any, posEval: Any, lenEval: Any): Any = {
    replaceFunc(inputEval, replaceEval, posEval.asInstanceOf[Int], lenEval.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (input, replace, pos, len) =>
      "org.apache.spark.sql.catalyst.expressions.Overlay" +
        s".calculate($input, $replace, $pos, $len);")
  }

  override def first: Expression = input
  override def second: Expression = replace
  override def third: Expression = pos
  override def fourth: Expression = len

  override protected def withNewChildrenInternal(
      first: Expression, second: Expression, third: Expression, fourth: Expression): Overlay =
    copy(input = first, replace = second, pos = third, len = fourth)
}

object StringTranslate {

  /**
   * Build a translation dictionary from UTF8Strings. First, this method converts the input strings
   * to valid Java Strings. However, we avoid any behavior changes for the UTF8_BINARY collation,
   * but ensure that all other collations use `UTF8String.toValidString` to achieve this step.
   */
  def buildDict(matchingString: UTF8String, replaceString: UTF8String, collationId: Integer)
    : JMap[String, String] = {
    val isCollationAware = collationId == CollationFactory.UTF8_BINARY_COLLATION_ID
    val matching: String = if (isCollationAware) {
      matchingString.toString
    } else {
      matchingString.toValidString
    }
    val replace: String = if (isCollationAware) {
      replaceString.toString
    } else {
      replaceString.toValidString
    }
    buildDict(matching, replace)
  }

  /**
   * Build a translation dictionary from Strings. This method assumes that the input strings are
   * already valid. The result dictionary maps each character in `matching` to the corresponding
   * character in `replace`. If `replace` is shorter than `matching`, the extra characters in
   * `matching` will be mapped to null terminator, which causes characters to get deleted during
   * translation. If `replace` is longer than `matching`, the extra characters will be ignored.
   */
  private def buildDict(matching: String, replace: String): JMap[String, String] = {
    val dict = new HashMap[String, String]()
    var i = 0
    var j = 0

    while (i < matching.length) {
      val rep = if (j < replace.length) {
        val repCharCount = Character.charCount(replace.codePointAt(j))
        val repStr = replace.substring(j, j + repCharCount)
        j += repCharCount
        repStr
      } else {
        "\u0000"
      }

      val matchCharCount = Character.charCount(matching.codePointAt(i))
      val matchStr = matching.substring(i, i + matchCharCount)
      if (null == dict.get(matchStr)) {
        dict.put(matchStr, rep)
      }
      i += matchCharCount
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class StringTranslate(srcExpr: Expression, matchingExpr: Expression, replaceExpr: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  @transient private var lastMatching: UTF8String = _
  @transient private var lastReplace: UTF8String = _
  @transient private var dict: JMap[String, String] = _

  final lazy val collationId: Int = first.dataType.asInstanceOf[StringType].collationId

  override def nullSafeEval(srcEval: Any, matchingEval: Any, replaceEval: Any): Any = {
    if (matchingEval != lastMatching || replaceEval != lastReplace) {
      lastMatching = matchingEval.asInstanceOf[UTF8String].clone()
      lastReplace = replaceEval.asInstanceOf[UTF8String].clone()
      dict = StringTranslate.buildDict(lastMatching, lastReplace, collationId)
    }

    CollationSupport.StringTranslate.exec(srcEval.asInstanceOf[UTF8String], dict, collationId)
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
          .buildDict($termLastMatching, $termLastReplace, $collationId);
      }
      ${ev.value} = CollationSupport.StringTranslate.exec($src, $termDict, $collationId);
      """
    })
  }

  override def dataType: DataType = srcExpr.dataType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, StringTypeAnyCollation)
  override def first: Expression = srcExpr
  override def second: Expression = matchingExpr
  override def third: Expression = replaceExpr
  override def prettyName: String = "translate"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringTranslate =
    copy(srcExpr = newFirst, matchingExpr = newSecond, replaceExpr = newThird)
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class FindInSet(left: Expression, right: Expression) extends BinaryExpression
    with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = left.dataType.asInstanceOf[StringType].collationId

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation)

  override protected def nullSafeEval(word: Any, set: Any): Any = {
    CollationSupport.FindInSet.
      exec(word.asInstanceOf[UTF8String], set.asInstanceOf[UTF8String], collationId)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (word, set) => CollationSupport.FindInSet.
      genCode(word, set, collationId))
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "find_in_set"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): FindInSet = copy(left = newLeft, right = newRight)
}

trait String2TrimExpression extends Expression with ImplicitCastInputTypes {

  protected def srcStr: Expression
  protected def trimStr: Option[Expression]
  protected def direction: String

  override def children: Seq[Expression] = srcStr +: trimStr.toSeq
  override def dataType: DataType = srcStr.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringTypeAnyCollation)

  final lazy val collationId: Int = srcStr.dataType.asInstanceOf[StringType].collationId

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  protected def doEval(srcString: UTF8String): UTF8String
  protected def doEval(srcString: UTF8String, trimString: UTF8String): UTF8String

  override def eval(input: InternalRow): Any = {
    val srcString = srcStr.eval(input).asInstanceOf[UTF8String]
    if (srcString == null) {
      null
    } else if (trimStr.isDefined) {
      doEval(srcString, trimStr.get.eval(input).asInstanceOf[UTF8String])
    } else {
      doEval(srcString)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val srcString = evals.head

    if (evals.length == 1) {
      val stringTrimCode: String = this match {
        case _: StringTrim =>
          CollationSupport.StringTrim.genCode(srcString.value)
        case _: StringTrimLeft =>
          CollationSupport.StringTrimLeft.genCode(srcString.value)
        case _: StringTrimRight =>
          CollationSupport.StringTrimRight.genCode(srcString.value)
      }
      ev.copy(code = code"""
         |${srcString.code}
         |boolean ${ev.isNull} = false;
         |UTF8String ${ev.value} = null;
         |if (${srcString.isNull}) {
         |  ${ev.isNull} = true;
         |} else {
         |  ${ev.value} = $stringTrimCode;
         |}""".stripMargin)
    } else {
      val trimString = evals(1)
      val stringTrimCode: String = this match {
        case _: StringTrim =>
          CollationSupport.StringTrim.genCode(srcString.value, trimString.value, collationId)
        case _: StringTrimLeft =>
          CollationSupport.StringTrimLeft.genCode(srcString.value, trimString.value, collationId)
        case _: StringTrimRight =>
          CollationSupport.StringTrimRight.genCode(srcString.value, trimString.value, collationId)
      }
      ev.copy(code = code"""
         |${srcString.code}
         |boolean ${ev.isNull} = false;
         |UTF8String ${ev.value} = null;
         |if (${srcString.isNull}) {
         |  ${ev.isNull} = true;
         |} else {
         |  ${trimString.code}
         |  if (${trimString.isNull}) {
         |    ${ev.isNull} = true;
         |  } else {
         |    ${ev.value} = $stringTrimCode;
         |  }
         |}""".stripMargin)
    }
  }

  override def sql: String = if (trimStr.isDefined) {
    s"TRIM($direction ${trimStr.get.sql} FROM ${srcStr.sql})"
  } else {
    super.sql
  }
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
      > SELECT _FUNC_('SL' FROM 'SSparkSQLS');
       parkSQ
      > SELECT _FUNC_(BOTH 'SL' FROM 'SSparkSQLS');
       parkSQ
      > SELECT _FUNC_(LEADING 'SL' FROM 'SSparkSQLS');
       parkSQLS
      > SELECT _FUNC_(TRAILING 'SL' FROM 'SSparkSQLS');
       SSparkSQ
  """,
  since = "1.5.0",
  group = "string_funcs")
case class StringTrim(srcStr: Expression, trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "trim"

  override protected def direction: String = "BOTH"

  override def doEval(srcString: UTF8String): UTF8String =
    CollationSupport.StringTrim.exec(srcString)

  override def doEval(srcString: UTF8String, trimString: UTF8String): UTF8String =
    CollationSupport.StringTrim.exec(srcString, trimString, collationId)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren.last) else None)
}

/**
 * A function that takes a character string, removes the leading and trailing characters matching
 * with any character in the trim string, returns the new string.
 * trimStr: A character string to be trimmed from the source string, if it has multiple characters,
 * the function searches for each character in the source string, removes the characters from the
 * source string until it encounters the first non-match character.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str) - Removes the leading and trailing space characters from `str`.

    _FUNC_(str, trimStr) - Remove the leading and trailing `trimStr` characters from `str`.
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
      > SELECT _FUNC_(encode('    SparkSQL   ', 'utf-8'));
       SparkSQL
      > SELECT _FUNC_('SSparkSQLS', 'SL');
       parkSQ
      > SELECT _FUNC_(encode('SSparkSQLS', 'utf-8'), encode('SL', 'utf-8'));
       parkSQ
  """,
  since = "3.2.0",
  group = "string_funcs")
case class StringTrimBoth(srcStr: Expression, trimStr: Option[Expression], replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(srcStr: Expression, trimStr: Expression) = {
    this(srcStr, Option(trimStr), StringTrim(srcStr, trimStr))
  }

  def this(srcStr: Expression) = {
    this(srcStr, None, StringTrim(srcStr))
  }

  override def prettyName: String = "btrim"

  override def parameters: Seq[Expression] = srcStr +: trimStr.toSeq

  override protected def withNewChildInternal(newChild: Expression): StringTrimBoth =
    copy(replacement = newChild)
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
  """,
  since = "1.5.0",
  group = "string_funcs")
case class StringTrimLeft(srcStr: Expression, trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "ltrim"

  override protected def direction: String = "LEADING"

  override def doEval(srcString: UTF8String): UTF8String =
    CollationSupport.StringTrimLeft.exec(srcString)

  override def doEval(srcString: UTF8String, trimString: UTF8String): UTF8String =
    CollationSupport.StringTrimLeft.exec(srcString, trimString, collationId)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): StringTrimLeft =
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren.last) else None)
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
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class StringTrimRight(srcStr: Expression, trimStr: Option[Expression] = None)
  extends String2TrimExpression {

  def this(trimStr: Expression, srcStr: Expression) = this(srcStr, Option(trimStr))

  def this(srcStr: Expression) = this(srcStr, None)

  override def prettyName: String = "rtrim"

  override protected def direction: String = "TRAILING"

  override def doEval(srcString: UTF8String): UTF8String =
    CollationSupport.StringTrimRight.exec(srcString)

  override def doEval(srcString: UTF8String, trimString: UTF8String): UTF8String =
    CollationSupport.StringTrimRight.exec(srcString, trimString, collationId)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): StringTrimRight =
    copy(
      srcStr = newChildren.head,
      trimStr = if (trimStr.isDefined) Some(newChildren.last) else None)
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class StringInstr(str: Expression, substr: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = left.dataType.asInstanceOf[StringType].collationId

  override def left: Expression = str
  override def right: Expression = substr
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation)

  override def nullSafeEval(string: Any, sub: Any): Any = {
    CollationSupport.StringInstr.
      exec(string.asInstanceOf[UTF8String], sub.asInstanceOf[UTF8String], collationId) + 1
  }

  override def prettyName: String = "instr"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      defineCodeGen(ctx, ev, (string, substring) =>
        CollationSupport.StringInstr.genCode(string, substring, collationId) + " + 1")
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): StringInstr = copy(str = newLeft, substr = newRight)
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class SubstringIndex(strExpr: Expression, delimExpr: Expression, countExpr: Expression)
 extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = first.dataType.asInstanceOf[StringType].collationId

  override def dataType: DataType = strExpr.dataType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, IntegerType)
  override def first: Expression = strExpr
  override def second: Expression = delimExpr
  override def third: Expression = countExpr
  override def prettyName: String = "substring_index"

  override def nullSafeEval(str: Any, delim: Any, count: Any): Any = {
    CollationSupport.SubstringIndex.exec(str.asInstanceOf[UTF8String],
      delim.asInstanceOf[UTF8String], count.asInstanceOf[Int], collationId);
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (str, delim, count) =>
      CollationSupport.SubstringIndex.genCode(str, delim, count, collationId))
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): SubstringIndex =
    copy(strExpr = newFirst, delimExpr = newSecond, countExpr = newThird)
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class StringLocate(substr: Expression, str: Expression, start: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  def this(substr: Expression, str: Expression) = {
    this(substr, str, Literal(1))
  }

  final lazy val collationId: Int = first.dataType.asInstanceOf[StringType].collationId

  override def first: Expression = substr
  override def second: Expression = str
  override def third: Expression = start
  override def nullable: Boolean = substr.nullable || str.nullable
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, IntegerType)

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
            CollationSupport.StringLocate.exec(l.asInstanceOf[UTF8String],
              r.asInstanceOf[UTF8String], s.asInstanceOf[Int] - 1, collationId) + 1;
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
              ${ev.value} = CollationSupport.StringLocate.exec(${strGen.value},
              ${substrGen.value}, ${startGen.value} - 1, $collationId) + 1;
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

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("locate")

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringLocate =
    copy(substr = newFirst, str = newSecond, start = newThird)

}

trait PadExpressionBuilderBase extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val behaviorChangeEnabled = !SQLConf.get.getConf(SQLConf.LEGACY_LPAD_RPAD_BINARY_TYPE_AS_STRING)
    val numArgs = expressions.length
    if (numArgs == 2) {
      if (expressions(0).dataType == BinaryType && behaviorChangeEnabled) {
        BinaryPad(funcName, expressions(0), expressions(1), Literal(Array[Byte](0)))
      } else {
        createStringPad(expressions(0), expressions(1), Literal(" "))
      }
    } else if (numArgs == 3) {
      if (expressions(0).dataType == BinaryType && expressions(2).dataType == BinaryType
        && behaviorChangeEnabled) {
        BinaryPad(funcName, expressions(0), expressions(1), expressions(2))
      } else {
        createStringPad(expressions(0), expressions(1), expressions(2))
      }
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2, 3), numArgs)
    }
  }

  protected def createStringPad(str: Expression, len: Expression, pad: Expression): Expression
}

@ExpressionDescription(
  usage = """
    _FUNC_(str, len[, pad]) - Returns `str`, left-padded with `pad` to a length of `len`.
      If `str` is longer than `len`, the return value is shortened to `len` characters or bytes.
      If `pad` is not specified, `str` will be padded to the left with space characters if it is
      a character string, and with zeros if it is a byte sequence.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('hi', 5, '??');
       ???hi
      > SELECT _FUNC_('hi', 1, '??');
       h
      > SELECT _FUNC_('hi', 5);
          hi
      > SELECT hex(_FUNC_(unhex('aabb'), 5));
       000000AABB
      > SELECT hex(_FUNC_(unhex('aabb'), 5, unhex('1122')));
       112211AABB
  """,
  since = "1.5.0",
  group = "string_funcs")
object LPadExpressionBuilder extends PadExpressionBuilderBase {
  override def createStringPad(str: Expression, len: Expression, pad: Expression): Expression = {
    StringLPad(str, len, pad)
  }
}

case class StringLPad(str: Expression, len: Expression, pad: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def first: Expression = str
  override def second: Expression = len
  override def third: Expression = pad

  override def dataType: DataType = str.dataType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, IntegerType, StringTypeAnyCollation)

  override def nullSafeEval(string: Any, len: Any, pad: Any): Any = {
    string.asInstanceOf[UTF8String].lpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (string, len, pad) => {
      s"$string.lpad($len, $pad)"
    })
  }

  override def prettyName: String = "lpad"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringLPad =
    copy(str = newFirst, len = newSecond, pad = newThird)
}

case class BinaryPad(funcName: String, str: Expression, len: Expression, pad: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {
  assert(funcName == "lpad" || funcName == "rpad")

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ByteArray],
    BinaryType,
    funcName,
    Seq(str, len, pad),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType, BinaryType)

  override def nodeName: String = funcName

  override def children: Seq[Expression] = Seq(str, len, pad)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(str = newChildren(0), len = newChildren(1), pad = newChildren(2))
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(str, len[, pad]) - Returns `str`, right-padded with `pad` to a length of `len`.
      If `str` is longer than `len`, the return value is shortened to `len` characters.
      If `pad` is not specified, `str` will be padded to the right with space characters if it is
      a character string, and with zeros if it is a binary string.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('hi', 5, '??');
       hi???
      > SELECT _FUNC_('hi', 1, '??');
       h
      > SELECT _FUNC_('hi', 5);
       hi
      > SELECT hex(_FUNC_(unhex('aabb'), 5));
       AABB000000
      > SELECT hex(_FUNC_(unhex('aabb'), 5, unhex('1122')));
       AABB112211
  """,
  since = "1.5.0",
  group = "string_funcs")
object RPadExpressionBuilder extends PadExpressionBuilderBase {
  override def createStringPad(str: Expression, len: Expression, pad: Expression): Expression = {
    StringRPad(str, len, pad)
  }
}

case class StringRPad(str: Expression, len: Expression, pad: Expression = Literal(" "))
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def first: Expression = str
  override def second: Expression = len
  override def third: Expression = pad

  override def dataType: DataType = str.dataType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, IntegerType, StringTypeAnyCollation)

  override def nullSafeEval(string: Any, len: Any, pad: Any): Any = {
    string.asInstanceOf[UTF8String].rpad(len.asInstanceOf[Int], pad.asInstanceOf[UTF8String])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (string, len, pad) => {
      s"$string.rpad($len, $pad)"
    })
  }

  override def prettyName: String = "rpad"

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): StringRPad =
    copy(str = newFirst, len = newSecond, pad = newThird)
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
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class FormatString(children: Expression*) extends Expression with ImplicitCastInputTypes {

  if (children.nonEmpty && !SQLConf.get.getConf(SQLConf.ALLOW_ZERO_INDEX_IN_FORMAT_STRING)) {
    checkArgumentIndexNotZero(children(0))
  }


  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children(0).nullable
  override def dataType: DataType = children(0).dataType

  override def inputTypes: Seq[AbstractDataType] =
    StringTypeAnyCollation :: List.fill(children.size - 1)(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      throw QueryCompilationErrors.wrongNumArgsError(
          toSQLId(prettyName), Seq("> 0"), children.length
      )
    } else {
      super.checkInputDataTypes()
    }
  }

  override def eval(input: InternalRow): Any = {
    val pattern = children(0).eval(input)
    if (pattern == null) {
      null
    } else {
      val formatter = new java.util.Formatter(Locale.US)
      val arglist = children.tail.map(_.eval(input).asInstanceOf[AnyRef])
      UTF8String.fromString(
        formatter.format(pattern.asInstanceOf[UTF8String].toString, arglist: _*).toString)
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
    ev.copy(code = code"""
      ${pattern.code}
      boolean ${ev.isNull} = ${pattern.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $formatter $form = new $formatter(${classOf[Locale].getName}.US);
        Object[] $argList = new Object[$numArgLists];
        $argListCodes
        ${ev.value} = UTF8String.fromString(
          $form.format(${pattern.value}.toString(), $argList).toString());
      }""")
  }

  override def prettyName: String = getTagValue(
    FunctionRegistry.FUNC_ALIAS).getOrElse("format_string")

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): FormatString = FormatString(newChildren: _*)

  /**
   * SPARK-37013: The `formatSpecifier` defined in `j.u.Formatter` as follows:
   *  "%[argument_index$][flags][width][.precision][t]conversion"
   * The optional `argument_index` is a decimal integer indicating the position of the argument
   * in the argument list. The first argument is referenced by "1$", the second by "2$", etc.
   * However, for the illegal definition of "%0$", Java 8 and Java 11 uses it as "%1$",
   * and Java 17 throws IllegalFormatArgumentIndexException(Illegal format argument index = 0).
   * Therefore, manually check that the pattern string not contains "%0$" to ensure consistent
   * behavior of Java 8, Java 11 and Java 17.
   */
  private def checkArgumentIndexNotZero(expression: Expression): Unit = expression match {
    case StringLiteral(pattern) if pattern.contains("%0$") =>
      throw QueryCompilationErrors.zeroArgumentIndexError()
    case _ => // do nothing
  }
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
  since = "1.5.0",
  group = "string_funcs")
case class InitCap(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  final lazy val collationId: Int = child.dataType.asInstanceOf[StringType].collationId

  // Flag to indicate whether to use ICU instead of JVM case mappings for UTF8_BINARY collation.
  private final lazy val useICU = SQLConf.get.getConf(SQLConf.ICU_CASE_MAPPINGS_ENABLED)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)
  override def dataType: DataType = child.dataType

  override def nullSafeEval(string: Any): Any = {
    CollationSupport.InitCap.exec(string.asInstanceOf[UTF8String], collationId, useICU)
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, str => CollationSupport.InitCap.genCode(str, collationId, useICU))
  }

  override protected def withNewChildInternal(newChild: Expression): InitCap =
    copy(child = newChild)
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
  since = "1.5.0",
  group = "string_funcs")
case class StringRepeat(str: Expression, times: Expression)
  extends BinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = str
  override def right: Expression = times
  override def dataType: DataType = str.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation, IntegerType)

  override def nullSafeEval(string: Any, n: Any): Any = {
    string.asInstanceOf[UTF8String].repeat(n.asInstanceOf[Integer])
  }

  override def prettyName: String = "repeat"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (l, r) => s"($l).repeat($r)")
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): StringRepeat = copy(str = newLeft, times = newRight)
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
  since = "1.5.0",
  group = "string_funcs")
case class StringSpace(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = SQLConf.get.defaultStringType
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

  override protected def withNewChildInternal(newChild: Expression): StringSpace =
    copy(child = newChild)
}

/**
 * A function that takes a substring of its first argument starting at a given position.
 * Defined for String and Binary types.
 *
 * NOTE: that this is not zero based, but 1-based index. The first character in str has index 1.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str, pos[, len]) - Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of byte array that starts at `pos` and is of length `len`.

    _FUNC_(str FROM pos[ FOR len]]) - Returns the substring of `str` that starts at `pos` and is of length `len`, or the slice of byte array that starts at `pos` and is of length `len`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('Spark SQL', 5);
       k SQL
      > SELECT _FUNC_('Spark SQL', -3);
       SQL
      > SELECT _FUNC_('Spark SQL', 5, 1);
       k
      > SELECT _FUNC_('Spark SQL' FROM 5);
       k SQL
      > SELECT _FUNC_('Spark SQL' FROM -3);
       SQL
      > SELECT _FUNC_('Spark SQL' FROM 5 FOR 1);
       k
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8'), 5);
       k SQL
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Substring(str: Expression, pos: Expression, len: Expression)
  extends TernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  def this(str: Expression, pos: Expression) = {
    this(str, pos, Literal(Integer.MAX_VALUE))
  }

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringTypeAnyCollation, BinaryType), IntegerType, IntegerType)

  override def first: Expression = str
  override def second: Expression = pos
  override def third: Expression = len

  override def nullSafeEval(string: Any, pos: Any, len: Any): Any = {
    str.dataType match {
      case _: StringType => string.asInstanceOf[UTF8String]
        .substringSQL(pos.asInstanceOf[Int], len.asInstanceOf[Int])
      case BinaryType => ByteArray.subStringSQL(string.asInstanceOf[Array[Byte]],
        pos.asInstanceOf[Int], len.asInstanceOf[Int])
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    defineCodeGen(ctx, ev, (string, pos, len) => {
      str.dataType match {
        case _: StringType => s"$string.substringSQL($pos, $len)"
        case BinaryType => s"${classOf[ByteArray].getName}.subStringSQL($string, $pos, $len)"
      }
    })
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Substring =
    copy(str = newFirst, pos = newSecond, len = newThird)

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
  since = "2.3.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Right(str: Expression, len: Expression) extends RuntimeReplaceable
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  override lazy val replacement: Expression = If(
    IsNull(str),
    Literal(null, str.dataType),
    If(
      LessThanOrEqual(len, Literal(0)),
      Literal(UTF8String.EMPTY_UTF8, str.dataType),
      new Substring(str, UnaryMinus(len))
    )
  )

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation, IntegerType)
  override def left: Expression = str
  override def right: Expression = len
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, len = newRight)
  }
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
      > SELECT _FUNC_(encode('Spark SQL', 'utf-8'), 3);
       Spa
  """,
  since = "2.3.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Left(str: Expression, len: Expression) extends RuntimeReplaceable
  with ImplicitCastInputTypes with BinaryLike[Expression] {

  override lazy val replacement: Expression = Substring(str, Literal(1), len)

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(StringTypeAnyCollation, BinaryType), IntegerType)
  }

  override def left: Expression = str
  override def right: Expression = len
  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): Expression = {
    copy(str = newLeft, len = newRight)
  }
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
      > SELECT _FUNC_(x'537061726b2053514c');
       9
      > SELECT CHAR_LENGTH('Spark SQL ');
       10
      > SELECT CHARACTER_LENGTH('Spark SQL ');
       10
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Length(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringTypeAnyCollation, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case _: StringType => value.asInstanceOf[UTF8String].numChars
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case _: StringType => defineCodeGen(ctx, ev, c => s"($c).numChars()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Length = copy(child = newChild)
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
      > SELECT _FUNC_(x'537061726b2053514c');
       72
  """,
  since = "2.3.0",
  group = "string_funcs")
case class BitLength(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringTypeAnyCollation, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case _: StringType => value.asInstanceOf[UTF8String].numBytes * 8
    case BinaryType => value.asInstanceOf[Array[Byte]].length * 8
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case _: StringType => defineCodeGen(ctx, ev, c => s"($c).numBytes() * 8")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length * 8")
    }
  }

  override def prettyName: String = "bit_length"

  override protected def withNewChildInternal(newChild: Expression): BitLength =
    copy(child = newChild)
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
      > SELECT _FUNC_(x'537061726b2053514c');
       9
  """,
  since = "2.3.0",
  group = "string_funcs")
case class OctetLength(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringTypeAnyCollation, BinaryType))

  protected override def nullSafeEval(value: Any): Any = child.dataType match {
    case _: StringType => value.asInstanceOf[UTF8String].numBytes
    case BinaryType => value.asInstanceOf[Array[Byte]].length
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.dataType match {
      case _: StringType => defineCodeGen(ctx, ev, c => s"($c).numBytes()")
      case BinaryType => defineCodeGen(ctx, ev, c => s"($c).length")
    }
  }

  override def prettyName: String = "octet_length"

  override protected def withNewChildInternal(newChild: Expression): OctetLength =
    copy(child = newChild)
}

/**
 * A function that return the Levenshtein distance between the two given strings.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(str1, str2[, threshold]) - Returns the Levenshtein distance between the two given strings. If threshold is set and distance more than it, return -1.""",
  examples = """
    Examples:
      > SELECT _FUNC_('kitten', 'sitting');
       3
      > SELECT _FUNC_('kitten', 'sitting', 2);
       -1
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Levenshtein(
    left: Expression,
    right: Expression,
    threshold: Option[Expression] = None)
  extends Expression
  with ImplicitCastInputTypes
  with NullIntolerant{

  def this(left: Expression, right: Expression, threshold: Expression) =
    this(left, right, Option(threshold))

  def this(left: Expression, right: Expression) = this(left, right, None)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length > 3 || children.length < 2) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName), Seq(2, 3), children.length)
    } else {
      super.checkInputDataTypes()
    }
  }

  override def inputTypes: Seq[AbstractDataType] = threshold match {
    case Some(_) => Seq(StringTypeAnyCollation, StringTypeAnyCollation, IntegerType)
    case _ => Seq(StringTypeAnyCollation, StringTypeAnyCollation)
  }

  override def children: Seq[Expression] = threshold match {
    case Some(value) => Seq(left, right, value)
    case _ => Seq(left, right)
  }

  override def dataType: DataType = IntegerType

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    threshold match {
      case Some(_) => copy(left = newChildren(0), right = newChildren(1),
        threshold = Some(newChildren(2)))
      case _ => copy(left = newChildren(0), right = newChildren(1))
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val leftEval = left.eval(input)
    if (leftEval == null) return null
    val rightEval = right.eval(input)
    if (rightEval == null) return null
    val thresholdEval = threshold.map(_.eval(input))
    thresholdEval match {
      case Some(v) =>
        leftEval.asInstanceOf[UTF8String].levenshteinDistance(rightEval.asInstanceOf[UTF8String],
          v.asInstanceOf[Int])
      case _ =>
        leftEval.asInstanceOf[UTF8String].levenshteinDistance(rightEval.asInstanceOf[UTF8String])
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    threshold match {
      case Some(_) => genCodeWithThreshold(ctx, ev, children(2))
      case _ => genCodeWithoutThreshold(ctx, ev)
    }
  }

  private def genCodeWithThreshold(
      ctx: CodegenContext,
      ev: ExprCode,
      thresholdExpr: Expression): ExprCode = {
    val leftGen = children.head.genCode(ctx)
    val rightGen = children(1).genCode(ctx)
    val thresholdGen = thresholdExpr.genCode(ctx)
    val resultCode = s"${ev.value} = ${leftGen.value}.levenshteinDistance(" +
      s"${rightGen.value}, ${thresholdGen.value});"
    if (nullable) {
      val nullSafeEval =
        leftGen.code.toString + ctx.nullSafeExec(children.head.nullable, leftGen.isNull) {
          rightGen.code.toString + ctx.nullSafeExec(children(1).nullable, rightGen.isNull) {
            thresholdGen.code.toString +
              ctx.nullSafeExec(thresholdExpr.nullable, thresholdGen.isNull) {
                s"""
                  ${ev.isNull} = false;
                  $resultCode
                 """
              }
          }
        }
      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      val eval = leftGen.code + rightGen.code + thresholdGen.code
      ev.copy(code = code"""
        $eval
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }

  private def genCodeWithoutThreshold(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = children.head.genCode(ctx)
    val rightGen = children(1).genCode(ctx)
    val resultCode = s"${ev.value} = ${leftGen.value}.levenshteinDistance(${rightGen.value});"
    if (nullable) {
      val nullSafeEval =
        leftGen.code.toString + ctx.nullSafeExec(children.head.nullable, leftGen.isNull) {
          rightGen.code.toString + ctx.nullSafeExec(children(1).nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false;
              $resultCode
             """
          }
        }
      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      val eval = leftGen.code + rightGen.code
      ev.copy(code = code"""
        $eval
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
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
  since = "1.5.0",
  group = "string_funcs")
case class SoundEx(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def nullSafeEval(input: Any): Any = input.asInstanceOf[UTF8String].soundex()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c.soundex()")
  }

  override protected def withNewChildInternal(newChild: Expression): SoundEx =
    copy(child = newChild)
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
  since = "1.5.0",
  group = "string_funcs")
case class Ascii(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  protected override def nullSafeEval(string: Any): Any = {
    // only pick the first character to reduce the `toString` cost
    val firstCharStr = string.asInstanceOf[UTF8String].substring(0, 1)
    if (firstCharStr.numChars > 0) {
      firstCharStr.toString.codePointAt(0)
    } else {
      0
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (child) => {
      val firstCharStr = ctx.freshName("firstCharStr")
      s"""
        UTF8String $firstCharStr = $child.substring(0, 1);
        if ($firstCharStr.numChars() > 0) {
          ${ev.value} = $firstCharStr.toString().codePointAt(0);
        } else {
          ${ev.value} = 0;
        }
       """})
  }

  override protected def withNewChildInternal(newChild: Expression): Ascii = copy(child = newChild)
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
  since = "2.3.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Chr(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = SQLConf.get.defaultStringType
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

  override protected def withNewChildInternal(newChild: Expression): Chr = copy(child = newChild)
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
      > SELECT _FUNC_(x'537061726b2053514c');
       U3BhcmsgU1FM
  """,
  since = "1.5.0",
  group = "string_funcs")
case class Base64(child: Expression, chunkBase64: Boolean)
  extends UnaryExpression with RuntimeReplaceable with ImplicitCastInputTypes {

  def this(expr: Expression) = this(expr, SQLConf.get.chunkBase64StringEnabled)

  override val dataType: DataType = SQLConf.get.defaultStringType
  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override lazy val replacement: Expression = StaticInvoke(
    classOf[Base64],
    dataType,
    "encode",
    Seq(child, Literal(chunkBase64, BooleanType)),
    Seq(BinaryType, BooleanType),
    returnNullable = false)

  override def toString: String = s"$prettyName($child)"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

object Base64 {
  def apply(expr: Expression): Base64 = new Base64(expr)

  private lazy val nonChunkEncoder = JBase64.getMimeEncoder(-1, Array())

  def encode(input: Array[Byte], chunkBase64: Boolean): UTF8String = {
    val encoder = if (chunkBase64) {
      JBase64.getMimeEncoder
    } else {
      nonChunkEncoder
    }
    UTF8String.fromBytes(encoder.encode(input))
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
  since = "1.5.0",
  group = "string_funcs")
case class UnBase64(child: Expression, failOnError: Boolean = false)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  def this(expr: Expression) = this(expr, false)

  protected override def nullSafeEval(string: Any): Any = {
    if (failOnError && !UnBase64.isValidBase64(string.asInstanceOf[UTF8String])) {
      // The failOnError is set only from `ToBinary` function - hence we might safely set `hint`
      // parameter to `try_to_binary`.
      throw QueryExecutionErrors.invalidInputInConversionError(
        BinaryType,
        string.asInstanceOf[UTF8String],
        UTF8String.fromString("BASE64"),
        "try_to_binary")
    }
    JBase64.getMimeDecoder.decode(string.asInstanceOf[UTF8String].toString)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, child => {
      val maybeValidateInputCode = if (failOnError) {
        val unbase64 = UnBase64.getClass.getName.stripSuffix("$")
        val binaryType = ctx.addReferenceObj("to", BinaryType, BinaryType.getClass.getName)
        s"""
           |if (!$unbase64.isValidBase64($child)) {
           |  throw QueryExecutionErrors.invalidInputInConversionError(
           |    $binaryType,
           |    $child,
           |    UTF8String.fromString("BASE64"),
           |    "try_to_binary");
           |}
       """.stripMargin
      } else {
        ""
      }
      s"""
         $maybeValidateInputCode
         ${ev.value} = ${classOf[JBase64].getName}.getMimeDecoder().decode($child.toString());
       """})
  }

  override protected def withNewChildInternal(newChild: Expression): UnBase64 =
    copy(child = newChild, failOnError)
}

object UnBase64 {
  def isValidBase64(srcString: UTF8String) : Boolean = {
    // We use RFC4648. The valid base64 string should contain zero or more groups of 4 symbols plus
    // last group consisting of 2-4 valid symbols and optional padding.
    // Last group should contain at least 2 valid symbols and up to 2 padding characters `=`.
    // Valid symbols include - (A-Za-z0-9+/). Each group might contain arbitrary number of
    // whitespaces which are ignored.
    // If padding is present - last group should include exactly 4 symbols.
    // Examples:
    //    "abcd"      - Valid, single group of 4 valid symbols
    //    "abc d"     - Valid, single group of 4 valid symbols, whitespace is skipped
    //    "abc?"      - Invalid, group contains invalid symbol `?`
    //    "abcdA"     - Invalid, last group should contain at least 2 valid symbols
    //    "abcdAE"    - Valid, a group of 4 valid symbols and a group of 2 valid symbols
    //    "abcdAE=="  - Valid, last group includes 2 padding symbols and total number of symbols
    //                  in a group is 4.
    //    "abcdAE="   - Invalid, last group include padding symbols, therefore it should have
    //                  exactly 4 symbols but contains only 3.
    //    "ab==tm+1"  - Invalid, nothing should be after padding.
    var position = 0
    var padSize = 0
    for (c: Char <- srcString.toString) {
      c match {
        case a
          if (a >= '0' && a <= '9')
            || (a >= 'A' && a <= 'Z')
            || (a >= 'a' && a <= 'z')
            || a == '/' || a == '+' =>
          if (padSize != 0) return false // Padding symbols should conclude the string.
          position += 1
        case '=' =>
          padSize += 1
          // Last group preceding padding should have 2 or more symbols. Padding size should be 1 or
          // less.
          if (padSize > 2 || position % 4 < 2) {
            return false
          }
        case ws if Character.isWhitespace(ws) =>
          if (padSize != 0) { // Padding symbols should conclude the string.
            return false
          }
        case _ => return false
      }
    }
    if (padSize > 0) { // When padding is present last group should have exactly 4 symbols.
      (position + padSize) % 4 == 0
    } else { // When padding is absent last group should include 2 or more symbols.
      position % 4 != 1
    }
  }
}

object Decode {
  def createExpr(params: Seq[Expression]): Expression = {
    params.length match {
      case 0 | 1 =>
        throw QueryCompilationErrors.wrongNumArgsError("decode", "2", params.length)
      case 2 => StringDecode(params.head, params.last)
      case _ =>
        val input = params.head
        val other = params.tail
        val itr = other.iterator
        var default: Expression = Literal.create(null, SQLConf.get.defaultStringType)
        val branches = ArrayBuffer.empty[(Expression, Expression)]
        while (itr.hasNext) {
          val search = itr.next()
          if (itr.hasNext) {
            val condition = EqualNullSafe(input, search)
            branches += ((condition, itr.next()))
          } else {
            default = search
          }
        }
        CaseWhen(branches.toSeq, default)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(bin, charset) - Decodes the first argument using the second argument character set. If either argument is null, the result will also be null.

    _FUNC_(expr, search, result [, search, result ] ... [, default]) - Compares expr
      to each search value in order. If expr is equal to a search value, _FUNC_ returns
      the corresponding result. If no match is found, then it returns default. If default
      is omitted, it returns null.
  """,
  arguments = """
    Arguments:
      * bin - a binary expression to decode
      * charset - one of the charsets 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'UTF-32' to decode `bin` into a STRING. It is case insensitive.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(encode('abc', 'utf-8'), 'utf-8');
       abc
      > SELECT _FUNC_(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
       San Francisco
      > SELECT _FUNC_(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
       Non domestic
      > SELECT _FUNC_(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');
       NULL
      > SELECT _FUNC_(null, 6, 'Spark', NULL, 'SQL', 4, 'rocks');
       SQL
  """,
  since = "1.5.0",
  note = """
    _FUNC_(expr, search, result [, search, result ] ... [, default]) is supported since 3.2.0
  """,
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Decode(params: Seq[Expression], replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  def this(params: Seq[Expression]) = this(params, Decode.createExpr(params))

  override def parameters: Seq[Expression] = params

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(replacement = newChild)
  }
}

case class StringDecode(
    bin: Expression,
    charset: Expression,
    legacyCharsets: Boolean,
    legacyErrorAction: Boolean)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  def this(bin: Expression, charset: Expression) =
    this(bin, charset, SQLConf.get.legacyJavaCharsets, SQLConf.get.legacyCodingErrorAction)

  override val dataType: DataType = SQLConf.get.defaultStringType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, StringTypeAnyCollation)
  override def prettyName: String = "decode"
  override def toString: String = s"$prettyName($bin, $charset)"

  override lazy val replacement: Expression = StaticInvoke(
    classOf[StringDecode],
    SQLConf.get.defaultStringType,
    "decode",
    Seq(bin, charset, Literal(legacyCharsets), Literal(legacyErrorAction)),
    Seq(BinaryType, StringTypeAnyCollation, BooleanType, BooleanType))

  override def children: Seq[Expression] = Seq(bin, charset)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(bin = newChildren(0), charset = newChildren(1))
}

object StringDecode {
  def apply(bin: Expression, charset: Expression): StringDecode = new StringDecode(bin, charset)
  def decode(
      input: Array[Byte],
      charset: UTF8String,
      legacyCharsets: Boolean,
      legacyErrorAction: Boolean): UTF8String = {
    val fromCharset = charset.toString
    val decoder = CharsetProvider.newDecoder(fromCharset, legacyCharsets, legacyErrorAction)
    try {
      val cb = decoder.decode(ByteBuffer.wrap(input))
      UTF8String.fromString(cb.toString)
    } catch {
      case _: CharacterCodingException =>
        throw QueryExecutionErrors.malformedCharacterCoding("decode", fromCharset)
    }
  }
}

/**
 * Encode the given string to a binary using the provided charset.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, charset) - Encodes the first argument using the second argument character set. If either argument is null, the result will also be null.",
  arguments = """
    Arguments:
      * str - a string expression
      * charset - one of the charsets 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16', 'UTF-32' to encode `str` into a BINARY. It is case insensitive.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('abc', 'utf-8');
       abc
  """,
  since = "1.5.0",
  group = "string_funcs")
// scalastyle:on line.size.limit
case class Encode(
    str: Expression,
    charset: Expression,
    legacyCharsets: Boolean,
    legacyErrorAction: Boolean)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  def this(value: Expression, charset: Expression) =
    this(value, charset, SQLConf.get.legacyJavaCharsets, SQLConf.get.legacyCodingErrorAction)

  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation)

  override lazy val replacement: Expression = StaticInvoke(
    classOf[Encode],
    BinaryType,
    "encode",
    Seq(
      str, charset, Literal(legacyCharsets, BooleanType), Literal(legacyErrorAction, BooleanType)),
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, BooleanType, BooleanType))

  override def toString: String = s"$prettyName($str, $charset)"

  override def children: Seq[Expression] = Seq(str, charset)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(str = newChildren.head, charset = newChildren(1))
}

object Encode {
  def apply(value: Expression, charset: Expression): Encode = new Encode(value, charset)

  def encode(
      input: UTF8String,
      charset: UTF8String,
      legacyCharsets: Boolean,
      legacyErrorAction: Boolean): Array[Byte] = {
    val toCharset = charset.toString
    if (input.numBytes == 0 || "UTF-8".equalsIgnoreCase(toCharset)) {
      return input.getBytes
    }
    val encoder = CharsetProvider.newEncoder(toCharset, legacyCharsets, legacyErrorAction)
    try {
      val bb = encoder.encode(CharBuffer.wrap(input.toString))
      JavaUtils.bufferToArray(bb)
    } catch {
      case _: CharacterCodingException =>
        throw QueryExecutionErrors.malformedCharacterCoding("encode", toCharset)
    }
  }
}

/**
 * Converts the input expression to a binary value based on the supplied format.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str[, fmt]) - Converts the input `str` to a binary value based on the supplied `fmt`.
      `fmt` can be a case-insensitive string literal of "hex", "utf-8", "utf8", or "base64".
      By default, the binary format for conversion is "hex" if `fmt` is omitted.
      The function returns NULL if at least one of the input parameters is NULL.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('abc', 'utf-8');
       abc
  """,
  since = "3.3.0",
  group = "string_funcs")
case class ToBinary(
    expr: Expression,
    format: Option[Expression],
    nullOnInvalidFormat: Boolean = false) extends RuntimeReplaceable
    with ImplicitCastInputTypes {

  @transient lazy val fmt: String = format.map { f =>
    val value = f.eval()
    if (value == null) {
      null
    } else {
      value.asInstanceOf[UTF8String].toString.toLowerCase(Locale.ROOT)
    }
  }.getOrElse("hex")

  override lazy val replacement: Expression = if (fmt == null) {
    Literal(null, BinaryType)
  } else {
    fmt match {
      case "hex" => Unhex(expr, failOnError = true)
      case "utf-8" | "utf8" => Encode(expr, Literal("UTF-8"))
      case "base64" => UnBase64(expr, failOnError = true)
      case _ => Literal(null, BinaryType)
    }
  }

  def this(expr: Expression) = this(expr, None, false)

  def this(expr: Expression, format: Expression) =
    this(expr, Some(format), false)

  override def prettyName: String = "to_binary"

  override def children: Seq[Expression] = expr +: format.toSeq

  override def inputTypes: Seq[AbstractDataType] = children.map(_ => StringTypeAnyCollation)

  override def checkInputDataTypes(): TypeCheckResult = {
    def isValidFormat: Boolean = {
      fmt == null || Set("hex", "utf-8", "utf8", "base64").contains(fmt)
    }
    format match {
      case Some(f) =>
        if (f.foldable && (f.dataType.isInstanceOf[StringType] || f.dataType == NullType)) {
          if (isValidFormat || nullOnInvalidFormat) {
            super.checkInputDataTypes()
          } else {
            DataTypeMismatch(
              errorSubClass = "INVALID_ARG_VALUE",
              messageParameters = Map(
                "inputName" -> "fmt",
                "requireType" -> s"case-insensitive ${toSQLType(StringTypeAnyCollation)}",
                "validValues" -> "'hex', 'utf-8', 'utf8', or 'base64'",
                "inputValue" -> toSQLValue(fmt, f.dataType)
              )
            )
          }
        } else if (!f.foldable) {
          DataTypeMismatch(
            errorSubClass = "NON_FOLDABLE_INPUT",
            messageParameters = Map(
              "inputName" -> toSQLId("fmt"),
              "inputType" -> toSQLType(StringTypeAnyCollation),
              "inputExpr" -> toSQLExpr(f)
            )
          )
        } else {
          DataTypeMismatch(
            errorSubClass = "INVALID_ARG_VALUE",
            messageParameters = Map(
              "inputName" -> "fmt",
              "requireType" -> s"case-insensitive ${toSQLType(StringTypeAnyCollation)}",
              "validValues" -> "'hex', 'utf-8', 'utf8', or 'base64'",
              "inputValue" -> toSQLValue(f.eval(), f.dataType)
            )
          )
        }
      case _ => super.checkInputDataTypes()
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
      if (format.isDefined) {
        copy(expr = newChildren.head, format = Some(newChildren.last))
      } else {
        copy(expr = newChildren.head)
      }
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
  since = "1.5.0",
  group = "string_funcs")
case class FormatNumber(x: Expression, d: Expression)
  extends BinaryExpression with ExpectsInputTypes with NullIntolerant {

  override def left: Expression = x
  override def right: Expression = d
  override def dataType: DataType = SQLConf.get.defaultStringType
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq(NumericType, TypeCollection(IntegerType, StringTypeAnyCollation))

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
      case _: StringType =>
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
        case _: StringType =>
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

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): FormatNumber = copy(x = newLeft, d = newRight)
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
  since = "2.0.0",
  group = "string_funcs")
case class Sentences(
    str: Expression,
    language: Expression = Literal(""),
    country: Expression = Literal(""))
  extends TernaryExpression with ImplicitCastInputTypes with CodegenFallback {

  def this(str: Expression) = this(str, Literal(""), Literal(""))
  def this(str: Expression, language: Expression) = this(str, language, Literal(""))

  override def nullable: Boolean = true
  override def dataType: DataType =
    ArrayType(ArrayType(str.dataType, containsNull = false), containsNull = false)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, StringTypeAnyCollation)
  override def first: Expression = str
  override def second: Expression = language
  override def third: Expression = country

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

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Sentences =
    copy(str = newFirst, language = newSecond, country = newThird)

}

/**
 * Splits a given string by a specified delimiter and return splits into a
 * GenericArrayData. This expression is different from `split` function as
 * `split` takes regex expression as the pattern to split strings while this
 * expression take delimiter (a string without carrying special meaning on its
 * characters, thus is not treated as regex) to split strings.
 */
case class StringSplitSQL(
    str: Expression,
    delimiter: Expression) extends BinaryExpression with NullIntolerant {
  override def dataType: DataType = ArrayType(str.dataType, containsNull = false)
  final lazy val collationId: Int = left.dataType.asInstanceOf[StringType].collationId
  override def left: Expression = str
  override def right: Expression = delimiter

  override def nullSafeEval(string: Any, delimiter: Any): Any = {
    val strings = CollationSupport.StringSplitSQL.exec(string.asInstanceOf[UTF8String],
      delimiter.asInstanceOf[UTF8String], collationId)
    new GenericArrayData(strings.asInstanceOf[Array[Any]])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, delimiter) => {
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"${ev.value} = new $arrayClass(" +
        s"${CollationSupport.StringSplitSQL.genCode(str, delimiter, collationId)});"
    })
  }

  override def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression): StringSplitSQL =
    copy(str = newFirst, delimiter = newSecond)
}

/**
 * Splits a given string by a specified delimiter and returns the requested part.
 * If any input is null, returns null.
 * If index is out of range of split parts, return empty string.
 * If index is 0, throws an ArrayIndexOutOfBoundsException.
 */
@ExpressionDescription(
  usage =
    """
    _FUNC_(str, delimiter, partNum) - Splits `str` by delimiter and return
      requested part of the split (1-based). If any input is null, returns null.
      if `partNum` is out of range of split parts, returns empty string. If `partNum` is 0,
      throws an error. If `partNum` is negative, the parts are counted backward from the
      end of the string. If the `delimiter` is an empty string, the `str` is not split.
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_('11.12.13', '.', 3);
       13
  """,
  since = "3.3.0",
  group = "string_funcs")
case class SplitPart (
    str: Expression,
    delimiter: Expression,
    partNum: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {
  override lazy val replacement: Expression =
    ElementAt(StringSplitSQL(str, delimiter), partNum, Some(Literal.create("", str.dataType)),
      false)
  override def nodeName: String = "split_part"
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeAnyCollation, StringTypeAnyCollation, IntegerType)
  def children: Seq[Expression] = Seq(str, delimiter, partNum)
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(str = newChildren.apply(0), delimiter = newChildren.apply(1),
      partNum = newChildren.apply(2))
  }
}

/**
 * A internal function that converts the empty string to null for partition values.
 * This function should be only used in V1Writes.
 */
case class Empty2Null(child: Expression) extends UnaryExpression with String2StringExpression {
  override def convert(v: UTF8String): UTF8String = if (v.numBytes() == 0) null else v

  override def nullable: Boolean = true

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""if ($c.numBytes() == 0) {
         |  ${ev.isNull} = true;
         |  ${ev.value} = null;
         |} else {
         |  ${ev.value} = $c;
         |}""".stripMargin
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Empty2Null =
    copy(child = newChild)
}

/**
 * Function to check if a given number string is a valid Luhn number. Returns true, if the number
 * string is a valid Luhn number, false otherwise.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(str ) - Checks that a string of digits is valid according to the Luhn algorithm.
    This checksum function is widely applied on credit card numbers and government identification
    numbers to distinguish valid numbers from mistyped, incorrect numbers.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('8112189876');
       true
      > SELECT _FUNC_('79927398713');
       true
      > SELECT _FUNC_('79927398714');
       false
  """,
  since = "3.5.0",
  group = "string_funcs")
case class Luhncheck(input: Expression) extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    BooleanType,
    "isLuhnNumber",
    Seq(input),
    inputTypes)

  override def inputTypes: Seq[AbstractDataType] = Seq(StringTypeAnyCollation)

  override def prettyName: String = "luhn_check"

  override def children: Seq[Expression] = Seq(input)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(newChildren(0))
}

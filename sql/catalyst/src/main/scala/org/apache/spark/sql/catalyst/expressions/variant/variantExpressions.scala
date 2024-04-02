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

package org.apache.spark.sql.catalyst.expressions.variant

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.analysis.ExpressionBuilder
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, VARIANT_GET}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.types.variant.VariantUtil.Type
import org.apache.spark.unsafe.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr) - Parse a JSON string as an Variant value. Throw an exception when the string is not valid JSON value.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1,"b":0.8}');
       {"a":1,"b":0.8}
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
case class ParseJson(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    VariantExpressionEvalUtils.getClass,
    VariantType,
    "parseJson",
    Seq(child),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = VariantType

  override def prettyName: String = "parse_json"

  override protected def withNewChildInternal(newChild: Expression): ParseJson =
    copy(child = newChild)
}

object VariantPathParser extends RegexParsers {
  // A path segment in the `VariantGet` expression represents either an object key access or an
  // array index access.
  type PathSegment = Either[String, Int]

  private def root: Parser[Char] = '$'

  // Parse index segment like `[123]`.
  private def index: Parser[PathSegment] =
    for {
      index <- '[' ~> "\\d+".r <~ ']'
    } yield {
      scala.util.Right(index.toInt)
    }

  // Parse key segment like `.name`, `['name']`, or `["name"]`.
  private def key: Parser[PathSegment] =
    for {
      key <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^\\'\\?]+".r <~ "']" |
        "[\"" ~> "[^\\\"\\?]+".r <~ "\"]"
    } yield {
      scala.util.Left(key)
    }

  private val parser: Parser[List[PathSegment]] = phrase(root ~> rep(key | index))

  def parse(str: String): Option[Array[PathSegment]] = {
    this.parseAll(parser, str) match {
      case Success(result, _) => Some(result.toArray)
      case _ => None
    }
  }
}

/**
 * The implementation for `variant_get` and `try_variant_get` expressions. Extracts a sub-variant
 * value according to a path and cast it into a concrete data type.
 * @param child The source variant value to extract from.
 * @param path A literal path expression. It has the same format as the JSON path.
 * @param targetType The target data type to cast into. Any non-nullable annotations are ignored.
 * @param failOnError Controls whether the expression should throw an exception or return null if
 *                    the cast fails.
 * @param timeZoneId A string identifier of a time zone. It is required by timestamp-related casts.
 */
case class VariantGet(
    child: Expression,
    path: Expression,
    targetType: DataType,
    failOnError: Boolean,
    timeZoneId: Option[String] = None)
    extends BinaryExpression
    with TimeZoneAwareExpression
    with NullIntolerant
    with ExpectsInputTypes
    with QueryErrorsBase {
  override def checkInputDataTypes(): TypeCheckResult = {
    val check = super.checkInputDataTypes()
    if (check.isFailure) {
      check
    } else if (!path.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("path"),
          "inputType" -> toSQLType(path.dataType),
          "inputExpr" -> toSQLExpr(path)))
    } else if (!VariantGet.checkDataType(targetType)) {
      DataTypeMismatch(
        errorSubClass = "CAST_WITHOUT_SUGGESTION",
        messageParameters =
          Map("srcType" -> toSQLType(VariantType), "targetType" -> toSQLType(targetType)))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override lazy val dataType: DataType = targetType.asNullable

  @transient private lazy val parsedPath = {
    val pathValue = path.eval().toString
    VariantPathParser.parse(pathValue).getOrElse {
      throw QueryExecutionErrors.invalidVariantGetPath(pathValue, prettyName)
    }
  }

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(VARIANT_GET)

  override def inputTypes: Seq[AbstractDataType] = Seq(VariantType, StringType)

  override def prettyName: String = if (failOnError) "variant_get" else "try_variant_get"

  override def nullable: Boolean = true

  protected override def nullSafeEval(input: Any, path: Any): Any = {
    VariantGet.variantGet(
      input.asInstanceOf[VariantVal],
      parsedPath,
      dataType,
      failOnError,
      timeZoneId)
  }

  protected override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.genCode(ctx)
    val tmp = ctx.freshVariable("tmp", classOf[Object])
    val parsedPathArg = ctx.addReferenceObj("parsedPath", parsedPath)
    val dataTypeArg = ctx.addReferenceObj("dataType", dataType)
    val zoneIdArg = ctx.addReferenceObj("zoneId", timeZoneId)
    val code = code"""
      ${childCode.code}
      boolean ${ev.isNull} = ${childCode.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        Object $tmp = org.apache.spark.sql.catalyst.expressions.variant.VariantGet.variantGet(
          ${childCode.value}, $parsedPathArg, $dataTypeArg, $failOnError, $zoneIdArg);
        if ($tmp == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = (${CodeGenerator.boxedType(dataType)})$tmp;
        }
      }
    """
    ev.copy(code = code)
  }

  override def left: Expression = child

  override def right: Expression = path

  override protected def withNewChildrenInternal(
      newChild: Expression,
      newPath: Expression): VariantGet = copy(child = newChild, path = newPath)

  override def withTimeZone(timeZoneId: String): VariantGet = copy(timeZoneId = Option(timeZoneId))
}

case object VariantGet {
  /**
   * Returns whether a data type can be cast into/from variant. For scalar types, we allow a subset
   * of them. For nested types, we reject map types with a non-string key type.
   */
  def checkDataType(dataType: DataType): Boolean = dataType match {
    case _: NumericType | BooleanType | _: StringType | BinaryType | _: DatetimeType |
        VariantType =>
      true
    case ArrayType(elementType, _) => checkDataType(elementType)
    case MapType(StringType, valueType, _) => checkDataType(valueType)
    case StructType(fields) => fields.forall(f => checkDataType(f.dataType))
    case _ => false
  }

  /** The actual implementation of the `VariantGet` expression. */
  def variantGet(
      input: VariantVal,
      parsedPath: Array[VariantPathParser.PathSegment],
      dataType: DataType,
      failOnError: Boolean,
      zoneId: Option[String]): Any = {
    var v = new Variant(input.getValue, input.getMetadata)
    for (path <- parsedPath) {
      v = path match {
        case scala.util.Left(key) if v.getType == Type.OBJECT => v.getFieldByKey(key)
        case scala.util.Right(index) if v.getType == Type.ARRAY => v.getElementAtIndex(index)
        case _ => null
      }
      if (v == null) return null
    }
    VariantGet.cast(v, dataType, failOnError, zoneId)
  }

  /**
   * Cast a variant `v` into a target data type `dataType`. If the variant represents a variant
   * null, the result is always a SQL NULL. The cast may fail due to an illegal type combination
   * (e.g., cast a variant int to binary), or an invalid input valid (e.g, cast a variant string
   * "hello" to int). If the cast fails, throw an exception when `failOnError` is true, or return a
   * SQL NULL when it is false.
   */
  def cast(v: Variant, dataType: DataType, failOnError: Boolean, zoneId: Option[String]): Any = {
    def invalidCast(): Any =
      if (failOnError) throw QueryExecutionErrors.invalidVariantCast(v.toJson, dataType) else null

    if (dataType == VariantType) return new VariantVal(v.getValue, v.getMetadata)
    val variantType = v.getType
    if (variantType == Type.NULL) return null
    dataType match {
      case _: AtomicType =>
        val input = variantType match {
          case Type.OBJECT | Type.ARRAY =>
            return if (dataType.isInstanceOf[StringType]) {
              UTF8String.fromString(v.toJson)
            } else {
              invalidCast()
            }
          case Type.BOOLEAN => v.getBoolean
          case Type.LONG => v.getLong
          case Type.STRING => UTF8String.fromString(v.getString)
          case Type.DOUBLE => v.getDouble
          case Type.DECIMAL => Decimal(v.getDecimal)
          // We have handled other cases and should never reach here. This case is only intended
          // to by pass the compiler exhaustiveness check.
          case _ => throw QueryExecutionErrors.unreachableError()
        }
        // We mostly use the `Cast` expression to implement the cast. However, `Cast` silently
        // ignores the overflow in the long/decimal -> timestamp cast, and we want to enforce
        // strict overflow checks.
        input match {
          case l: Long if dataType == TimestampType =>
            try Math.multiplyExact(l, MICROS_PER_SECOND)
            catch {
              case _: ArithmeticException => invalidCast()
            }
          case d: Decimal if dataType == TimestampType =>
            try {
              d.toJavaBigDecimal
                .multiply(new java.math.BigDecimal(MICROS_PER_SECOND))
                .toBigInteger
                .longValueExact()
            } catch {
              case _: ArithmeticException => invalidCast()
            }
          case _ =>
            val inputLiteral = Literal(input)
            if (Cast.canAnsiCast(inputLiteral.dataType, dataType)) {
              val result = Cast(inputLiteral, dataType, zoneId, EvalMode.TRY).eval()
              if (result == null) invalidCast() else result
            } else {
              invalidCast()
            }
        }
      case ArrayType(elementType, _) =>
        if (variantType == Type.ARRAY) {
          val size = v.arraySize()
          val array = new Array[Any](size)
          for (i <- 0 until size) {
            array(i) = cast(v.getElementAtIndex(i), elementType, failOnError, zoneId)
          }
          new GenericArrayData(array)
        } else {
          invalidCast()
        }
      case MapType(StringType, valueType, _) =>
        if (variantType == Type.OBJECT) {
          val size = v.objectSize()
          val keyArray = new Array[Any](size)
          val valueArray = new Array[Any](size)
          for (i <- 0 until size) {
            val field = v.getFieldAtIndex(i)
            keyArray(i) = UTF8String.fromString(field.key)
            valueArray(i) = cast(field.value, valueType, failOnError, zoneId)
          }
          ArrayBasedMapData(keyArray, valueArray)
        } else {
          invalidCast()
        }
      case st @ StructType(fields) =>
        if (variantType == Type.OBJECT) {
          val row = new GenericInternalRow(fields.length)
          for (i <- 0 until v.objectSize()) {
            val field = v.getFieldAtIndex(i)
            st.getFieldIndex(field.key) match {
              case Some(idx) =>
                row.update(idx, cast(field.value, fields(idx).dataType, failOnError, zoneId))
              case _ =>
            }
          }
          row
        } else {
          invalidCast()
        }
    }
  }
}

abstract class VariantGetExpressionBuilderBase(failOnError: Boolean) extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 2) {
      VariantGet(expressions(0), expressions(1), VariantType, failOnError)
    } else if (numArgs == 3) {
      VariantGet(
        expressions(0),
        expressions(1),
        ExprUtils.evalTypeExpr(expressions(2)),
        failOnError)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2, 3), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(v, path[, type]) - Extracts a sub-variant from `v` according to `path`, and then cast the sub-variant to `type`. When `type` is omitted, it is default to `variant`. Returns null if the path does not exist. Throws an exception if the cast fails.",
  examples = """
    Examples:
      > SELECT _FUNC_(parse_json('{"a": 1}'), '$.a', 'int');
       1
      > SELECT _FUNC_(parse_json('{"a": 1}'), '$.b', 'int');
       NULL
      > SELECT _FUNC_(parse_json('[1, "2"]'), '$[1]', 'string');
       2
      > SELECT _FUNC_(parse_json('[1, "2"]'), '$[2]', 'string');
       NULL
      > SELECT _FUNC_(parse_json('[1, "hello"]'), '$[1]');
       "hello"
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
object VariantGetExpressionBuilder extends VariantGetExpressionBuilderBase(true)

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(v, path[, type]) - Extracts a sub-variant from `v` according to `path`, and then cast the sub-variant to `type`. When `type` is omitted, it is default to `variant`. Returns null if the path does not exist or the cast fails.",
  examples = """
    Examples:
      > SELECT _FUNC_(parse_json('{"a": 1}'), '$.a', 'int');
       1
      > SELECT _FUNC_(parse_json('{"a": 1}'), '$.b', 'int');
       NULL
      > SELECT _FUNC_(parse_json('[1, "2"]'), '$[1]', 'string');
       2
      > SELECT _FUNC_(parse_json('[1, "2"]'), '$[2]', 'string');
       NULL
      > SELECT _FUNC_(parse_json('[1, "hello"]'), '$[1]');
       "hello"
      > SELECT _FUNC_(parse_json('[1, "hello"]'), '$[1]', 'int');
       NULL
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
object TryVariantGetExpressionBuilder extends VariantGetExpressionBuilderBase(false)

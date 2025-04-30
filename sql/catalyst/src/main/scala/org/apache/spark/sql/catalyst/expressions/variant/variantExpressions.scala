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

import java.time.ZoneId

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, GeneratorBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.json.JsonInferSchema
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, VARIANT_GET}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, QuotingUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.types.variant.VariantUtil.Type
import org.apache.spark.unsafe.types._


/**
 * The implementation for `parse_json` and `try_parse_json` expressions. Parse a JSON string as a
 * Variant value.
 * @param child The string value to parse as a variant.
 * @param failOnError Controls whether the expression should throw an exception or return null if
 *                    the string does not represent a valid JSON value.
 */
case class ParseJson(child: Expression, failOnError: Boolean = true)
  extends UnaryExpression with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    VariantExpressionEvalUtils.getClass,
    VariantType,
    "parseJson",
    Seq(
      child,
      Literal(SQLConf.get.getConf(SQLConf.VARIANT_ALLOW_DUPLICATE_KEYS), BooleanType),
      Literal(failOnError, BooleanType)),
    inputTypes :+ BooleanType :+ BooleanType,
    returnNullable = !failOnError)

  override def inputTypes: Seq[AbstractDataType] =
    StringTypeWithCollation(supportsTrimCollation = true) :: Nil

  override def dataType: DataType = VariantType

  override def prettyName: String = if (failOnError) "parse_json" else "try_parse_json"

  override protected def withNewChildInternal(newChild: Expression): ParseJson =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Check if a variant value is a variant null. Returns true if and only if the input is a variant null and false otherwise (including in the case of SQL NULL).",
  examples = """
    Examples:
      > SELECT _FUNC_(parse_json('null'));
       true
      > SELECT _FUNC_(parse_json('"null"'));
       false
      > SELECT _FUNC_(parse_json('13'));
       false
      > SELECT _FUNC_(parse_json(null));
       false
      > SELECT _FUNC_(variant_get(parse_json('{"a":null, "b":"spark"}'), "$.c"));
       false
      > SELECT _FUNC_(variant_get(parse_json('{"a":null, "b":"spark"}'), "$.a"));
       true
  """,
  since = "4.0.0",
  group = "variant_funcs")
// scalastyle:on line.size.limit
case class IsVariantNull(child: Expression) extends UnaryExpression
  with Predicate with ExpectsInputTypes with RuntimeReplaceable {

  override lazy val replacement: Expression = StaticInvoke(
    VariantExpressionEvalUtils.getClass,
    BooleanType,
    "isVariantNull",
    Seq(child),
    inputTypes,
    propagateNull = false,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = Seq(VariantType)

  override def prettyName: String = "is_variant_null"

  override protected def withNewChildInternal(newChild: Expression): IsVariantNull =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr) - Convert a nested input (array/map/struct) into a variant where maps and structs are converted to variant objects which are unordered unlike SQL structs. Input maps can only have string keys.",
  examples = """
    Examples:
      > SELECT _FUNC_(named_struct('a', 1, 'b', 2));
       {"a":1,"b":2}
      > SELECT _FUNC_(array(1, 2, 3));
       [1,2,3]
      > SELECT _FUNC_(array(named_struct('a', 1)));
       [{"a":1}]
      > SELECT _FUNC_(array(map("a", 2)));
       [{"a":2}]
  """,
  since = "4.0.0",
  group = "variant_funcs")
// scalastyle:on line.size.limit
case class ToVariantObject(child: Expression)
    extends UnaryExpression
    with QueryErrorsBase {
  override def nullIntolerant: Boolean = true
  override val dataType: DataType = VariantType

  // Only accept nested types at the root but any types can be nested inside.
  override def checkInputDataTypes(): TypeCheckResult = {
    val checkResult: Boolean = child.dataType match {
      case _: StructType | _: ArrayType | _: MapType =>
        VariantGet.checkDataType(child.dataType, allowStructsAndMaps = true)
      case _ => false
    }
    if (!checkResult) {
      DataTypeMismatch(
        errorSubClass = "CAST_WITHOUT_SUGGESTION",
        messageParameters =
          Map("srcType" -> toSQLType(child.dataType), "targetType" -> toSQLType(VariantType)))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def prettyName: String = "to_variant_object"

  override protected def withNewChildInternal(newChild: Expression): ToVariantObject =
    copy(child = newChild)

  protected override def nullSafeEval(input: Any): Any =
    VariantExpressionEvalUtils.castToVariant(input, child.dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.genCode(ctx)
    val cls = variant.VariantExpressionEvalUtils.getClass.getName.stripSuffix("$")
    val fromArg = ctx.addReferenceObj("from", child.dataType)
    val javaType = JavaCode.javaType(VariantType)
    val code =
      code"""
        ${childCode.code}
        boolean ${ev.isNull} = ${childCode.isNull};
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(VariantType)};
        if (!${childCode.isNull}) {
          ${ev.value} = $cls.castToVariant(${childCode.value}, $fromArg);
        }
      """
    ev.copy(code = code)
  }
}

// A path segment in the `VariantGet` expression represents either an object key access or an array
// index access.
sealed abstract class VariantPathSegment extends Serializable

case class ObjectExtraction(key: String) extends VariantPathSegment

case class ArrayExtraction(index: Int) extends VariantPathSegment

object VariantPathParser extends RegexParsers {
  private def root: Parser[Char] = '$'

  // Parse index segment like `[123]`.
  private def index: Parser[VariantPathSegment] =
    for {
      index <- '[' ~> "\\d+".r <~ ']'
    } yield {
      ArrayExtraction(index.toInt)
    }

  override def skipWhitespace: Boolean = false

  // Parse key segment like `.name`, `['name']`, or `["name"]`.
  private def key: Parser[VariantPathSegment] =
    for {
      key <- '.' ~> "[^\\.\\[]+".r | "['" ~> "[^']*".r <~ "']" |
        "[\"" ~> """[^"]*""".r <~ "\"]"
    } yield {
      ObjectExtraction(key)
    }

  private val parser: Parser[List[VariantPathSegment]] = phrase(root ~> rep(key | index))

  def parse(str: String): Option[Array[VariantPathSegment]] = {
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
    with ExpectsInputTypes
    with QueryErrorsBase {
  override def checkInputDataTypes(): TypeCheckResult = {
    val check = super.checkInputDataTypes()
    if (check.isFailure) {
      check
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

  @transient private lazy val parsedPath: Option[Array[VariantPathSegment]] = {
    if (path.foldable) {
      val pathValue = path.eval().toString
      Some(VariantGet.getParsedPath(pathValue, prettyName))
    } else {
      None
    }
  }

  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(VARIANT_GET)

  override def inputTypes: Seq[AbstractDataType] =
    Seq(VariantType, StringTypeWithCollation(supportsTrimCollation = true))

  override def prettyName: String = if (failOnError) "variant_get" else "try_variant_get"

  override def nullable: Boolean = true
  override def nullIntolerant: Boolean = true

  private lazy val castArgs = VariantCastArgs(
    failOnError,
    timeZoneId,
    zoneId)

  protected override def nullSafeEval(input: Any, path: Any): Any = parsedPath match {
    case Some(pp) =>
      VariantGet.variantGet(input.asInstanceOf[VariantVal], pp, dataType, castArgs)
    case _ =>
      VariantGet.variantGet(input.asInstanceOf[VariantVal], path.asInstanceOf[UTF8String], dataType,
        castArgs, prettyName)
  }

  protected override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tmp = ctx.freshVariable("tmp", classOf[Object])
    val childCode = child.genCode(ctx)
    val dataTypeArg = ctx.addReferenceObj("dataType", dataType)
    val castArgsArg = ctx.addReferenceObj("castArgs", castArgs)
    val (pathCode, parsedPathArg) = if (parsedPath.isEmpty) {
      val pathCode = path.genCode(ctx)
      (pathCode, pathCode.value)
    } else {
      (
        new ExprCode(EmptyBlock, FalseLiteral, TrueLiteral),
        ctx.addReferenceObj("parsedPath", parsedPath.get)
      )
    }
    val code = code"""
      ${childCode.code}
      ${pathCode.code}
      boolean ${ev.isNull} = ${childCode.isNull} || ${pathCode.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        Object $tmp = org.apache.spark.sql.catalyst.expressions.variant.VariantGet.variantGet(
          ${childCode.value}, $parsedPathArg, $dataTypeArg,
          $castArgsArg${if (parsedPath.isEmpty) s""", "$prettyName"""" else ""});
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

// Several parameters used by `VariantGet.cast`. Packed together to simplify parameter passing.
case class VariantCastArgs(
    failOnError: Boolean,
    zoneStr: Option[String],
    zoneId: ZoneId)

case object VariantGet {
  /**
   * Returns whether a data type can be cast into/from variant. For scalar types, we allow a subset
   * of them. For nested types, we reject map types with a non-string key type.
   */
  def checkDataType(dataType: DataType, allowStructsAndMaps: Boolean = true): Boolean =
    dataType match {
    case CharType(_) | VarcharType(_) => false
    case _: NumericType | BooleanType | _: StringType | BinaryType | _: DatetimeType |
        VariantType =>
      true
    case ArrayType(elementType, _) => checkDataType(elementType, allowStructsAndMaps)
    case MapType(_: StringType, valueType, _) if allowStructsAndMaps =>
        checkDataType(valueType, allowStructsAndMaps)
    case StructType(fields) if allowStructsAndMaps =>
        fields.forall(f => checkDataType(f.dataType, allowStructsAndMaps))
    case _ => false
  }

  /**
   * Get parsed Array[VariantPathSegment] from string representing path
   */
  def getParsedPath(pathValue: String, prettyName: String): Array[VariantPathSegment] = {
    VariantPathParser.parse(pathValue).getOrElse {
      throw QueryExecutionErrors.invalidVariantGetPath(pathValue, prettyName)
    }
  }

  /** The actual implementation of the `VariantGet` expression. */
  def variantGet(
      input: VariantVal,
      parsedPath: Array[VariantPathSegment],
      dataType: DataType,
      castArgs: VariantCastArgs): Any = {
    var v = new Variant(input.getValue, input.getMetadata)
    for (path <- parsedPath) {
      v = path match {
        case ObjectExtraction(key) if v.getType == Type.OBJECT => v.getFieldByKey(key)
        case ArrayExtraction(index) if v.getType == Type.ARRAY => v.getElementAtIndex(index)
        case _ => null
      }
      if (v == null) return null
    }
    VariantGet.cast(v, dataType, castArgs)
  }

  /**
   * Implementation of the `VariantGet` expression where the path is provided as a UTF8String
   */
  def variantGet(
      input: VariantVal,
      path: UTF8String,
      dataType: DataType,
      castArgs: VariantCastArgs,
      prettyName: String): Any = {
    val pathValue = path.toString
    val parsedPath = VariantGet.getParsedPath(pathValue, prettyName)
    variantGet(input, parsedPath, dataType, castArgs)
  }

  /**
   * A simple wrapper of the `cast` function that takes `Variant` rather than `VariantVal`. The
   * `Cast` expression uses it and makes the implementation simpler.
   */
  def cast(input: VariantVal, dataType: DataType, castArgs: VariantCastArgs): Any = {
    val v = new Variant(input.getValue, input.getMetadata)
    VariantGet.cast(v, dataType, castArgs)
  }

  /**
   * Cast a variant `v` into a target data type `dataType`. If the variant represents a variant
   * null, the result is always a SQL NULL. The cast may fail due to an illegal type combination
   * (e.g., cast a variant int to binary), or an invalid input valid (e.g, cast a variant string
   * "hello" to int). If the cast fails, throw an exception when `failOnError` is true, or return a
   * SQL NULL when it is false.
   */
  def cast(v: Variant, dataType: DataType, castArgs: VariantCastArgs): Any = {
    def invalidCast(): Any = {
      if (castArgs.failOnError) {
        throw QueryExecutionErrors.invalidVariantCast(v.toJson(castArgs.zoneId), dataType)
      } else {
        null
      }
    }

    if (dataType == VariantType) {
      // Build a new variant, in order to strip off any unnecessary metadata.
      val builder = new VariantBuilder(false)
      builder.appendVariant(v)
      val result = builder.result()
      return new VariantVal(result.getValue, result.getMetadata)
    }
    val variantType = v.getType
    if (variantType == Type.NULL) return null
    if (variantType == Type.UUID) {
      // There's no UUID type in Spark. We only allow it to be cast to string.
      if (dataType == StringType) {
        return UTF8String.fromString(v.getUuid.toString)
      } else {
        return invalidCast()
      }
    }
    dataType match {
      case _: AtomicType =>
        val input = variantType match {
          case Type.OBJECT | Type.ARRAY =>
            return if (dataType.isInstanceOf[StringType]) {
              UTF8String.fromString(v.toJson(castArgs.zoneId))
            } else {
              invalidCast()
            }
          case Type.BOOLEAN => Literal(v.getBoolean, BooleanType)
          case Type.LONG => Literal(v.getLong, LongType)
          case Type.STRING => Literal(UTF8String.fromString(v.getString),
            StringType)
          case Type.DOUBLE => Literal(v.getDouble, DoubleType)
          case Type.DECIMAL =>
            val d = Decimal(v.getDecimal)
            Literal(d, DecimalType(d.precision, d.scale))
          case Type.DATE => Literal(v.getLong.toInt, DateType)
          case Type.TIMESTAMP => Literal(v.getLong, TimestampType)
          case Type.TIMESTAMP_NTZ => Literal(v.getLong, TimestampNTZType)
          case Type.FLOAT => Literal(v.getFloat, FloatType)
          case Type.BINARY => Literal(v.getBinary, BinaryType)
          // We have handled other cases and should never reach here. This case is only intended
          // to by pass the compiler exhaustiveness check.
          case _ => throw new SparkRuntimeException(
            errorClass = "UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT",
            messageParameters = Map("id" -> v.getTypeInfo.toString)
          )
        }
        input.dataType match {
          case LongType if dataType == TimestampType =>
            try castLongToTimestamp(input.value.asInstanceOf[Long])
            catch {
              case _: ArithmeticException => invalidCast()
            }
          case _: DecimalType if dataType == TimestampType =>
            try castDecimalToTimestamp(input.value.asInstanceOf[Decimal])
            catch {
              case _: ArithmeticException => invalidCast()
            }
          case _ =>
            if (Cast.canAnsiCast(input.dataType, dataType)) {
              val result = Cast(input, dataType, castArgs.zoneStr, EvalMode.TRY).eval()
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
            array(i) = cast(v.getElementAtIndex(i), elementType, castArgs)
          }
          new GenericArrayData(array)
        } else {
          invalidCast()
        }
      case MapType(_: StringType, valueType, _) =>
        if (variantType == Type.OBJECT) {
          val size = v.objectSize()
          val keyArray = new Array[Any](size)
          val valueArray = new Array[Any](size)
          for (i <- 0 until size) {
            val field = v.getFieldAtIndex(i)
            keyArray(i) = UTF8String.fromString(field.key)
            valueArray(i) = cast(field.value, valueType, castArgs)
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
                row.update(idx, cast(field.value, fields(idx).dataType, castArgs))
              case _ =>
            }
          }
          row
        } else {
          invalidCast()
        }
    }
  }

  // We mostly use the `Cast` expression to implement the cast, but we need some custom logic for
  // certain type combinations.
  //
  // `castLongToTimestamp/castDecimalToTimestamp`: `Cast` silently ignores the overflow in the
  // long/decimal -> timestamp cast, and we want to enforce strict overflow checks. They both throw
  // an `ArithmeticException` when overflow happens.
  def castLongToTimestamp(input: Long): Long =
    Math.multiplyExact(input, MICROS_PER_SECOND)

  def castDecimalToTimestamp(input: Decimal): Long = {
    val multiplier = new java.math.BigDecimal(MICROS_PER_SECOND)
    input.toJavaBigDecimal.multiply(multiplier).toBigInteger.longValueExact()
  }

  // Cast decimal to string, but strip any trailing zeros. We don't have to call it if the decimal
  // is returned by `Variant.getDecimal`, which already strips any trailing zeros. But we need it
  // if the decimal is produced by Spark internally, e.g., on a shredded decimal produced by the
  // Spark Parquet reader.
  def castDecimalToString(input: Decimal): UTF8String =
    UTF8String.fromString(input.toJavaBigDecimal.stripTrailingZeros.toPlainString)
}

abstract class ParseJsonExpressionBuilderBase(failOnError: Boolean) extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 1) {
      ParseJson(expressions.head, failOnError)
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1), numArgs)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr) - Parse a JSON string as a Variant value. Throw an exception when the string is not valid JSON value.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1,"b":0.8}');
       {"a":1,"b":0.8}
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
object ParseJsonExpressionBuilder extends ParseJsonExpressionBuilderBase(true)

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(jsonStr) - Parse a JSON string as a Variant value. Return NULL when the string is not valid JSON value.",
  examples = """
    Examples:
      > SELECT _FUNC_('{"a":1,"b":0.8}');
       {"a":1,"b":0.8}
      > SELECT _FUNC_('{"a":1,');
       NULL
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
// scalastyle:on line.size.limit
object TryParseJsonExpressionBuilder extends ParseJsonExpressionBuilderBase(false)

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

case class VariantExplode(child: Expression) extends UnaryExpression with Generator
  with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(VariantType)

  override def prettyName: String = "variant_explode"

  override protected def withNewChildInternal(newChild: Expression): VariantExplode =
    copy(child = newChild)

  override def eval(input: InternalRow): IterableOnce[InternalRow] = {
    val inputVariant = child.eval(input).asInstanceOf[VariantVal]
    VariantExplode.variantExplode(inputVariant, inputVariant == null)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.genCode(ctx)
    val cls = classOf[VariantExplode].getName
    val code = code"""
      ${childCode.code}
      scala.collection.Seq<InternalRow> ${ev.value} = $cls.variantExplode(
          ${childCode.value}, ${childCode.isNull});
    """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def elementSchema: StructType = {
    new StructType()
      .add("pos", IntegerType, nullable = false)
      .add("key", StringType, nullable = true)
      .add("value", VariantType, nullable = false)
  }
}

trait VariantExplodeGeneratorBuilderBase extends GeneratorBuilder {
  override def functionSignature: Option[FunctionSignature] =
    Some(FunctionSignature(Seq(InputParameter("input"))))
  override def buildGenerator(funcName: String, expressions: Seq[Expression]): Generator = {
    assert(expressions.size == 1)
    VariantExplode(expressions(0))
  }
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - It separates a variant object/array into multiple rows containing its fields/elements. Its result schema is `struct<pos int, key string, value variant>`. `pos` is the position of the field/element in its parent object/array, and `value` is the field/element value. `key` is the field name when exploding a variant object, or is NULL when exploding a variant array. It ignores any input that is not a variant array/object, including SQL NULL, variant null, and any other variant values.",
  examples = """
    Examples:
      > SELECT * from _FUNC_(parse_json('["hello", "world"]'));
       0	NULL	"hello"
       1	NULL	"world"
      > SELECT * from _FUNC_(input => parse_json('{"a": true, "b": 3.14}'));
       0	a	true
       1	b	3.14
  """,
  since = "4.0.0",
  group = "variant_funcs")
// scalastyle:on line.size.limit line.contains.tab
object VariantExplodeGeneratorBuilder extends VariantExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = false
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = "_FUNC_(expr) - It separates a variant object/array into multiple rows containing its fields/elements. Its result schema is `struct<pos int, key string, value variant>`. `pos` is the position of the field/element in its parent object/array, and `value` is the field/element value. `key` is the field name when exploding a variant object, or is NULL when exploding a variant array. It ignores any input that is not a variant array/object, including SQL NULL, variant null, and any other variant values.",
  examples = """
    Examples:
      > SELECT * from _FUNC_(parse_json('["hello", "world"]'));
       0	NULL	"hello"
       1	NULL	"world"
      > SELECT * from _FUNC_(input => parse_json('{"a": true, "b": 3.14}'));
       0	a	true
       1	b	3.14
  """,
  since = "4.0.0",
  group = "variant_funcs")
// scalastyle:on line.size.limit line.contains.tab
object VariantExplodeOuterGeneratorBuilder extends VariantExplodeGeneratorBuilderBase {
  override def isOuter: Boolean = true
}

object VariantExplode {
  /**
   * The actual implementation of the `VariantExplode` expression. We check `isNull` separately
   * rather than `input == null` because the documentation of `ExprCode` says that the value is not
   * valid if `isNull` is set to `true`.
   */
  def variantExplode(input: VariantVal, isNull: Boolean): scala.collection.Seq[InternalRow] = {
    if (isNull) {
      return Nil
    }
    val v = new Variant(input.getValue, input.getMetadata)
    v.getType match {
      case Type.OBJECT =>
        val size = v.objectSize()
        val result = new Array[InternalRow](size)
        for (i <- 0 until size) {
          val field = v.getFieldAtIndex(i)
          result(i) = InternalRow(i, UTF8String.fromString(field.key),
            new VariantVal(field.value.getValue, field.value.getMetadata))
        }
        result
      case Type.ARRAY =>
        val size = v.arraySize()
        val result = new Array[InternalRow](size)
        for (i <- 0 until size) {
          val elem = v.getElementAtIndex(i)
          result(i) = InternalRow(i, null, new VariantVal(elem.getValue, elem.getMetadata))
        }
        result
      case _ => Nil
    }
  }
}

@ExpressionDescription(
  usage = "_FUNC_(v) - Returns schema in the SQL format of a variant.",
  examples = """
    Examples:
      > SELECT _FUNC_(parse_json('null'));
       VOID
      > SELECT _FUNC_(parse_json('[{"b":true,"a":0}]'));
       ARRAY<OBJECT<a: BIGINT, b: BOOLEAN>>
  """,
  since = "4.0.0",
  group = "variant_funcs"
)
case class SchemaOfVariant(child: Expression)
  extends UnaryExpression
    with RuntimeReplaceable
    with DefaultStringProducingExpression
    with ExpectsInputTypes {
  override lazy val replacement: Expression = StaticInvoke(
    SchemaOfVariant.getClass,
    dataType,
    "schemaOfVariant",
    Seq(child),
    inputTypes,
    returnNullable = false)

  override def inputTypes: Seq[AbstractDataType] = Seq(VariantType)

  override def prettyName: String = "schema_of_variant"

  override protected def withNewChildInternal(newChild: Expression): SchemaOfVariant =
    copy(child = newChild)
}

object SchemaOfVariant {
  /** The actual implementation of the `SchemaOfVariant` expression. */
  def schemaOfVariant(input: VariantVal): UTF8String = {
    val v = new Variant(input.getValue, input.getMetadata)
    UTF8String.fromString(printSchema(schemaOf(v)))
  }

  /**
   * Similar to `dataType.sql`. The only difference is that `StructType` is shown as
   * `OBJECT<...>` rather than `STRUCT<...>`.
   * SchemaOfVariant expressions use the Struct DataType to denote the Object type in the variant
   * spec. However, the Object type is not equivalent to the struct type as an Object represents an
   * unordered bag of key-value pairs while the Struct type is ordered.
   */
  def printSchema(dataType: DataType): String = dataType match {
    case StructType(fields) =>
      def printField(f: StructField): String =
        s"${QuotingUtils.quoteIfNeeded(f.name)}: ${printSchema(f.dataType)}"

      s"OBJECT<${fields.map(printField).mkString(", ")}>"
    case ArrayType(elementType, _) => s"ARRAY<${printSchema(elementType)}>"
    case _ => dataType.sql
  }

  // Dummy class to use for UUID, which doesn't currently exist in Spark.
  private class UuidType extends DataType {
    override def defaultSize: Int = 16
    override def typeName: String = "uuid"
    private[spark] override def asNullable: UuidType = this
  }
  private case object UuidType extends UuidType

  /**
   * Return the schema of a variant. Struct fields are guaranteed to be sorted alphabetically.
   */
  def schemaOf(v: Variant): DataType = v.getType match {
    case Type.OBJECT =>
      val size = v.objectSize()
      val fields = new Array[StructField](size)
      for (i <- 0 until size) {
        val field = v.getFieldAtIndex(i)
        fields(i) = StructField(field.key, schemaOf(field.value))
      }
      // According to the variant spec, object fields must be sorted alphabetically. So we don't
      // have to sort, but just need to validate they are sorted.
      for (i <- 1 until size) {
        if (fields(i - 1).name >= fields(i).name) {
          throw new SparkRuntimeException("MALFORMED_VARIANT", Map.empty)
        }
      }
      StructType(fields)
    case Type.ARRAY =>
      var elementType: DataType = NullType
      for (i <- 0 until v.arraySize()) {
        elementType = mergeSchema(elementType, schemaOf(v.getElementAtIndex(i)))
      }
      ArrayType(elementType)
    case Type.NULL => NullType
    case Type.BOOLEAN => BooleanType
    case Type.LONG => LongType
    case Type.STRING => StringType
    case Type.DOUBLE => DoubleType
    case Type.DECIMAL =>
      val d = Decimal(v.getDecimal)
      DecimalType(d.precision, d.scale)
    case Type.DATE => DateType
    case Type.TIMESTAMP => TimestampType
    case Type.TIMESTAMP_NTZ => TimestampNTZType
    case Type.FLOAT => FloatType
    case Type.BINARY => BinaryType
    case Type.UUID => UuidType
  }

  /**
   * Returns the tightest common type for two given data types. Input struct fields are assumed to
   * be sorted alphabetically.
   */
  def mergeSchema(t1: DataType, t2: DataType): DataType =
    JsonInferSchema.compatibleType(t1, t2, VariantType)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(v) - Returns the merged schema in the SQL format of a variant column.",
  examples = """
    Examples:
      > SELECT _FUNC_(parse_json(j)) FROM VALUES ('1'), ('2'), ('3') AS tab(j);
       BIGINT
      > SELECT _FUNC_(parse_json(j)) FROM VALUES ('{"a": 1}'), ('{"b": true}'), ('{"c": 1.23}') AS tab(j);
       OBJECT<a: BIGINT, b: BOOLEAN, c: DECIMAL(3,2)>
  """,
  since = "4.0.0",
  group = "variant_funcs")
// scalastyle:on line.size.limit
case class SchemaOfVariantAgg(
    child: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[DataType]
    with ExpectsInputTypes
    with QueryErrorsBase
    with DefaultStringProducingExpression
    with UnaryLike[Expression] {
  def this(child: Expression) = this(child, 0, 0)

  override def inputTypes: Seq[AbstractDataType] = Seq(VariantType)

  override def nullable: Boolean = false

  override def createAggregationBuffer(): DataType = NullType

  override def update(buffer: DataType, input: InternalRow): DataType = {
    val inputVariant = child.eval(input).asInstanceOf[VariantVal]
    if (inputVariant != null) {
      val v = new Variant(inputVariant.getValue, inputVariant.getMetadata)
      SchemaOfVariant.mergeSchema(buffer, SchemaOfVariant.schemaOf(v))
    } else {
      buffer
    }
  }

  override def merge(buffer: DataType, input: DataType): DataType =
    SchemaOfVariant.mergeSchema(buffer, input)

  override def eval(buffer: DataType): Any =
    UTF8String.fromString(SchemaOfVariant.printSchema(buffer))

  override def serialize(buffer: DataType): Array[Byte] = buffer.json.getBytes("UTF-8")

  override def deserialize(storageFormat: Array[Byte]): DataType =
    DataType.fromJson(new String(storageFormat, "UTF-8"))

  override def prettyName: String = "schema_of_variant_agg"

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

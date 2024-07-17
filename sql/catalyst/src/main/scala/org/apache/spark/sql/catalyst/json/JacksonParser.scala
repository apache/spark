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

package org.apache.spark.sql.catalyst.json

import java.io.{ByteArrayOutputStream, CharConversionException}
import java.nio.charset.MalformedInputException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.fasterxml.jackson.core._
import org.apache.hadoop.fs.PositionedReadable

import org.apache.spark.SparkUpgradeException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, StructFilters}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.errors.{ExecutionErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String, VariantVal}
import org.apache.spark.util.Utils

/**
 * Constructs a parser for a given schema that translates a json string to an [[InternalRow]].
 */
class JacksonParser(
    schema: DataType,
    val options: JSONOptions,
    allowArrayAsStructs: Boolean,
    filters: Seq[Filter] = Seq.empty) extends Logging {

  import JacksonUtils._
  import com.fasterxml.jackson.core.JsonToken._

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `InternalRow`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter = makeRootConverter(schema)

  private val factory = options.buildJsonFactory()

  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormatInRead,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
  private lazy val timestampNTZFormatter = TimestampFormatter(
    options.timestampNTZFormatInRead,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true,
    forTimestampNTZ = true)
  private lazy val dateFormatter = DateFormatter(
    options.dateFormatInRead,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  // Flags to signal if we need to fall back to the backward compatible behavior of parsing
  // dates and timestamps.
  // For more information, see comments for "enableDateTimeParsingFallback" option in JSONOptions.
  private val enableParsingFallbackForTimestampType =
    options.enableDateTimeParsingFallback
      .orElse(SQLConf.get.jsonEnableDateTimeParsingFallback)
      .getOrElse {
        SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
          options.timestampFormatInRead.isEmpty
      }
  private val enableParsingFallbackForDateType =
    options.enableDateTimeParsingFallback
      .orElse(SQLConf.get.jsonEnableDateTimeParsingFallback)
      .getOrElse {
        SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
          options.dateFormatInRead.isEmpty
      }

  private val enablePartialResults = SQLConf.get.jsonEnablePartialResults

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema. This is a wrapper for the method
   * `makeConverter()` to handle a row wrapped with an array.
   */
  private def makeRootConverter(dt: DataType): JsonParser => Iterable[InternalRow] = {
    dt match {
      case _: StructType if options.singleVariantColumn.isDefined => (parser: JsonParser) => {
        Some(InternalRow(parseVariant(parser)))
      }
      case st: StructType => makeStructRootConverter(st)
      case mt: MapType => makeMapRootConverter(mt)
      case at: ArrayType => makeArrayRootConverter(at)
    }
  }

  protected final def parseVariant(parser: JsonParser): VariantVal = {
    // Skips `FIELD_NAME` at the beginning. This check is adapted from `parseJsonToken`, but we
    // cannot directly use the function here because it also handles the `VALUE_NULL` token and
    // returns null (representing a SQL NULL). Instead, we want to return a variant null.
    if (parser.getCurrentToken == FIELD_NAME) {
      parser.nextToken()
    }
    try {
      val v = VariantBuilder.parseJson(parser)
      new VariantVal(v.getValue, v.getMetadata)
    } catch {
      case _: VariantSizeLimitException =>
        throw QueryExecutionErrors.variantSizeLimitError(VariantUtil.SIZE_LIMIT, "JacksonParser")
    }
  }

  private def makeStructRootConverter(st: StructType): JsonParser => Iterable[InternalRow] = {
    val elementConverter = makeConverter(st)
    val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
    val jsonFilters = if (SQLConf.get.jsonFilterPushDown) {
      new JsonFilters(filters, st)
    } else {
      new NoopFilters
    }
    (parser: JsonParser) => parseJsonToken[Iterable[InternalRow]](parser, st) {
      case START_OBJECT => convertObject(parser, st, fieldConverters, jsonFilters, isRoot = true)
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        //
        // For example, we support, the JSON data as below:
        //
        // [{"a":"str_a_1"}]
        // [{"a":"str_a_2"}, {"b":"str_b_3"}]
        //
        // resulting in:
        //
        // List([str_a_1,null])
        // List([str_a_2,null], [null,str_b_3])
        //
      case START_ARRAY if allowArrayAsStructs =>
        val array = convertArray(parser, elementConverter, isRoot = true, arrayAsStructs = true)
        // Here, as we support reading top level JSON arrays and take every element
        // in such an array as a row, this case is possible.
        if (array.numElements() == 0) {
          Array.empty[InternalRow]
        } else {
          array.toArray[InternalRow](schema)
        }
      case START_ARRAY =>
        throw JsonArraysAsStructsException()
    }
  }

  private def makeMapRootConverter(mt: MapType): JsonParser => Iterable[InternalRow] = {
    val fieldConverter = makeConverter(mt.valueType)
    (parser: JsonParser) => parseJsonToken[Iterable[InternalRow]](parser, mt) {
      case START_OBJECT => Some(InternalRow(convertMap(parser, fieldConverter)))
    }
  }

  private def makeArrayRootConverter(at: ArrayType): JsonParser => Iterable[InternalRow] = {
    val elemConverter = makeConverter(at.elementType)
    (parser: JsonParser) => parseJsonToken[Iterable[InternalRow]](parser, at) {
      case START_ARRAY => Some(InternalRow(convertArray(parser, elemConverter)))
      case START_OBJECT if at.elementType.isInstanceOf[StructType] =>
        // This handles the case when an input JSON object is a structure but
        // the specified schema is an array of structures. In that case, the input JSON is
        // considered as an array of only one element of struct type.
        // This behavior was introduced by changes for SPARK-19595.
        //
        // For example, if the specified schema is ArrayType(new StructType().add("i", IntegerType))
        // and JSON input as below:
        //
        // [{"i": 1}, {"i": 2}]
        // [{"i": 3}]
        // {"i": 4}
        //
        // The last row is considered as an array with one element, and result of conversion:
        //
        // Seq(Row(1), Row(2))
        // Seq(Row(3))
        // Seq(Row(4))
        //
        val st = at.elementType.asInstanceOf[StructType]
        val fieldConverters = st.map(_.dataType).map(makeConverter).toArray

        val res = try {
          convertObject(parser, st, fieldConverters)
        } catch {
          case err: PartialResultException =>
            throw PartialArrayDataResultException(
              new GenericArrayData(Seq(err.partialResult)),
              err.cause
            )
        }

        Some(InternalRow(new GenericArrayData(res.toArray)))
    }
  }

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser`
   * to a value according to a desired schema.
   */
  def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Boolean](parser, dataType) {
        case VALUE_TRUE => true
        case VALUE_FALSE => false
      }

    case ByteType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Byte](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getByteValue
      }

    case ShortType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Short](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getShortValue
      }

    case IntegerType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getIntValue
      }

    case LongType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_NUMBER_INT => parser.getLongValue
      }

    case FloatType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Float](parser, dataType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getFloatValue

        case VALUE_STRING if parser.getTextLength >= 1 =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" if options.allowNonNumericNumbers =>
              Float.NaN
            case "+INF" | "+Infinity" | "Infinity" if options.allowNonNumericNumbers =>
              Float.PositiveInfinity
            case "-INF" | "-Infinity" if options.allowNonNumericNumbers =>
              Float.NegativeInfinity
            case _ => throw StringAsDataTypeException(parser.currentName, parser.getText,
              FloatType)
          }
      }

    case DoubleType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Double](parser, dataType) {
        case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
          parser.getDoubleValue

        case VALUE_STRING if parser.getTextLength >= 1 =>
          // Special case handling for NaN and Infinity.
          parser.getText match {
            case "NaN" if options.allowNonNumericNumbers =>
              Double.NaN
            case "+INF" | "+Infinity" | "Infinity" if options.allowNonNumericNumbers =>
              Double.PositiveInfinity
            case "-INF" | "-Infinity" if options.allowNonNumericNumbers =>
              Double.NegativeInfinity
            case _ => throw StringAsDataTypeException(parser.currentName, parser.getText,
              DoubleType)
          }
      }

    case _: StringType => (parser: JsonParser) => {
      // This must be enabled if we will retrieve the bytes directly from the raw content:
      val includeSourceInLocation = JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION
      val originalMask = if (includeSourceInLocation.enabledIn(parser.getFeatureMask)) {
        1
      } else {
        0
      }
      parser.overrideStdFeatures(includeSourceInLocation.getMask, includeSourceInLocation.getMask)
      val result = parseJsonToken[UTF8String](parser, dataType) {
        case VALUE_STRING =>
          UTF8String.fromString(parser.getText)

        case other =>
          // Note that it always tries to convert the data as string without the case of failure.
          val startLocation = parser.currentTokenLocation()
          def skipAhead(): Unit = {
            other match {
              case START_OBJECT =>
                parser.skipChildren()
              case START_ARRAY =>
                parser.skipChildren()
              case _ =>
              // Do nothing in this case; we've already read the token
            }
          }

          // PositionedReadable
          startLocation.contentReference().getRawContent match {
            case byteArray: Array[Byte] if exactStringParsing =>
              skipAhead()
              val endLocation = parser.currentLocation.getByteOffset

              UTF8String.fromBytes(
                byteArray,
                startLocation.getByteOffset.toInt,
                endLocation.toInt - (startLocation.getByteOffset.toInt))
            case positionedReadable: PositionedReadable if exactStringParsing =>
              skipAhead()
              val endLocation = parser.currentLocation.getByteOffset

              val size = endLocation.toInt - (startLocation.getByteOffset.toInt)
              val buffer = new Array[Byte](size)
              positionedReadable.read(startLocation.getByteOffset, buffer, 0, size)
              UTF8String.fromBytes(buffer, 0, size)
            case _ =>
              val writer = new ByteArrayOutputStream()
              Utils.tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
                generator => generator.copyCurrentStructure(parser)
              }
              UTF8String.fromBytes(writer.toByteArray)
          }
        }
      // Reset back to the original configuration:
      parser.overrideStdFeatures(includeSourceInLocation.getMask, originalMask)
      result
    }

    case TimestampType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_STRING if parser.getTextLength >= 1 =>
          try {
            timestampFormatter.parse(parser.getText)
          } catch {
            case NonFatal(e) =>
              // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
              // compatibility if enabled.
              if (!enableParsingFallbackForTimestampType) {
                throw e
              }
              val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(parser.getText))
              DateTimeUtils.stringToTimestamp(str, options.zoneId).getOrElse(throw e)
          }

        case VALUE_NUMBER_INT =>
          parser.getLongValue * 1000000L
      }

    case TimestampNTZType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_STRING if parser.getTextLength >= 1 =>
          timestampNTZFormatter.parseWithoutTimeZone(parser.getText, false)
      }

    case DateType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Integer](parser, dataType) {
        case VALUE_STRING if parser.getTextLength >= 1 =>
          try {
            dateFormatter.parse(parser.getText)
          } catch {
            case NonFatal(e) =>
              // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
              // compatibility if enabled.
              if (!enableParsingFallbackForDateType) {
                throw e
              }
              val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(parser.getText))
              DateTimeUtils.stringToDate(str).getOrElse {
                // In Spark 1.5.0, we store the data as number of days since epoch in string.
                // So, we just convert it to Int.
                try {
                  RebaseDateTime.rebaseJulianToGregorianDays(parser.getText.toInt)
                } catch {
                  case _: NumberFormatException => throw e
                }
              }.asInstanceOf[Integer]
          }
      }

    case BinaryType =>
      (parser: JsonParser) => parseJsonToken[Array[Byte]](parser, dataType) {
        case VALUE_STRING => parser.getBinaryValue
      }

    case dt: DecimalType =>
      (parser: JsonParser) => parseJsonToken[Decimal](parser, dataType) {
        case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
          Decimal(parser.getDecimalValue, dt.precision, dt.scale)
        case VALUE_STRING if parser.getTextLength >= 1 =>
          val bigDecimal = decimalParser(parser.getText)
          Decimal(bigDecimal, dt.precision, dt.scale)
      }

    case CalendarIntervalType => (parser: JsonParser) =>
      parseJsonToken[CalendarInterval](parser, dataType) {
        case VALUE_STRING =>
          IntervalUtils.safeStringToInterval(UTF8String.fromString(parser.getText))
      }

    case ym: YearMonthIntervalType => (parser: JsonParser) =>
      parseJsonToken[Integer](parser, dataType) {
        case VALUE_STRING =>
          val expr = Cast(Literal(parser.getText), ym)
          Integer.valueOf(expr.eval(EmptyRow).asInstanceOf[Int])
      }

    case dt: DayTimeIntervalType => (parser: JsonParser) =>
      parseJsonToken[java.lang.Long](parser, dataType) {
        case VALUE_STRING =>
          val expr = Cast(Literal(parser.getText), dt)
          java.lang.Long.valueOf(expr.eval(EmptyRow).asInstanceOf[Long])
      }

    case st: StructType =>
      val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
      (parser: JsonParser) => parseJsonToken[InternalRow](parser, dataType) {
        case START_OBJECT => convertObject(parser, st, fieldConverters).get
      }

    case at: ArrayType =>
      val elementConverter = makeConverter(at.elementType)
      (parser: JsonParser) => parseJsonToken[ArrayData](parser, dataType) {
        case START_ARRAY => convertArray(parser, elementConverter)
      }

    case mt: MapType =>
      val valueConverter = makeConverter(mt.valueType)
      (parser: JsonParser) => parseJsonToken[MapData](parser, dataType) {
        case START_OBJECT => convertMap(parser, valueConverter)
      }

    case udt: UserDefinedType[_] =>
      makeConverter(udt.sqlType)

    case _: NullType =>
      (parser: JsonParser) => parseJsonToken[java.lang.Long](parser, dataType) {
        case _ => null
      }

    case _: VariantType => parseVariant

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw ExecutionErrors.unsupportedDataTypeError(dataType)
  }

  /**
   * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying
   * to parse the JSON token using given function `f`. If the `f` failed to parse and convert the
   * token, call `failedConversion` to handle the token.
   */
  @scala.annotation.tailrec
  private def parseJsonToken[R >: Null](
      parser: JsonParser,
      dataType: DataType)(f: PartialFunction[JsonToken, R]): R = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        // There are useless FIELD_NAMEs between START_OBJECT and END_OBJECT tokens
        parser.nextToken()
        parseJsonToken[R](parser, dataType)(f)

      case null | VALUE_NULL => null

      case other => f.applyOrElse(other, failedConversion(parser, dataType))
    }
  }

  private val allowEmptyString = SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_EMPTY_STRING_IN_JSON)

  private val exactStringParsing = SQLConf.get.getConf(SQLConf.JSON_EXACT_STRING_PARSING)

  /**
   * This function throws an exception for failed conversion. For empty string on data types
   * except for string and binary types, this also throws an exception.
   */
  private def failedConversion[R >: Null](
      parser: JsonParser,
      dataType: DataType): PartialFunction[JsonToken, R] = {

    // SPARK-25040: Disallows empty strings for data types except for string and binary types.
    // But treats empty strings as null for certain types if the legacy config is enabled.
    case VALUE_STRING if parser.getTextLength < 1 && allowEmptyString =>
      dataType match {
        case FloatType | DoubleType | TimestampType | DateType =>
          throw EmptyJsonFieldValueException(dataType)
        case _ => null
      }

    case VALUE_STRING if parser.getTextLength < 1 =>
      throw EmptyJsonFieldValueException(dataType)

    case token =>
      // We cannot parse this token based on the given data type. So, we throw a
      // RuntimeException and this exception will be caught by `parse` method.
      throw CannotParseJSONFieldException(parser.currentName, parser.getText, token, dataType)
  }

  private val useUnsafeRow = SQLConf.get.getConf(SQLConf.JSON_USE_UNSAFE_ROW)
  private val cachedUnsafeProjection = mutable.HashMap.empty[StructType, UnsafeProjection]

  protected final def convertRow(row: InternalRow, schema: StructType): InternalRow = {
    if (useUnsafeRow) {
      val p = cachedUnsafeProjection.getOrElseUpdate(schema, UnsafeProjection.create(schema))
      // The copy is necessary: each time `UnsafeProjection` produces a row, it updates an
      // internal `UnsafeRow` object and returns the reference. We need to avoid overwriting
      // previously returned results.
      p(row).copy()
    } else {
      row
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: JsonParser,
      schema: StructType,
      fieldConverters: Array[ValueConverter],
      structFilters: StructFilters = new NoopFilters(),
      isRoot: Boolean = false): Option[InternalRow] = {
    val row = new GenericInternalRow(schema.length)
    var badRecordException: Option[Throwable] = None
    var skipRow = false

    structFilters.reset()
    lazy val bitmask = ResolveDefaultColumns.existenceDefaultsBitmask(schema)
    resetExistenceDefaultsBitmask(schema, bitmask)
    while (!skipRow && nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.currentName) match {
        case Some(index) =>
          try {
            row.update(index, fieldConverters(index).apply(parser))
            skipRow = structFilters.skipRow(row, index)
            bitmask(index) = false
          } catch {
            case e: SparkUpgradeException => throw e
            case err: PartialValueException if enablePartialResults =>
              badRecordException = badRecordException.orElse(Some(err.cause))
              row.update(index, err.partialResult)
              skipRow = structFilters.skipRow(row, index)
              bitmask(index) = false
            case NonFatal(e) if isRoot || enablePartialResults =>
              badRecordException = badRecordException.orElse(Some(e))
              parser.skipChildren()
          }
        case None =>
          parser.skipChildren()
      }
    }
    if (skipRow) {
      None
    } else if (badRecordException.isEmpty) {
      applyExistenceDefaultValuesToRow(schema, row, bitmask)
      Some(convertRow(row, schema))
    } else {
      throw PartialResultException(row, badRecordException.get)
    }
  }

  /**
   * Parse an object as a Map, preserving all fields.
   */
  private def convertMap(
      parser: JsonParser,
      fieldConverter: ValueConverter): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    var badRecordException: Option[Throwable] = None

    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.currentName)
      try {
        values += fieldConverter.apply(parser)
      } catch {
        case err: PartialValueException if enablePartialResults =>
          badRecordException = badRecordException.orElse(Some(err.cause))
          values += err.partialResult
        case NonFatal(e) if enablePartialResults =>
          badRecordException = badRecordException.orElse(Some(e))
          parser.skipChildren()
      }
    }

    // The JSON map will never have null or duplicated map keys, it's safe to create a
    // ArrayBasedMapData directly here.
    val mapData = ArrayBasedMapData(keys.toArray, values.toArray)

    if (badRecordException.isEmpty) {
      mapData
    } else {
      throw PartialMapDataResultException(mapData, badRecordException.get)
    }
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(
      parser: JsonParser,
      fieldConverter: ValueConverter,
      isRoot: Boolean = false,
      arrayAsStructs: Boolean = false): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    var badRecordException: Option[Throwable] = None

    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      try {
        val v = fieldConverter.apply(parser)
        if (isRoot && v == null) throw QueryExecutionErrors.rootConverterReturnNullError()
        values += v
      } catch {
        case err: PartialValueException if enablePartialResults =>
          badRecordException = badRecordException.orElse(Some(err.cause))
          values += err.partialResult
      }
    }

    val arrayData = new GenericArrayData(values.toArray)

    if (badRecordException.isEmpty) {
      arrayData
    } else if (arrayAsStructs) {
      throw PartialResultArrayException(arrayData.toArray[InternalRow](schema),
        badRecordException.get)
    } else {
      throw PartialArrayDataResultException(arrayData, badRecordException.get)
    }
  }

  /**
   * Converts the non-stacktrace exceptions to user-friendly QueryExecutionErrors.
   */
  private def convertCauseForPartialResult(err: Throwable): Throwable = err match {
    case CannotParseJSONFieldException(fieldName, fieldValue, jsonType, dataType) =>
      QueryExecutionErrors.cannotParseJSONFieldError(fieldName, fieldValue, jsonType, dataType)
    case EmptyJsonFieldValueException(dataType) =>
      QueryExecutionErrors.emptyJsonFieldValueError(dataType)
    case _ => err
  }

  /**
   * Parse the JSON input to the set of [[InternalRow]]s.
   *
   * @param recordLiteral an optional function that will be used to generate
   *   the corrupt record text instead of record.toString
   */
  def parse[T](
      record: T,
      createParser: (JsonFactory, T) => JsonParser,
      recordLiteral: T => UTF8String): Iterable[InternalRow] = {
    try {
      Utils.tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => None
          case _ => rootConverter.apply(parser) match {
            case null => throw QueryExecutionErrors.rootConverterReturnNullError()
            case rows => rows.toSeq
          }
        }
      }
    } catch {
      case e: SparkUpgradeException => throw e
      case e @ (_: RuntimeException | _: JsonProcessingException | _: MalformedInputException) =>
        // JSON parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the field configured by
        // `columnNameOfCorruptRecord` are set to `null`.
        throw BadRecordException(() => recordLiteral(record), () => Array.empty, e)
      case e: CharConversionException if options.encoding.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => recordLiteral(record), () => Array.empty,
          wrappedCharException)
      case PartialResultException(row, cause) =>
        throw BadRecordException(
          record = () => recordLiteral(record),
          partialResults = () => Array(row),
          convertCauseForPartialResult(cause))
      case PartialResultArrayException(rows, cause) =>
        throw BadRecordException(
          record = () => recordLiteral(record),
          partialResults = () => rows,
          cause)
      // These exceptions should never be thrown outside of JacksonParser.
      // They are used for the control flow in the parser. We add them here for completeness
      // since they also indicate a bad record.
      case PartialArrayDataResultException(arrayData, cause) =>
        throw BadRecordException(
          record = () => recordLiteral(record),
          partialResults = () => Array(InternalRow(arrayData)),
          convertCauseForPartialResult(cause))
      case PartialMapDataResultException(mapData, cause) =>
        throw BadRecordException(
          record = () => recordLiteral(record),
          partialResults = () => Array(InternalRow(mapData)),
          convertCauseForPartialResult(cause))
    }
  }
}

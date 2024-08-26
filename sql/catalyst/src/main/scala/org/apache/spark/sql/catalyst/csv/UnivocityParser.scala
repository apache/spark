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

package org.apache.spark.sql.catalyst.csv

import java.io.InputStream

import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.SparkUpgradeException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.{ExecutionErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Constructs a parser for a given schema that translates CSV data to an [[InternalRow]].
 *
 * @param dataSchema The CSV data schema that is specified by the user, or inferred from underlying
 *                   data files.
 * @param requiredSchema The schema of the data that should be output for each row. This should be a
 *                       subset of the columns in dataSchema.
 * @param options Configuration options for a CSV parser.
 * @param filters The pushdown filters that should be applied to converted values.
 */
class UnivocityParser(
    dataSchema: StructType,
    requiredSchema: StructType,
    val options: CSVOptions,
    filters: Seq[Filter]) extends Logging {
  require(requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString}).")

  def this(dataSchema: StructType, requiredSchema: StructType, options: CSVOptions) = {
    this(dataSchema, requiredSchema, options, Seq.empty)
  }
  def this(schema: StructType, options: CSVOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = String => Any

  // This index is used to reorder parsed tokens
  private val tokenIndexArr = requiredSchema.map(f => dataSchema.indexOf(f)).toArray

  // True if we should inform the Univocity CSV parser to select which fields to read by their
  // positions. Generally assigned by input configuration options, except when input column(s) have
  // default values, in which case we omit the explicit indexes in order to know how many tokens
  // were present in each line instead.
  private def columnPruning: Boolean = options.isColumnPruningEnabled(requiredSchema)

  // When column pruning is enabled, the parser only parses the required columns based on
  // their positions in the data schema.
  private val parsedSchema = if (columnPruning) requiredSchema else dataSchema

  val tokenizer: CsvParser = {
    val parserSetting = options.asParserSettings
    // When to-be-parsed schema is shorter than the to-be-read data schema, we let Univocity CSV
    // parser select a sequence of fields for reading by their positions.
    if (parsedSchema.length < dataSchema.length) {
      // Box into Integer here to avoid unboxing where `tokenIndexArr` is used during parsing
      parserSetting.selectIndexes(tokenIndexArr.map(java.lang.Integer.valueOf(_)): _*)
    }
    new CsvParser(parserSetting)
  }

  // Pre-allocated Some to avoid the overhead of building Some per each-row.
  private val requiredRow = Some(new GenericInternalRow(requiredSchema.length))
  // Pre-allocated empty sequence returned when the parsed row cannot pass filters.
  // We preallocate it avoid unnecessary allocations.
  private val noRows = None

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

  private val csvFilters = if (SQLConf.get.csvFilterPushDown) {
    new OrderedFilters(filters, requiredSchema)
  } else {
    new NoopFilters
  }

  // Flags to signal if we need to fall back to the backward compatible behavior of parsing
  // dates and timestamps.
  // For more information, see comments for "enableDateTimeParsingFallback" option in CSVOptions.
  private val enableParsingFallbackForTimestampType =
    options.enableDateTimeParsingFallback
      .orElse(SQLConf.get.csvEnableDateTimeParsingFallback)
      .getOrElse {
        SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
          options.timestampFormatInRead.isEmpty
      }
  private val enableParsingFallbackForDateType =
    options.enableDateTimeParsingFallback
      .orElse(SQLConf.get.csvEnableDateTimeParsingFallback)
      .getOrElse {
        SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY ||
          options.dateFormatOption.isEmpty
      }

  // Retrieve the raw record string.
  private def getCurrentInput: UTF8String = {
    if (tokenizer.getContext == null) return null
    val currentContent = tokenizer.getContext.currentParsedContent()
    if (currentContent == null) null else UTF8String.fromString(currentContent.stripLineEnd)
  }

  // This parser first picks some tokens from the input tokens, according to the required schema,
  // then parse these tokens and put the values in a row, with the order specified by the required
  // schema.
  //
  // For example, let's say there is CSV data as below:
  //
  //   a,b,c
  //   1,2,A
  //
  // So the CSV data schema is: ["a", "b", "c"]
  // And let's say the required schema is: ["c", "b"]
  //
  // with the input tokens,
  //
  //   input tokens - [1, 2, "A"]
  //
  // Each input token is placed in each output row's position by mapping these. In this case,
  //
  //   output row - ["A", 2]
  private val valueConverters: Array[ValueConverter] = {
    requiredSchema.map(f => makeConverter(f.name, f.dataType, f.nullable)).toArray
  }

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  /**
   * Create a converter which converts the string value to a value according to a desired type.
   * Currently, we do not support complex types (`ArrayType`, `MapType`, `StructType`).
   *
   * For other nullable types, returns null if it is null or equals to the value specified
   * in `nullValue` option.
   */
  def makeConverter(
      name: String,
      dataType: DataType,
      nullable: Boolean = true): ValueConverter = dataType match {
    case _: ByteType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toByte)

    case _: ShortType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toShort)

    case _: IntegerType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toInt)

    case _: LongType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toLong)

    case _: FloatType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Float.NaN
        case options.negativeInf => Float.NegativeInfinity
        case options.positiveInf => Float.PositiveInfinity
        case datum => datum.toFloat
      }

    case _: DoubleType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Double.NaN
        case options.negativeInf => Double.NegativeInfinity
        case options.positiveInf => Double.PositiveInfinity
        case datum => datum.toDouble
      }

    case _: BooleanType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toBoolean)

    case dt: DecimalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        Decimal(decimalParser(datum), dt.precision, dt.scale)
      }

    case _: DateType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        try {
          dateFormatter.parse(datum)
        } catch {
          case NonFatal(e) =>
            // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
            // compatibility if enabled.
            if (!enableParsingFallbackForDateType) {
              throw e
            }
            val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum))
            DateTimeUtils.stringToDate(str).getOrElse(throw e)
        }
      }

    case _: TimestampType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        try {
          timestampFormatter.parse(datum)
        } catch {
          case NonFatal(e) =>
            // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
            // compatibility if enabled.
            if (!enableParsingFallbackForTimestampType) {
              throw e
            }
            val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum))
            DateTimeUtils.stringToTimestamp(str, options.zoneId).getOrElse(throw(e))
        }
      }

    case _: TimestampNTZType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        timestampNTZFormatter.parseWithoutTimeZone(datum, false)
      }

    case _: StringType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(UTF8String.fromString)

    case _: BinaryType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.getBytes)

    case CalendarIntervalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        IntervalUtils.safeStringToInterval(UTF8String.fromString(datum))
      }

    case ym: YearMonthIntervalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        IntervalUtils.castStringToYMInterval(
          UTF8String.fromString(datum), ym.startField, ym.endField)
      }

    case dt: DayTimeIntervalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        IntervalUtils.castStringToDTInterval(
          UTF8String.fromString(datum), dt.startField, dt.endField)
      }

    case udt: UserDefinedType[_] =>
      makeConverter(name, udt.sqlType, nullable)

    case _ => throw ExecutionErrors.unsupportedDataTypeError(dataType)
  }

  private def nullSafeDatum(
       datum: String,
       name: String,
       nullable: Boolean,
       options: CSVOptions)(converter: ValueConverter): Any = {
    if (datum == options.nullValue || datum == null) {
      if (!nullable) {
        throw QueryExecutionErrors.foundNullValueForNotNullableFieldError(name)
      }
      null
    } else {
      converter.apply(datum)
    }
  }

  /**
   * Parses a single CSV string and turns it into either one resulting row or no row (if the
   * the record is malformed).
   */
  val parse: String => Option[InternalRow] = {
    // This is intentionally a val to create a function once and reuse.
    if (columnPruning && requiredSchema.isEmpty) {
      // If `columnPruning` enabled and partition attributes scanned only,
      // `schema` gets empty.
      (_: String) => Some(InternalRow.empty)
    } else {
      // parse if the columnPruning is disabled or requiredSchema is nonEmpty
      (input: String) => convert(tokenizer.parseLine(input))
    }
  }

  private val getToken = if (columnPruning) {
    (tokens: Array[String], index: Int) => tokens(index)
  } else {
    (tokens: Array[String], index: Int) => tokens(tokenIndexArr(index))
  }

  private def convert(tokens: Array[String]): Option[InternalRow] = {
    if (tokens == null) {
      throw BadRecordException(
        () => getCurrentInput,
        () => Array.empty,
        LazyBadRecordCauseWrapper(() => QueryExecutionErrors.malformedCSVRecordError("")))
    }

    val currentInput = getCurrentInput

    var badRecordException: Option[Throwable] = if (tokens.length != parsedSchema.length) {
      // If the number of tokens doesn't match the schema, we should treat it as a malformed record.
      // However, we still have chance to parse some of the tokens. It continues to parses the
      // tokens normally and sets null when `ArrayIndexOutOfBoundsException` occurs for missing
      // tokens.
      Some(LazyBadRecordCauseWrapper(
        () => QueryExecutionErrors.malformedCSVRecordError(currentInput.toString)))
    } else None
    // When the length of the returned tokens is identical to the length of the parsed schema,
    // we just need to:
    //  1. Convert the tokens that correspond to the required schema.
    //  2. Apply the pushdown filters to `requiredRow`.
    var i = 0
    val row = requiredRow.get
    var skipRow = false
    while (i < requiredSchema.length) {
      try {
        if (skipRow) {
          row.setNullAt(i)
        } else {
          row(i) = valueConverters(i).apply(getToken(tokens, i))
          if (csvFilters.skipRow(row, i)) {
            skipRow = true
          }
        }
      } catch {
        case e: SparkUpgradeException => throw e
        case NonFatal(e) =>
          badRecordException = badRecordException.orElse(Some(e))
          // Use the corresponding DEFAULT value associated with the column, if any.
          row.update(i, ResolveDefaultColumns.existenceDefaultValues(requiredSchema)(i))
      }
      i += 1
    }
    if (skipRow) {
      noRows
    } else {
      if (badRecordException.isDefined) {
        throw BadRecordException(
          () => currentInput, () => Array(requiredRow.get), badRecordException.get)
      } else {
        requiredRow
      }
    }
  }
}

private[sql] object UnivocityParser {

  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of tokens.
   */
  def tokenizeStream(
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      tokenizer: CsvParser,
      encoding: String): Iterator[Array[String]] = {
    val handleHeader: () => Unit =
      () => if (shouldDropHeader) tokenizer.parseNext

    convertStream(inputStream, tokenizer, handleHeader, encoding)(tokens => tokens)
  }

  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of rows.
   */
  def parseStream(
      inputStream: InputStream,
      parser: UnivocityParser,
      headerChecker: CSVHeaderChecker,
      schema: StructType): Iterator[InternalRow] = {
    val tokenizer = parser.tokenizer
    val safeParser = new FailureSafeParser[Array[String]](
      input => parser.convert(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    val handleHeader: () => Unit =
      () => headerChecker.checkHeaderColumnNames(tokenizer)

    convertStream(inputStream, tokenizer, handleHeader, parser.options.charset) { tokens =>
      safeParser.parse(tokens)
    }.flatten
  }

  private def convertStream[T](
      inputStream: InputStream,
      tokenizer: CsvParser,
      handleHeader: () => Unit,
      encoding: String)(
      convert: Array[String] => T) = new Iterator[T] {
    tokenizer.beginParsing(inputStream, encoding)

    // We can handle header here since here the stream is open.
    handleHeader()

    private var nextRecord = tokenizer.parseNext()

    override def hasNext: Boolean = nextRecord != null

    override def next(): T = {
      if (!hasNext) {
        throw QueryExecutionErrors.endOfStreamError()
      }
      val curRecord = convert(nextRecord)
      nextRecord = tokenizer.parseNext()
      curRecord
    }
  }

  /**
   * Parses an iterator that contains CSV strings and turns it into an iterator of rows.
   */
  def parseIterator(
      lines: Iterator[String],
      parser: UnivocityParser,
      headerChecker: CSVHeaderChecker,
      schema: StructType): Iterator[InternalRow] = {
    headerChecker.checkHeaderColumnNames(lines, parser.tokenizer)

    val options = parser.options

    val filteredLines: Iterator[String] = CSVExprUtils.filterCommentAndEmpty(lines, options)

    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    filteredLines.flatMap(safeParser.parse)
  }
}

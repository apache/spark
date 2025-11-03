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
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.univocity.parsers.common.TextParsingException
import com.univocity.parsers.csv.CsvParser

import org.apache.spark.{SparkRuntimeException, SparkUpgradeException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.{ExecutionErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.types.variant._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

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

  // When `options.needHeaderForSingleVariantColumn` is true, it will be set to the header column
  // names by `CSVDataSource.readHeaderForSingleVariantColumn`.
  var headerColumnNames: Option[Array[String]] = None
  private val singleVariantFieldConverters = new ArrayBuffer[VariantValueConverter]()

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
  private val valueConverters: Array[ValueConverter] =
    if (options.singleVariantColumn.isDefined) {
      null
    } else {
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

    case _: VariantType => new VariantValueConverter

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

  private def parseLine(line: String): Array[String] = {
    try {
      tokenizer.parseLine(line)
    }
    catch {
      case e: TextParsingException if e.getCause.isInstanceOf[ArrayIndexOutOfBoundsException] =>
        throw new SparkRuntimeException(
          errorClass = "MALFORMED_CSV_RECORD",
          messageParameters = Map("badRecord" -> line),
          cause = e
        )
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
      (input: String) => convert(parseLine(input))
    }
  }

  private val getToken = if (columnPruning) {
    (tokens: Array[String], index: Int) => tokens(index)
  } else {
    (tokens: Array[String], index: Int) => tokens(tokenIndexArr(index))
  }

  /**
   * The entire line of CSV data is collected into a single variant object. When `headerColumnNames`
   * is defined, the field names will be extracted from it. Otherwise, the field names will have a
   * a format of "_c$i" to match the position of the values in the CSV data.
   */
  protected final def convertSingleVariantRow(
      tokens: Array[String],
      currentInput: UTF8String): GenericInternalRow = {
    val row = new GenericInternalRow(1)
    try {
      val keys = headerColumnNames.orNull
      val numFields = if (keys != null) tokens.length.min(keys.length) else tokens.length
      if (singleVariantFieldConverters.length < numFields) {
        val extra = numFields - singleVariantFieldConverters.length
        singleVariantFieldConverters.appendAll(Array.fill(extra)(new VariantValueConverter))
      }
      val builder = new VariantBuilder(false)
      val start = builder.getWritePos
      val fields = new java.util.ArrayList[VariantBuilder.FieldEntry](numFields)
      for (i <- 0 until numFields) {
        val key = if (keys != null) keys(i) else "_c" + i
        val id = builder.addKey(key)
        fields.add(new VariantBuilder.FieldEntry(key, id, builder.getWritePos - start))
        singleVariantFieldConverters(i).convertInput(builder, tokens(i))
      }
      builder.finishWritingObject(start, fields)
      val v = builder.result()
      row(0) = new VariantVal(v.getValue, v.getMetadata)
      // If the header line has different number of tokens than the content line, the CSV data is
      // malformed. We may still have partially parsed data in `row`.
      if (keys != null && keys.length != tokens.length) {
        throw QueryExecutionErrors.malformedCSVRecordError(currentInput.toString)
      }
      row
    } catch {
      case NonFatal(e) =>
        throw BadRecordException(() => currentInput, () => Array(row), cause = e)
    }
  }

  private def convert(tokens: Array[String]): Option[InternalRow] = {
    if (tokens == null) {
      throw BadRecordException(
        () => getCurrentInput,
        () => Array.empty,
        LazyBadRecordCauseWrapper(() => QueryExecutionErrors.malformedCSVRecordError("")))
    }

    val currentInput = getCurrentInput

    if (options.singleVariantColumn.isDefined) {
      return Some(convertSingleVariantRow(tokens, currentInput))
    }

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

  /**
   * This class converts a comma-separated value into a variant column (when the schema contains
   * variant type) or a variant field (when in singleVariantColumn mode).
   *
   * It has a list of scalar types to try (long, decimal, date, timestamp, boolean) and maintains
   * the current content type. It tries to parse the input as the current content type. If the
   * parsing fails, it moves to the next type in the list and continues the trial. It never checks
   * the previous types that have already failed. In the end, it either successfully parses the
   * input as a specific scalar type, or fails after trying all the types and defaults to the string
   * type. The state is reset for every input file.
   *
   * Floating point types (double, float) are not considered to avoid precision loss.
   */
  private final class VariantValueConverter extends ValueConverter {
    private var currentType: DataType = LongType
    // Keep consistent with `CSVInferSchema`: only produce TimestampNTZ when the default timestamp
    // type is TimestampNTZ.
    private val isDefaultNTZ = SQLConf.get.timestampType == TimestampNTZType

    override def apply(s: String): Any = {
      val builder = new VariantBuilder(false)
      convertInput(builder, s)
      val v = builder.result()
      new VariantVal(v.getValue, v.getMetadata)
    }

    def convertInput(builder: VariantBuilder, s: String): Unit = {
      if (s == null || s == options.nullValue) {
        builder.appendNull()
        return
      }

      def parseLong(): DataType = {
        try {
          builder.appendLong(s.toLong)
          // The actual integral type doesn't matter. `appendLong` will use the smallest possible
          // integral type to store the value.
          LongType
        } catch {
          case NonFatal(_) => parseDecimal()
        }
      }

      def parseDecimal(): DataType = {
        try {
          var d = decimalParser(s)
          if (d.scale() < 0) {
            d = d.setScale(0)
          }
          if (d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION &&
            d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION) {
            builder.appendDecimal(d)
            // The actual decimal type doesn't matter. `appendDecimal` will use the smallest
            // possible decimal type to store the value.
            DecimalType.USER_DEFAULT
          } else {
            if (options.preferDate) parseDate() else parseTimestampNTZ()
          }
        } catch {
          case NonFatal(_) =>
            if (options.preferDate) parseDate() else parseTimestampNTZ()
        }
      }

      def parseDate(): DataType = {
        try {
          builder.appendDate(dateFormatter.parse(s))
          DateType
        } catch {
          case NonFatal(_) => parseTimestampNTZ()
        }
      }

      def parseTimestampNTZ(): DataType = {
        if (isDefaultNTZ) {
          try {
            builder.appendTimestampNtz(timestampNTZFormatter.parseWithoutTimeZone(s, false))
            TimestampNTZType
          } catch {
            case NonFatal(_) => parseTimestamp()
          }
        } else {
          parseTimestamp()
        }
      }

      def parseTimestamp(): DataType = {
        try {
          builder.appendTimestamp(timestampFormatter.parse(s))
          TimestampType
        } catch {
          case NonFatal(_) => parseBoolean()
        }
      }

      def parseBoolean(): DataType = {
        val lower = s.toLowerCase(Locale.ROOT)
        if (lower == "true") {
          builder.appendBoolean(true)
          BooleanType
        } else if (lower == "false") {
          builder.appendBoolean(false)
          BooleanType
        } else {
          parseString()
        }
      }

      def parseString(): DataType = {
        builder.appendString(s)
        StringType
      }

      val newType = currentType match {
        case LongType => parseLong()
        case _: DecimalType => parseDecimal()
        case DateType => parseDate()
        case TimestampNTZType => parseTimestampNTZ()
        case TimestampType => parseTimestamp()
        case BooleanType => parseBoolean()
        case StringType => parseString()
      }
      currentType = newType
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

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

package org.apache.spark.sql.execution.datasources.csv

import java.io.InputStream
import java.math.BigDecimal

import scala.util.Try
import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.FailureSafeParser
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
 */
class UnivocityParser(
    dataSchema: StructType,
    requiredSchema: StructType,
    val options: CSVOptions) extends Logging {
  require(requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString}).")

  def this(schema: StructType, options: CSVOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = String => Any

  // This index is used to reorder parsed tokens
  private val tokenIndexArr =
    requiredSchema.map(f => java.lang.Integer.valueOf(dataSchema.indexOf(f))).toArray

  // When column pruning is enabled, the parser only parses the required columns based on
  // their positions in the data schema.
  private val parsedSchema = if (options.columnPruning) requiredSchema else dataSchema

  val tokenizer = {
    val parserSetting = options.asParserSettings
    // When to-be-parsed schema is shorter than the to-be-read data schema, we let Univocity CSV
    // parser select a sequence of fields for reading by their positions.
    // if (options.columnPruning && requiredSchema.length < dataSchema.length) {
    if (parsedSchema.length < dataSchema.length) {
      parserSetting.selectIndexes(tokenIndexArr: _*)
    }
    new CsvParser(parserSetting)
  }

  private val row = new GenericInternalRow(requiredSchema.length)

  // Retrieve the raw record string.
  private def getCurrentInput: UTF8String = {
    UTF8String.fromString(tokenizer.getContext.currentParsedContent().stripLineEnd)
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
    requiredSchema.map(f => makeConverter(f.name, f.dataType, f.nullable, options)).toArray
  }

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
      nullable: Boolean = true,
      options: CSVOptions): ValueConverter = dataType match {
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
        val value = new BigDecimal(datum.replaceAll(",", ""))
        Decimal(value, dt.precision, dt.scale)
      }

    case _: TimestampType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        // This one will lose microseconds parts.
        // See https://issues.apache.org/jira/browse/SPARK-10681.
        Try(options.timestampFormat.parse(datum).getTime * 1000L)
          .getOrElse {
          // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
          // compatibility.
          DateTimeUtils.stringToTime(datum).getTime * 1000L
        }
      }

    case _: DateType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        // This one will lose microseconds parts.
        // See https://issues.apache.org/jira/browse/SPARK-10681.x
        Try(DateTimeUtils.millisToDays(options.dateFormat.parse(datum).getTime))
          .getOrElse {
          // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
          // compatibility.
          DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(datum).getTime)
        }
      }

    case _: StringType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(UTF8String.fromString)

    case udt: UserDefinedType[_] => (datum: String) =>
      makeConverter(name, udt.sqlType, nullable, options)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
  }

  private def nullSafeDatum(
       datum: String,
       name: String,
       nullable: Boolean,
       options: CSVOptions)(converter: ValueConverter): Any = {
    if (datum == options.nullValue || datum == null) {
      if (!nullable) {
        throw new RuntimeException(s"null value found but field $name is not nullable.")
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
  def parse(input: String): InternalRow = convert(tokenizer.parseLine(input))

  private val getToken = if (options.columnPruning) {
    (tokens: Array[String], index: Int) => tokens(index)
  } else {
    (tokens: Array[String], index: Int) => tokens(tokenIndexArr(index))
  }

  private def convert(tokens: Array[String]): InternalRow = {
    if (tokens == null) {
      throw BadRecordException(
        () => getCurrentInput,
        () => None,
        new RuntimeException("Malformed CSV record"))
    } else if (tokens.length != parsedSchema.length) {
      // If the number of tokens doesn't match the schema, we should treat it as a malformed record.
      // However, we still have chance to parse some of the tokens, by adding extra null tokens in
      // the tail if the number is smaller, or by dropping extra tokens if the number is larger.
      val checkedTokens = if (parsedSchema.length > tokens.length) {
        tokens ++ new Array[String](parsedSchema.length - tokens.length)
      } else {
        tokens.take(parsedSchema.length)
      }
      def getPartialResult(): Option[InternalRow] = {
        try {
          Some(convert(checkedTokens))
        } catch {
          case _: BadRecordException => None
        }
      }
      // For records with less or more tokens than the schema, tries to return partial results
      // if possible.
      throw BadRecordException(
        () => getCurrentInput,
        () => getPartialResult(),
        new RuntimeException("Malformed CSV record"))
    } else {
      try {
        // When the length of the returned tokens is identical to the length of the parsed schema,
        // we just need to convert the tokens that correspond to the required columns.
        var i = 0
        while (i < requiredSchema.length) {
          row(i) = valueConverters(i).apply(getToken(tokens, i))
          i += 1
        }
        row
      } catch {
        case NonFatal(e) =>
          // For corrupted records with the number of tokens same as the schema,
          // CSV reader doesn't support partial results. All fields other than the field
          // configured by `columnNameOfCorruptRecord` are set to `null`.
          throw BadRecordException(() => getCurrentInput, () => None, e)
      }
    }
  }
}

private[csv] object UnivocityParser {

  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of tokens.
   */
  def tokenizeStream(
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      tokenizer: CsvParser): Iterator[Array[String]] = {
    convertStream(inputStream, shouldDropHeader, tokenizer)(tokens => tokens)
  }

  /**
   * Parses a stream that contains CSV strings and turns it into an iterator of rows.
   */
  def parseStream(
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      parser: UnivocityParser,
      schema: StructType,
      checkHeader: Array[String] => Unit): Iterator[InternalRow] = {
    val tokenizer = parser.tokenizer
    val safeParser = new FailureSafeParser[Array[String]](
      input => Seq(parser.convert(input)),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord,
      parser.options.multiLine)
    convertStream(inputStream, shouldDropHeader, tokenizer, checkHeader) { tokens =>
      safeParser.parse(tokens)
    }.flatten
  }

  private def convertStream[T](
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      tokenizer: CsvParser,
      checkHeader: Array[String] => Unit = _ => ())(
      convert: Array[String] => T) = new Iterator[T] {
    tokenizer.beginParsing(inputStream)
    private var nextRecord = {
      if (shouldDropHeader) {
        val firstRecord = tokenizer.parseNext()
        checkHeader(firstRecord)
      }
      tokenizer.parseNext()
    }

    override def hasNext: Boolean = nextRecord != null

    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
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
      schema: StructType): Iterator[InternalRow] = {
    val options = parser.options

    val filteredLines: Iterator[String] = CSVUtils.filterCommentAndEmpty(lines, options)

    val safeParser = new FailureSafeParser[String](
      input => Seq(parser.parse(input)),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord,
      parser.options.multiLine)
    filteredLines.flatMap(safeParser.parse)
  }
}

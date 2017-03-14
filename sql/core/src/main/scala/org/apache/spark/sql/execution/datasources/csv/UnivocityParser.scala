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
import java.text.NumberFormat
import java.util.Locale

import scala.util.Try
import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class UnivocityParser(
    schema: StructType,
    requiredSchema: StructType,
    private val options: CSVOptions) extends Logging {
  require(requiredSchema.toSet.subsetOf(schema.toSet),
    "requiredSchema should be the subset of schema.")

  def this(schema: StructType, options: CSVOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = String => Any

  private val corruptFieldIndex = schema.getFieldIndex(options.columnNameOfCorruptRecord)
  corruptFieldIndex.foreach { corrFieldIndex =>
    require(schema(corrFieldIndex).dataType == StringType)
    require(schema(corrFieldIndex).nullable)
  }

  private val dataSchema = StructType(schema.filter(_.name != options.columnNameOfCorruptRecord))

  private val tokenizer = new CsvParser(options.asParserSettings)

  private var numMalformedRecords = 0

  private val row = new GenericInternalRow(requiredSchema.length)

  // In `PERMISSIVE` parse mode, we should be able to put the raw malformed row into the field
  // specified in `columnNameOfCorruptRecord`. The raw input is retrieved by this method.
  private def getCurrentInput(): String = tokenizer.getContext.currentParsedContent().stripLineEnd

  // This parser loads an `tokenIndexArr`-th position value in input tokens,
  // then put the value in `row(rowIndexArr)`.
  //
  // For example, let's say there is CSV data as below:
  //
  //   a,b,c
  //   1,2,A
  //
  // Also, let's say `columnNameOfCorruptRecord` is set to "_unparsed", `header` is `true`
  // by user and the user selects "c", "b", "_unparsed" and "a" fields. In this case, we need
  // to map those values below:
  //
  //   required schema - ["c", "b", "_unparsed", "a"]
  //   CSV data schema - ["a", "b", "c"]
  //   required CSV data schema - ["c", "b", "a"]
  //
  // with the input tokens,
  //
  //   input tokens - [1, 2, "A"]
  //
  // Each input token is placed in each output row's position by mapping these. In this case,
  //
  //   output row - ["A", 2, null, 1]
  //
  // In more details,
  // - `valueConverters`, input tokens - CSV data schema
  //   `valueConverters` keeps the positions of input token indices (by its index) to each
  //   value's converter (by its value) in an order of CSV data schema. In this case,
  //   [string->int, string->int, string->string].
  //
  // - `tokenIndexArr`, input tokens - required CSV data schema
  //   `tokenIndexArr` keeps the positions of input token indices (by its index) to reordered
  //   fields given the required CSV data schema (by its value). In this case, [2, 1, 0].
  //
  // - `rowIndexArr`, input tokens - required schema
  //   `rowIndexArr` keeps the positions of input token indices (by its index) to reordered
  //   field indices given the required schema (by its value). In this case, [0, 1, 3].
  private val valueConverters: Array[ValueConverter] =
    dataSchema.map(f => makeConverter(f.name, f.dataType, f.nullable, options)).toArray

  // Only used to create both `tokenIndexArr` and `rowIndexArr`. This variable means
  // the fields that we should try to convert.
  private val reorderedFields = if (options.dropMalformed) {
    // If `dropMalformed` is enabled, then it needs to parse all the values
    // so that we can decide which row is malformed.
    requiredSchema ++ schema.filterNot(requiredSchema.contains(_))
  } else {
    requiredSchema
  }

  private val tokenIndexArr: Array[Int] = {
    reorderedFields
      .filter(_.name != options.columnNameOfCorruptRecord)
      .map(f => dataSchema.indexOf(f)).toArray
  }

  private val rowIndexArr: Array[Int] = if (corruptFieldIndex.isDefined) {
    val corrFieldIndex = corruptFieldIndex.get
    reorderedFields.indices.filter(_ != corrFieldIndex).toArray
  } else {
    reorderedFields.indices.toArray
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
        case datum =>
          Try(datum.toFloat)
            .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).floatValue())
      }

    case _: DoubleType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Double.NaN
        case options.negativeInf => Double.NegativeInfinity
        case options.positiveInf => Double.PositiveInfinity
        case datum =>
          Try(datum.toDouble)
            .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).doubleValue())
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
      nullSafeDatum(d, name, nullable, options)(UTF8String.fromString(_))

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
  def parse(input: String): Option[InternalRow] = convert(tokenizer.parseLine(input))

  private def convert(tokens: Array[String]): Option[InternalRow] = {
    convertWithParseMode(tokens) { tokens =>
      var i: Int = 0
      while (i < tokenIndexArr.length) {
        // It anyway needs to try to parse since it decides if this row is malformed
        // or not after trying to cast in `DROPMALFORMED` mode even if the casted
        // value is not stored in the row.
        val from = tokenIndexArr(i)
        val to = rowIndexArr(i)
        val value = valueConverters(from).apply(tokens(from))
        if (i < requiredSchema.length) {
          row(to) = value
        }
        i += 1
      }
      row
    }
  }

  private def convertWithParseMode(
      tokens: Array[String])(convert: Array[String] => InternalRow): Option[InternalRow] = {
    if (options.dropMalformed && dataSchema.length != tokens.length) {
      if (numMalformedRecords < options.maxMalformedLogPerPartition) {
        logWarning(s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
      }
      if (numMalformedRecords == options.maxMalformedLogPerPartition - 1) {
        logWarning(
          s"More than ${options.maxMalformedLogPerPartition} malformed records have been " +
            "found on this partition. Malformed records from now on will not be logged.")
      }
      numMalformedRecords += 1
      None
    } else if (options.failFast && dataSchema.length != tokens.length) {
      throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
        s"${tokens.mkString(options.delimiter.toString)}")
    } else {
      // If a length of parsed tokens is not equal to expected one, it makes the length the same
      // with the expected. If the length is shorter, it adds extra tokens in the tail.
      // If longer, it drops extra tokens.
      //
      // TODO: Revisit this; if a length of tokens does not match an expected length in the schema,
      // we probably need to treat it as a malformed record.
      // See an URL below for related discussions:
      // https://github.com/apache/spark/pull/16928#discussion_r102657214
      val checkedTokens = if (options.permissive && dataSchema.length != tokens.length) {
        if (dataSchema.length > tokens.length) {
          tokens ++ new Array[String](dataSchema.length - tokens.length)
        } else {
          tokens.take(dataSchema.length)
        }
      } else {
        tokens
      }

      try {
        Some(convert(checkedTokens))
      } catch {
        case NonFatal(e) if options.permissive =>
          val row = new GenericInternalRow(requiredSchema.length)
          corruptFieldIndex.foreach(row(_) = UTF8String.fromString(getCurrentInput()))
          Some(row)
        case NonFatal(e) if options.dropMalformed =>
          if (numMalformedRecords < options.maxMalformedLogPerPartition) {
            logWarning("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
          }
          if (numMalformedRecords == options.maxMalformedLogPerPartition - 1) {
            logWarning(
              s"More than ${options.maxMalformedLogPerPartition} malformed records have been " +
                "found on this partition. Malformed records from now on will not be logged.")
          }
          numMalformedRecords += 1
          None
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
      parser: UnivocityParser): Iterator[InternalRow] = {
    val tokenizer = parser.tokenizer
    convertStream(inputStream, shouldDropHeader, tokenizer) { tokens =>
      parser.convert(tokens)
    }.flatten
  }

  private def convertStream[T](
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      tokenizer: CsvParser)(convert: Array[String] => T) = new Iterator[T] {
    tokenizer.beginParsing(inputStream)
    private var nextRecord = {
      if (shouldDropHeader) {
        tokenizer.parseNext()
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
      shouldDropHeader: Boolean,
      parser: UnivocityParser): Iterator[InternalRow] = {
    val options = parser.options

    val linesWithoutHeader = if (shouldDropHeader) {
      // Note that if there are only comments in the first block, the header would probably
      // be not dropped.
      CSVUtils.dropHeaderLine(lines, options)
    } else {
      lines
    }

    val filteredLines: Iterator[String] =
      CSVUtils.filterCommentAndEmpty(linesWithoutHeader, options)
    filteredLines.flatMap(line => parser.parse(line))
  }
}

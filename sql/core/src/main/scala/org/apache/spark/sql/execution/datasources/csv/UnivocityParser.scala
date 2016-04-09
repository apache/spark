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

import scala.util.control.NonFatal

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Converts CSV string to a sequence of string
 */
private[csv] object UnivocityParser extends Logging{
  /**
   * Convert the input RDD to a tokenized RDD by Univocity
   */
  def tokenize(
      input: RDD[String],
      options: CSVOptions,
      headers: Array[String] = Array.empty): RDD[Array[String]] = {
    input.mapPartitions { iter =>
      tokenizeCsv(iter, options, headers)
    }
  }

  /**
   * Convert the input iterator to a tokenized iterator by Univocity
   */
  def tokenizeCsv(
      iter: Iterator[String],
      options: CSVOptions,
      headers: Array[String] = Array.empty): Iterator[Array[String]] = {
    val settings = getSettings(options)
    if (headers != null) settings.setHeaders(headers: _*)
    val parser = new CsvParser(settings)

    iter.map { record =>
      parser.parseLine(record)
    }
  }

  /**
   * Convert the input RDD to a RDD having [[InternalRow]]
   */
  def parse(
      input: RDD[String],
      schema: StructType,
      requiredSchema: StructType,
      headers: Array[String],
      options: CSVOptions): RDD[InternalRow] = {
    input.mapPartitions { iter =>
      parseCsv(iter, schema, requiredSchema, headers, options)
    }
  }

  /**
   * Convert the input iterator to a iterator having [[InternalRow]]
   */
  def parseCsv(
       input: Iterator[String],
       schema: StructType,
       requiredSchema: StructType,
       headers: Array[String],
       options: CSVOptions): Iterator[InternalRow] = {
    val schemaFields = schema.fields
    val requiredFields = requiredSchema.fields
    val safeRequiredFields = if (options.dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
    schemaFields.zipWithIndex.filter {
      case (field, _) => safeRequiredFields.contains(field)
    }.foreach {
      case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
    }
    val requiredSize = requiredFields.length

    tokenizeCsv(input, options, headers).flatMap { tokens =>
      if (options.dropMalformed && schemaFields.length != tokens.length) {
        logWarning(s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
        None
      } else if (options.failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
          s"${tokens.mkString(options.delimiter.toString)}")
      } else {
        val indexSafeTokens = if (options.permissive && schemaFields.length > tokens.length) {
          tokens ++ new Array[String](schemaFields.length - tokens.length)
        } else if (options.permissive && schemaFields.length < tokens.length) {
          tokens.take(schemaFields.length)
        } else {
          tokens
        }
        try {
          val row = convertTokens(
            indexSafeTokens,
            safeRequiredIndices,
            schemaFields,
            requiredSize,
            options)
          Some(row)
        } catch {
          case NonFatal(e) if options.dropMalformed =>
            logWarning("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(options.delimiter.toString)}")
            None
        }
      }
    }
  }

  /**
   * Convert the tokens to [[InternalRow]]
   */
  private def convertTokens(
      tokens: Array[String],
      requiredIndices: Array[Int],
      schemaFields: Array[StructField],
      requiredSize: Int,
      options: CSVOptions): InternalRow = {
    val row = new GenericMutableRow(requiredSize)
    var index: Int = 0
    var subIndex: Int = 0
    while (subIndex < requiredIndices.length) {
      index = requiredIndices(subIndex)
      val field = schemaFields(index)
      // It anyway needs to try to parse since it decides if this row is malformed
      // or not after trying to cast in `DROPMALFORMED` mode even if the casted
      // value is not stored in the row.
      val value = CSVTypeCast.castTo(
        tokens(index),
        field.dataType,
        field.nullable,
        options.nullValue)
      if (subIndex < requiredSize) {
        row(subIndex) = value
      }
      subIndex = subIndex + 1
    }
    row
  }

  /**
   * Tokenize only single line by Univocity.
   */
  def tokenizeSingleLine(
      line: String,
      options: CSVOptions): Array[String] = {
    val settings = getSettings(options)
    val parser = new CsvParser(settings)
    parser.parseLine(line)
  }

  /**
   * Get a `CsvParserSettings` object from [[CSVOptions]].
   *
   * @param options CSVOptions object containing options
   */
  def getSettings(options: CSVOptions): CsvParserSettings = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(options.delimiter)
    format.setLineSeparator(options.rowSeparator)
    format.setQuote(options.quote)
    format.setQuoteEscape(options.escape)
    format.setComment(options.comment)
    settings.setIgnoreLeadingWhitespaces(options.ignoreLeadingWhiteSpaceFlag)
    settings.setIgnoreTrailingWhitespaces(options.ignoreTrailingWhiteSpaceFlag)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(options.inputBufferSize)
    settings.setMaxColumns(options.maxColumns)
    settings.setNullValue(options.nullValue)
    settings.setMaxCharsPerColumn(options.maxCharsPerColumn)
    settings.setParseUnescapedQuotesUntilDelimiter(true)
    settings
  }
}

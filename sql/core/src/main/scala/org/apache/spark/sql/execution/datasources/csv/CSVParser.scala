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

import java.io.{CharArrayWriter, StringReader}

import com.univocity.parsers.csv._

import org.apache.spark.internal.Logging

/**
 * Read and parse CSV-like input
 *
 * @param params Parameters object
 */
private[csv] class CsvReader(params: CSVOptions) {

  private val parser: CsvParser = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(params.delimiter)
    format.setLineSeparator(params.rowSeparator)
    format.setQuote(params.quote)
    format.setQuoteEscape(params.escape)
    format.setComment(params.comment)
    settings.setIgnoreLeadingWhitespaces(params.ignoreLeadingWhiteSpaceFlag)
    settings.setIgnoreTrailingWhitespaces(params.ignoreTrailingWhiteSpaceFlag)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(params.inputBufferSize)
    settings.setMaxColumns(params.maxColumns)
    settings.setNullValue(params.nullValue)
    settings.setMaxCharsPerColumn(params.maxCharsPerColumn)
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER)

    new CsvParser(settings)
  }

  /**
   * parse a line
   *
   * @param line a String with no newline at the end
   * @return array of strings where each string is a field in the CSV record
   */
  def parseLine(line: String): Array[String] = parser.parseLine(line)
}

/**
 * Converts a sequence of string to CSV string
 *
 * @param params Parameters object for configuration
 * @param headers headers for columns
 */
private[csv] class LineCsvWriter(params: CSVOptions, headers: Seq[String]) extends Logging {
  private val writerSettings = new CsvWriterSettings
  private val format = writerSettings.getFormat

  format.setDelimiter(params.delimiter)
  format.setLineSeparator(params.rowSeparator)
  format.setQuote(params.quote)
  format.setQuoteEscape(params.escape)
  format.setComment(params.comment)

  writerSettings.setNullValue(params.nullValue)
  writerSettings.setEmptyValue(params.nullValue)
  writerSettings.setSkipEmptyLines(true)
  writerSettings.setQuoteAllFields(params.quoteAll)
  writerSettings.setHeaders(headers: _*)
  writerSettings.setQuoteEscapingEnabled(params.escapeQuotes)

  private val buffer = new CharArrayWriter()
  private val writer = new CsvWriter(buffer, writerSettings)

  def writeHeader(): Unit = writer.writeHeaders()

  def writeRow(row: Seq[String]): Unit = writer.writeRow(row.toArray: _*)

  def flush(): String = {
    writer.flush()
    val lines = buffer.toString.stripLineEnd
    buffer.reset()
    lines
  }

  def close(): Unit = {
    writer.close()
  }
}

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

import java.io.{ByteArrayOutputStream, OutputStreamWriter, StringReader}
import java.nio.charset.StandardCharsets

import com.univocity.parsers.csv._

import org.apache.spark.internal.Logging

/**
 * Read and parse CSV-like input
 *
 * @param params Parameters object
 * @param headers headers for the columns
 */
private[sql] abstract class CsvReader(params: CSVOptions, headers: Seq[String]) {

  protected lazy val parser: CsvParser = {
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
    if (headers != null) settings.setHeaders(headers: _*)

    new CsvParser(settings)
  }
}

/**
 * Converts a sequence of string to CSV string
 *
 * @param params Parameters object for configuration
 * @param headers headers for columns
 */
private[sql] class LineCsvWriter(params: CSVOptions, headers: Seq[String]) extends Logging {
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
  writerSettings.setQuoteAllFields(false)
  writerSettings.setHeaders(headers: _*)
  writerSettings.setQuoteEscapingEnabled(params.escapeQuotes)

  private var buffer = new ByteArrayOutputStream()
  private var writer = new CsvWriter(
    new OutputStreamWriter(buffer, StandardCharsets.UTF_8),
    writerSettings)

  def writeRow(row: Seq[String], includeHeader: Boolean): Unit = {
    if (includeHeader) {
      writer.writeHeaders()
    }
    writer.writeRow(row.toArray: _*)
  }

  def flush(): String = {
    writer.close()
    val lines = buffer.toString.stripLineEnd
    buffer = new ByteArrayOutputStream()
    writer = new CsvWriter(
      new OutputStreamWriter(buffer, StandardCharsets.UTF_8),
      writerSettings)
    lines
  }
}

/**
 * Parser for parsing a line at a time. Not efficient for bulk data.
 *
 * @param params Parameters object
 */
private[sql] class LineCsvReader(params: CSVOptions)
  extends CsvReader(params, null) {
  /**
   * parse a line
   *
   * @param line a String with no newline at the end
   * @return array of strings where each string is a field in the CSV record
   */
  def parseLine(line: String): Array[String] = {
    parser.beginParsing(new StringReader(line))
    val parsed = parser.parseNext()
    parser.stopParsing()
    parsed
  }
}

/**
 * Parser for parsing lines in bulk. Use this when efficiency is desired.
 *
 * @param iter iterator over lines in the file
 * @param params Parameters object
 * @param headers headers for the columns
 */
private[sql] class BulkCsvReader(
    iter: Iterator[String],
    params: CSVOptions,
    headers: Seq[String])
  extends CsvReader(params, headers) with Iterator[Array[String]] {

  private val reader = new StringIteratorReader(iter)
  parser.beginParsing(reader)
  private var nextRecord = parser.parseNext()

  /**
   * get the next parsed line.
   * @return array of strings where each string is a field in the CSV record
   */
  override def next(): Array[String] = {
    val curRecord = nextRecord
    if(curRecord != null) {
      nextRecord = parser.parseNext()
    } else {
      throw new NoSuchElementException("next record is null")
    }
    curRecord
  }

  override def hasNext: Boolean = nextRecord != null

}

/**
 * A Reader that "reads" from a sequence of lines. Spark's textFile method removes newlines at
 * end of each line Univocity parser requires a Reader that provides access to the data to be
 * parsed and needs the newlines to be present
 * @param iter iterator over RDD[String]
 */
private class StringIteratorReader(val iter: Iterator[String]) extends java.io.Reader {

  private var next: Long = 0
  private var length: Long = 0  // length of input so far
  private var start: Long = 0
  private var str: String = null   // current string from iter

  /**
   * fetch next string from iter, if done with current one
   * pretend there is a new line at the end of every string we get from from iter
   */
  private def refill(): Unit = {
    if (length == next) {
      if (iter.hasNext) {
        str = iter.next()
        start = length
        length += (str.length + 1) // allowance for newline removed by SparkContext.textFile()
      } else {
        str = null
      }
    }
  }

  /**
   * read the next character, if at end of string pretend there is a new line
   */
  override def read(): Int = {
    refill()
    if (next >= length) {
      -1
    } else {
      val cur = next - start
      next += 1
      if (cur == str.length) '\n' else str.charAt(cur.toInt)
    }
  }

  /**
   * read from str into cbuf
   */
  override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
    refill()
    var n = 0
    if ((off < 0) || (off > cbuf.length) || (len < 0) ||
      ((off + len) > cbuf.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException()
    } else if (len == 0) {
      n = 0
    } else {
      if (next >= length) {   // end of input
        n = -1
      } else {
        n = Math.min(length - next, len).toInt // lesser of amount of input available or buf size
        if (n == length - next) {
          str.getChars((next - start).toInt, (next - start + n - 1).toInt, cbuf, off)
          cbuf(off + n - 1) = '\n'
        } else {
          str.getChars((next - start).toInt, (next - start + n).toInt, cbuf, off)
        }
        next += n
        if (n < len) {
          val m = read(cbuf, off + n, len - n)  // have more space, fetch more input from iter
          if(m != -1) n += m
        }
      }
    }

    n
  }

  override def skip(ns: Long): Long = {
    throw new IllegalArgumentException("Skip not implemented")
  }

  override def ready: Boolean = {
    refill()
    true
  }

  override def markSupported: Boolean = false

  override def mark(readAheadLimit: Int): Unit = {
    throw new IllegalArgumentException("Mark not implemented")
  }

  override def reset(): Unit = {
    throw new IllegalArgumentException("Mark and hence reset not implemented")
  }

  override def close(): Unit = { }
}

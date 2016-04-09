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

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Converts a sequence of string to CSV string
 */
private[csv] object UnivocityGenerator extends Logging {
  /**
   * Transforms a single InternalRow to CSV using Univocity
   *
   * @param rowSchema the schema object used for conversion
   * @param writer a CsvWriter object
   * @param headers headers to write
   * @param writeHeader true if it needs to write header
   * @param options CSVOptions object containing options
   * @param row The row to convert
   */
  def apply(
      rowSchema: StructType,
      writer: CsvWriter,
      headers: Array[String],
      writeHeader: Boolean,
      options: CSVOptions)(row: InternalRow): Unit = {
    val tokens = {
      row.toSeq(rowSchema).map { field =>
        if (field != null) {
          field.toString
        } else {
          options.nullValue
        }
      }
    }
    if (writeHeader) {
      writer.writeHeaders()
    }
    writer.writeRow(tokens: _*)
  }

  /**
   * Get a `CsvWriterSettings` object from [[CSVOptions]].
   *
   * @param options CSVOptions object containing options
   */
  def getSettings(options: CSVOptions): CsvWriterSettings = {
    val writerSettings = new CsvWriterSettings()
    val format = writerSettings.getFormat
    format.setDelimiter(options.delimiter)
    format.setLineSeparator(options.rowSeparator)
    format.setQuote(options.quote)
    format.setQuoteEscape(options.escape)
    format.setComment(options.comment)
    writerSettings.setNullValue(options.nullValue)
    writerSettings.setEmptyValue(options.nullValue)
    writerSettings.setSkipEmptyLines(true)
    writerSettings.setQuoteAllFields(false)
    writerSettings
  }
}

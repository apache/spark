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

import com.univocity.parsers.common.AbstractParser
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys.{CSV_HEADER_COLUMN_NAME, CSV_HEADER_COLUMN_NAMES, CSV_HEADER_LENGTH, CSV_SCHEMA_FIELD_NAME, CSV_SCHEMA_FIELD_NAMES, CSV_SOURCE, NUM_COLUMNS}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Checks that column names in a CSV header and field names in the schema are the same
 * by taking into account case sensitivity.
 *
 * @param schema provided (or inferred) schema to which CSV must conform.
 * @param options parsed CSV options.
 * @param source name of CSV source that are currently checked. It is used in error messages.
 * @param isStartOfFile indicates if the currently processing partition is the start of the file.
 *                      if unknown or not applicable (for instance when the input is a dataset),
 *                      can be omitted.
 */
class CSVHeaderChecker(
    schema: StructType,
    options: CSVOptions,
    source: String,
    isStartOfFile: Boolean = false) extends Logging {

  // Indicates if it is set to `false`, comparison of column names and schema field
  // names is not case sensitive.
  private val caseSensitive = SQLConf.get.caseSensitiveAnalysis

  // Indicates if it is `true`, column names are ignored otherwise the CSV column
  // names are checked for conformance to the schema. In the case if
  // the column name don't conform to the schema, an exception is thrown.
  private val enforceSchema = options.enforceSchema

  /**
   * Checks that column names in a CSV header and field names in the schema are the same
   * by taking into account case sensitivity.
   *
   * @param columnNames names of CSV columns that must be checked against to the schema.
   */
  private def checkHeaderColumnNames(columnNames: Array[String]): Unit = {
    if (columnNames != null) {
      val fieldNames = schema.map(_.name).toIndexedSeq
      val (headerLen, schemaSize) = (columnNames.length, fieldNames.length)
      var errorMessage: Option[MessageWithContext] = None

      if (headerLen == schemaSize) {
        var i = 0
        while (errorMessage.isEmpty && i < headerLen) {
          var (nameInSchema, nameInHeader) = (fieldNames(i), columnNames(i))
          if (!caseSensitive) {
            // scalastyle:off caselocale
            nameInSchema = nameInSchema.toLowerCase
            nameInHeader = nameInHeader.toLowerCase
            // scalastyle:on caselocale
          }
          if (nameInHeader != nameInSchema) {
            // scalastyle:off line.size.limit
            errorMessage = Some(
              log"""|CSV header does not conform to the schema.
                    | Header: ${MDC(CSV_HEADER_COLUMN_NAMES, columnNames.mkString(", "))}
                    | Schema: ${MDC(CSV_SCHEMA_FIELD_NAMES, fieldNames.mkString(", "))}
                    |Expected: ${MDC(CSV_SCHEMA_FIELD_NAME, fieldNames(i))} but found: ${MDC(CSV_HEADER_COLUMN_NAME, columnNames(i))}
                    |${MDC(CSV_SOURCE, source)}""".stripMargin)
            // scalastyle:on line.size.limit
          }
          i += 1
        }
      } else {
        errorMessage = Some(
          // scalastyle:off line.size.limit
          log"""|Number of column in CSV header is not equal to number of fields in the schema:
                | Header length: ${MDC(CSV_HEADER_LENGTH, headerLen)}, schema size: ${MDC(NUM_COLUMNS, schemaSize)}
                |${MDC(CSV_SOURCE, source)}""".stripMargin)
          // scalastyle:on line.size.limit
      }

      errorMessage.foreach { msg =>
        if (enforceSchema) {
          logWarning(msg)
        } else {
          throw new SparkIllegalArgumentException(
            errorClass = "_LEGACY_ERROR_TEMP_3241",
            messageParameters = Map("msg" -> msg.message))
        }
      }
    }
  }

  // This is currently only used to parse CSV from Dataset[String].
  def checkHeaderColumnNames(line: String): Unit = {
    if (options.headerFlag) {
      val parser = new CsvParser(options.asParserSettings)
      checkHeaderColumnNames(parser.parseLine(line))
    }
  }

  // This is currently only used to parse CSV with multiLine mode.
  private[csv] def checkHeaderColumnNames(tokenizer: AbstractParser[CsvParserSettings]): Unit = {
    assert(options.multiLine, "This method should be executed with multiLine.")
    if (options.headerFlag) {
      val firstRecord = tokenizer.parseNext()
      checkHeaderColumnNames(firstRecord)
    }
  }

  // This is currently only used to parse CSV with non-multiLine mode.
  private[csv] def checkHeaderColumnNames(
      lines: Iterator[String], tokenizer: AbstractParser[CsvParserSettings]): Unit = {
    assert(!options.multiLine, "This method should not be executed with multiline.")
    // Checking that column names in the header are matched to field names of the schema.
    // The header will be removed from lines.
    // Note: if there are only comments in the first block, the header would probably
    // be not extracted.
    if (options.headerFlag && isStartOfFile) {
      CSVExprUtils.extractHeader(lines, options).foreach { header =>
        checkHeaderColumnNames(tokenizer.parseLine(header))
      }
    }
  }
}

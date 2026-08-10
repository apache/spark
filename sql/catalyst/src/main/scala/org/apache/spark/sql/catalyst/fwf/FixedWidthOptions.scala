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

package org.apache.spark.sql.catalyst.fwf

import java.nio.charset.StandardCharsets
import java.time.ZoneId
import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.csv.{CSVExprUtils, CSVOptions}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils, ParseMode, PermissiveMode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

/** Where the field boundaries for a fixed-width file come from. */
sealed trait FwfColSpecs
/** Infer colspecs from a sample of the data. */
case object InferFwfColSpecs extends FwfColSpecs
/** Explicit half-open `(from, to)` character intervals, one per field. */
case class ExplicitFwfColSpecs(colspecs: Seq[(Int, Int)]) extends FwfColSpecs

class FixedWidthOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    private val defaultTimeZoneId: String,
    private val defaultColumnNameOfCorruptRecord: String)
  extends FileSourceOptions(parameters) with Logging {

  import FixedWidthOptions._

  override def equals(obj: Any): Boolean = obj match {
    case other: FixedWidthOptions =>
      (parameters == null && other.parameters == null ||
        parameters != null && parameters == other.parameters) &&
        defaultTimeZoneId == other.defaultTimeZoneId &&
        defaultColumnNameOfCorruptRecord == other.defaultColumnNameOfCorruptRecord
    case _ => false
  }

  override def hashCode(): Int = {
    var result = Option(parameters).map(_.hashCode()).getOrElse(0)
    result = 31 * result + defaultTimeZoneId.hashCode()
    result = 31 * result + defaultColumnNameOfCorruptRecord.hashCode()
    result
  }

  def this(parameters: Map[String, String], defaultTimeZoneId: String) = {
    this(
      CaseInsensitiveMap(parameters),
      defaultTimeZoneId,
      SQLConf.get.columnNameOfCorruptRecord)
  }

  def this(
      parameters: Map[String, String],
      defaultTimeZoneId: String,
      defaultColumnNameOfCorruptRecord: String) = {
    this(CaseInsensitiveMap(parameters), defaultTimeZoneId, defaultColumnNameOfCorruptRecord)
  }

  private def getInt(paramName: String, default: Int): Int = {
    parameters.get(paramName) match {
      case None => default
      case Some(value) =>
        try {
          value.toInt
        } catch {
          case _: NumberFormatException =>
            throw QueryExecutionErrors.paramIsNotIntegerError(paramName, value)
        }
    }
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    parameters.get(paramName) match {
      case None => default
      case Some(value) if value.toLowerCase(Locale.ROOT) == "true" => true
      case Some(value) if value.toLowerCase(Locale.ROOT) == "false" => false
      case Some(_) => throw QueryExecutionErrors.paramIsNotBooleanValueError(paramName)
    }
  }

  private def parseWidths(value: String): Seq[Int] = {
    value.split(",").map(_.trim).map { w =>
      try {
        w.toInt
      } catch {
        case _: NumberFormatException =>
          throw QueryExecutionErrors.malformedFwfWidthsError(value)
      }
    }.toSeq
  }

  private def parseColspecs(value: String): Seq[(Int, Int)] = {
    value.split(",").map(_.trim).map { spec =>
      spec.split("-", 2) match {
        case Array(from, to) =>
          try {
            (from.trim.toInt, to.trim.toInt)
          } catch {
            case _: NumberFormatException =>
              throw QueryExecutionErrors.malformedFwfColspecsError(value)
          }
        case _ =>
          throw QueryExecutionErrors.malformedFwfColspecsError(value)
      }
    }.toSeq
  }

  private def parseSkipRows(value: String): Set[Int] = {
    try {
      if (value.contains(",")) {
        value.split(",").map(_.trim.toInt).toSet
      } else {
        val n = value.trim.toInt
        if (n < 0) {
          throw QueryExecutionErrors.negativeFwfSkipRowsError(value)
        }
        (0 until n).toSet
      }
    } catch {
      case _: NumberFormatException =>
        throw QueryExecutionErrors.malformedFwfSkipRowsError(value)
    }
  }

  /**
   * 0-indexed row numbers to skip. An integer value skips the first N rows; a comma-separated
   * list skips those specific row indices, anywhere in the file. Numbered before `header` is
   * applied, so the header (if any) is the first row remaining after skipping, not necessarily
   * row 0. Reading a file with this set disables splitting it across partitions, since a
   * mid-file partition has no way to count rows from the start of the file.
   */
  val skipRows: Set[Int] = parameters.get(SKIP_ROWS).map(parseSkipRows).getOrElse(Set.empty)

  private val colspecsParam: Option[String] = parameters.get(COLSPECS)
  private val widthsParam: Option[String] = parameters.get(WIDTHS)

  if (colspecsParam.exists(v => !v.equalsIgnoreCase(INFER)) && widthsParam.isDefined) {
    throw QueryExecutionErrors.conflictingFwfColspecsAndWidthsError()
  }

  val colSpecs: FwfColSpecs = widthsParam match {
    case Some(w) => ExplicitFwfColSpecs(FixedWidthUtils.widthsToColspecs(parseWidths(w)))
    case None =>
      colspecsParam match {
        case Some(v) if v.equalsIgnoreCase(INFER) => InferFwfColSpecs
        case Some(v) => ExplicitFwfColSpecs(parseColspecs(v))
        case None => InferFwfColSpecs
      }
  }

  val inferNrows: Int = getInt(INFER_NROWS, 100)

  val header: Boolean = getBool(HEADER, default = false)

  val comment: Option[Char] = parameters.get(COMMENT).filter(_.nonEmpty).map(CSVExprUtils.toChar)

  val delimiter: Option[String] = parameters.get(DELIMITER).map(CSVExprUtils.toDelimiterStr)

  val charset: String = parameters.get(ENCODING)
    .orElse(parameters.get(CHARSET))
    .getOrElse(StandardCharsets.UTF_8.name())

  val nullValue: String = parameters.getOrElse(NULL_VALUE, "")

  val dateFormatOption: Option[String] = parameters.get(DATE_FORMAT)
  val timestampFormatOption: Option[String] = parameters.get(TIMESTAMP_FORMAT)
  val timeZone: String = parameters.getOrElse(TIME_ZONE, defaultTimeZoneId)
  val zoneId: ZoneId = DateTimeUtils.getZoneId(timeZone)
  val locale: Locale = parameters.get(LOCALE).map(Locale.forLanguageTag).getOrElse(Locale.US)

  val parseMode: ParseMode =
    parameters.get(MODE).map(ParseMode.fromString).getOrElse(PermissiveMode)

  val columnNameOfCorruptRecord: String =
    parameters.getOrElse(COLUMN_NAME_OF_CORRUPT_RECORD, defaultColumnNameOfCorruptRecord)

  /**
   * A [[CSVOptions]] carrying just the fields shared with CSV (`nullValue`, date/timestamp
   * formats, time zone, locale, corrupt-record column), for `CSVInferSchema` to use so
   * fixed-width files get the same type-inference behavior as every other file source.
   * `CSVOptions.inferSchemaFlag` defaults to `false`, so it's forced on here.
   */
  def toCSVOptionsShim: CSVOptions = new CSVOptions(
    parameters.toMap + ("inferSchema" -> "true"),
    columnPruning = false,
    defaultTimeZoneId,
    defaultColumnNameOfCorruptRecord)
}

object FixedWidthOptions extends DataSourceOptions {
  val COLSPECS = newOption("colspecs")
  val WIDTHS = newOption("widths")
  val INFER = "infer"
  val INFER_NROWS = newOption("inferNrows")
  val HEADER = newOption("header")
  val COMMENT = newOption("comment")
  val DELIMITER = newOption("delimiter")
  val NULL_VALUE = newOption("nullValue")
  val DATE_FORMAT = newOption("dateFormat")
  val TIMESTAMP_FORMAT = newOption("timestampFormat")
  val TIME_ZONE = newOption("timeZone")
  val LOCALE = newOption("locale")
  val MODE = newOption("mode")
  val COLUMN_NAME_OF_CORRUPT_RECORD = newOption(DataSourceOptions.COLUMN_NAME_OF_CORRUPT_RECORD)
  val SKIP_ROWS = newOption("skipRows")
  val ENCODING = "encoding"
  val CHARSET = "charset"
  newOption(ENCODING, CHARSET)
}

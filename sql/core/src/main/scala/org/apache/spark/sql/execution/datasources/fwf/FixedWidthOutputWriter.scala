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

package org.apache.spark.sql.execution.datasources.fwf

import java.nio.charset.Charset

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ToStringBase
import org.apache.spark.sql.catalyst.fwf.{ExplicitFwfColSpecs, FixedWidthOptions, InferFwfColSpecs}
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types._

/**
 * Writes rows as fixed-width text, padded or truncated to each column's configured width and
 * concatenated with no separator. Mirrors `UnivocityGenerator`'s per-type `makeConverter` shape
 * (date/timestamp values go through `options`-configured formatters, everything else uses its
 * plain `toString`), minus the complex-type cases CSV needs and FWF does not.
 */
class FixedWidthOutputWriter(
    val path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    options: FixedWidthOptions) extends OutputWriter {

  private val widths: Array[Int] = options.colSpecs match {
    case ExplicitFwfColSpecs(specs) => specs.map { case (from, to) => to - from }.toArray
    case InferFwfColSpecs =>
      throw QueryExecutionErrors.fwfWriteRequiresExplicitColspecsError()
  }
  if (widths.length != dataSchema.length) {
    throw QueryExecutionErrors.fwfWidthCountMismatchError(widths.length, dataSchema.length)
  }

  private val charset = Charset.forName(options.charset)
  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), charset)

  private type ValueConverter = (InternalRow, Int) => String

  private lazy val dateFormatter = DateFormatter(
    options.dateFormatOption,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormatOption,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false)
  private lazy val timestampNTZFormatter = TimestampFormatter(
    options.timestampFormatOption,
    options.zoneId,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = false,
    forTimestampNTZ = true)
  private lazy val binaryFormatter = ToStringBase.getBinaryFormatter

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case _: BinaryType => (row, i) => binaryFormatter(row.getBinary(i)).toString
    case _: DateType => (row, i) => dateFormatter.format(row.getInt(i))
    case _: TimestampType => (row, i) => timestampFormatter.format(row.getLong(i))
    case _: TimestampNTZType =>
      (row, i) => timestampNTZFormatter.format(DateTimeUtils.microsToLocalDateTime(row.getLong(i)))
    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)
    case dt: DataType => (row, i) => row.get(i, dt).toString
  }

  private val valueConverters: Array[ValueConverter] =
    dataSchema.fields.map(f => makeConverter(f.dataType))

  private def fieldToString(row: InternalRow, i: Int): String = {
    if (row.isNullAt(i)) options.nullValue else valueConverters(i)(row, i)
  }

  private def writeLine(fields: Seq[String]): Unit = {
    val sb = new StringBuilder
    var i = 0
    while (i < widths.length) {
      val width = widths(i)
      val str = fields(i)
      if (str.length >= width) {
        sb.append(str.substring(0, width))
      } else {
        sb.append(str)
        var pad = width - str.length
        while (pad > 0) {
          sb.append(' ')
          pad -= 1
        }
      }
      i += 1
    }
    writer.write(sb.toString())
    writer.write("\n")
  }

  if (options.header) {
    writeLine(dataSchema.fieldNames.toSeq)
  }

  override def write(row: InternalRow): Unit = {
    writeLine(dataSchema.fields.indices.map(i => fieldToString(row, i)))
  }

  override def close(): Unit = writer.close()
}

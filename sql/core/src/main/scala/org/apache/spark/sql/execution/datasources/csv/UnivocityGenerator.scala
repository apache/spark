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

import java.io.Writer

import com.univocity.parsers.csv.CsvWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimestampParser
import org.apache.spark.sql.types._

private[csv] class UnivocityGenerator(
    schema: StructType,
    writer: Writer,
    options: CSVOptions) {
  private val writerSettings = options.asWriterSettings
  writerSettings.setHeaders(schema.fieldNames: _*)
  private val gen = new CsvWriter(writer, writerSettings)
  private var printHeader = options.headerFlag

  // A `ValueConverter` is responsible for converting a value of an `InternalRow` to `String`.
  // When the value is null, this converter should not be called.
  private type ValueConverter = (InternalRow, Int) => String

  // `ValueConverter`s for all values in the fields of the schema
  private val valueConverters: Array[ValueConverter] =
    schema.map(_.dataType).map(makeConverter).toArray

  @transient private lazy val timestampParser = new TimestampParser(options.timestampFormat)

  private def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case DateType =>
      (row: InternalRow, ordinal: Int) =>
        options.dateFormat.format(DateTimeUtils.toJavaDate(row.getInt(ordinal)))

    case TimestampType =>
      (row: InternalRow, ordinal: Int) => timestampParser.format(row.getLong(ordinal))

    case udt: UserDefinedType[_] => makeConverter(udt.sqlType)

    case dt: DataType =>
      (row: InternalRow, ordinal: Int) =>
        row.get(ordinal, dt).toString
  }

  private def convertRow(row: InternalRow): Seq[String] = {
    var i = 0
    val values = new Array[String](row.numFields)
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        values(i) = valueConverters(i).apply(row, i)
      } else {
        values(i) = options.nullValue
      }
      i += 1
    }
    values
  }

  /**
   * Writes a single InternalRow to CSV using Univocity.
   */
  def write(row: InternalRow): Unit = {
    if (printHeader) {
      gen.writeHeaders()
    }
    gen.writeRow(convertRow(row): _*)
    printHeader = false
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()
}

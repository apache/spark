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
package org.apache.spark.sql.catalyst.expressions.csv

import com.univocity.parsers.csv.CsvParser

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{DataSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.csv.{CSVInferSchema, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util.{FailFastMode, FailureSafeParser, PermissiveMode}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * The expression `CsvToStructs` will utilize it to support codegen.
 */
case class CsvToStructsEvaluator(
    options: Map[String, String],
    nullableSchema: StructType,
    nameOfCorruptRecord: String,
    timeZoneId: Option[String],
    requiredSchema: Option[StructType],
    corruptRecordExactMatch: Boolean) {

  // This converts parsed rows to the desired output by the given schema.
  @transient
  private lazy val converter = (rows: Iterator[InternalRow]) => {
    if (!rows.hasNext) {
      throw SparkException.internalError("Expected one row from CSV parser.")
    }
    val result = rows.next()
    // CSV's parser produces one record only.
    assert(!rows.hasNext)
    result
  }

  @transient
  private lazy val parser = {
    // 'lineSep' is a plan-wise option so we set a noncharacter, according to
    // the unicode specification, which should not appear in Java's strings.
    // See also SPARK-38955 and https://www.unicode.org/charts/PDF/UFFF0.pdf.
    // scalastyle:off nonascii
    val exprOptions = options ++ Map("lineSep" -> '\uFFFF'.toString)
    // scalastyle:on nonascii
    def csvOptions(parameters: Map[String, String]): CSVOptions = new CSVOptions(
      parameters,
      columnPruning = true,
      defaultTimeZoneId = timeZoneId.get,
      defaultColumnNameOfCorruptRecord = nameOfCorruptRecord)
    // Resolve the corrupt record column once, also checking its type, and parse with options
    // that carry the resolved name, so that every comparison below is exact.
    val initialOptions = csvOptions(exprOptions)
    val resolvedOptions = ExprUtils.withResolvedColumnNameOfCorruptRecord(
      exprOptions, nullableSchema, initialOptions.columnNameOfCorruptRecord,
      corruptRecordExactMatch)
    val parsedOptions =
      if (resolvedOptions eq exprOptions) initialOptions else csvOptions(resolvedOptions)
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("from_csv", mode)
    }
    DataSourceOptions.validateSingleVariantColumn(parsedOptions.parameters, Some(nullableSchema))

    val actualSchema =
      StructType(nullableSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    // The schema of the rows this returns: the pruned one when the optimizer asked for a
    // single field, the full one otherwise.
    val outputSchema = requiredSchema.map(_.asNullable).getOrElse(nullableSchema)
    val actualRequiredSchema =
      StructType(outputSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val rawParser = new UnivocityParser(actualSchema,
      actualRequiredSchema,
      parsedOptions)
    new FailureSafeParser[String](
      input => rawParser.parse(input),
      mode,
      // Not `nullableSchema`: the parser emits rows of the required schema, and
      // `FailureSafeParser` copies them by position.
      outputSchema,
      parsedOptions.columnNameOfCorruptRecord)
  }

  final def evaluate(csv: UTF8String): InternalRow = {
    if (csv == null) return null
    converter(parser.parse(csv.toString))
  }
}

case class SchemaOfCsvEvaluator(options: Map[String, String]) {

  @transient
  private lazy val csvOptions: CSVOptions = {
    // 'lineSep' is a plan-wise option so we set a noncharacter, according to
    // the unicode specification, which should not appear in Java's strings.
    // See also SPARK-38955 and https://www.unicode.org/charts/PDF/UFFF0.pdf.
    // scalastyle:off nonascii
    val exprOptions = options ++ Map("lineSep" -> '\uFFFF'.toString)
    // scalastyle:on nonascii
    new CSVOptions(exprOptions, true, "UTC")
  }

  @transient
  private lazy val csvParser: CsvParser = new CsvParser(csvOptions.asParserSettings)

  @transient
  private lazy val csvInferSchema = new CSVInferSchema(csvOptions)

  final def evaluate(csv: UTF8String): Any = {
    val row = csvParser.parseLine(csv.toString)
    assert(row != null, "Parsed CSV record should not be null.")
    val header = row.zipWithIndex.map { case (_, index) => s"_c$index" }
    val startType: Array[DataType] = Array.fill[DataType](header.length)(NullType)
    val fieldTypes = csvInferSchema.inferRowType(startType, row)
    val st = StructType(csvInferSchema.toStructFields(fieldTypes, header))
    UTF8String.fromString(st.sql)
  }
}

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
package org.apache.spark.sql.execution.datasources.v2.csv

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create CSV readers.
 *
 * @param sqlConf SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param dataSchema Schema of CSV files.
 * @param readDataSchema Required data schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options Options for parsing CSV files.
 */
case class CSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: CSVOptions,
    filters: Seq[Filter]) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val actualDataSchema = StructType(
      dataSchema.filterNot(_.name == options.columnNameOfCorruptRecord))
    val actualReadDataSchema = StructType(
      readDataSchema.filterNot(_.name == options.columnNameOfCorruptRecord))
    val parser = new UnivocityParser(
      actualDataSchema,
      actualReadDataSchema,
      options,
      filters)
    val schema = if (options.columnPruning) actualReadDataSchema else actualDataSchema
    val isStartOfFile = file.start == 0
    val headerChecker = new CSVHeaderChecker(
      schema, options, source = s"CSV file: ${file.filePath}", isStartOfFile)
    val iter = CSVDataSource(options).readFile(
      conf,
      file,
      parser,
      headerChecker,
      readDataSchema)
    val defaultColumnWrapper = new NullValuesToDefaultColumns(iter, dataSchema, "CSV")
    val fileReader = new PartitionReaderFromIterator[InternalRow](defaultColumnWrapper)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}

/**
 * Wraps an iterator of InternalRow elements and replaces any NULL values for columns with mapped
 * DEFAULT values in the schema. This works for data sources that only return NULL values when the
 * corresponding data is not present in storage, such as CSV scans.
 * @param iter an iterator to InternalRow data produced by the data source.
 * @param readDataSchema Required data schema in the batch scan.
 * @param dataSourceType String identifying the type of data scanned, useful for error messages.
 */
case class NullValuesToDefaultColumns(
    iter: Iterator[InternalRow],
    readDataSchema: StructType,
    dataSourceType: String) extends Iterator[InternalRow] {
  def hasNext: Boolean = iter.hasNext
  def next(): InternalRow = {
    val row: InternalRow = iter.next()
    lazy val defaultValues: Seq[Option[Any]] = readDataSchema.fields.map { field =>
      lazy val parser = new SparkSqlParser
      val colText: Option[String] = field.getExistenceDefaultValue()
      colText.map { s =>
        val result = try {
          parser.parseExpression(s) match {
            case ExprLiteral(value, _) => value
          }
        } catch {
          case _: ParseException | _: MatchError =>
            throw QueryCompilationErrors.failedToParseExistenceDefaultAsLiteral(
              dataSourceType, field.name, s)
        }
        Some(result)
      }.getOrElse(None)
    }
    defaultValues.zipWithIndex.foreach { case (value: Option[Any], i: Int) =>
      if (value.isDefined) {
        row.update(i, value.get)
      }
    }
    row
  }
}

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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.fwf.{FixedWidthOptions, FixedWidthParser}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Provides access to fixed-width files from pure SQL statements.
 */
case class FixedWidthFileFormat() extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "fwf"

  override def toString: String = "FixedWidth"

  // `skipRows` numbers rows from the start of the file, so a file read that way must be read as
  // a single whole-file partition; see `FixedWidthDataSource.readFile`.
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    getFwfOptions(sparkSession, options).skipRows.isEmpty &&
      super.isSplitable(sparkSession, options, path)
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = getFwfOptions(sparkSession, options)
    FixedWidthDataSource.inferSchema(sparkSession, files, parsedOptions)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val parsedOptions = getFwfOptions(sparkSession, options)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new FixedWidthOutputWriter(path, dataSchema, context, parsedOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String = ".txt"
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)
    val parsedOptions = getFwfOptions(sparkSession, options)

    // Check a field requirement for corrupt records here to throw an exception in a driver side
    ExprUtils.verifyColumnNameOfCorruptRecord(dataSchema, parsedOptions.columnNameOfCorruptRecord)
    if (requiredSchema.length == 1 &&
      requiredSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      throw QueryCompilationErrors.queryFromRawFilesIncludeCorruptRecordColumnError()
    }

    // The corrupt-record column is virtual -- it is not a field in the file -- so drop it
    // before resolving colspecs and constructing the parser. FailureSafeParser still sees
    // `requiredSchema` and fills the column in. Mirrors CSVFileFormat.buildReader.
    val actualDataSchema = StructType(
      dataSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val actualRequiredSchema = StructType(
      requiredSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val colspecs = FixedWidthDataSource.resolveColspecsForRead(parsedOptions, actualDataSchema)
    val readColspecs =
      FixedWidthDataSource.effectiveColspecs(actualDataSchema, actualRequiredSchema, colspecs)

    // Don't push any filter which refers to the "virtual" column which cannot present in the
    // input. Such filters will be applied later on the upper layer.
    val actualFilters =
      filters.filterNot(_.references.contains(parsedOptions.columnNameOfCorruptRecord))

    (file: PartitionedFile) => {
      val conf = broadcastedHadoopConf.value.value
      val parser = new FixedWidthParser(
        actualDataSchema, actualRequiredSchema, parsedOptions, actualFilters)
      FixedWidthDataSource.readFile(
        conf, file, readColspecs, parser, parsedOptions, requiredSchema)
    }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)
    case _ => false
  }

  private def getFwfOptions(
      sparkSession: SparkSession,
      options: Map[String, String]): FixedWidthOptions = {
    new FixedWidthOptions(options, sparkSession.sessionState.conf.sessionLocalTimeZone)
  }
}

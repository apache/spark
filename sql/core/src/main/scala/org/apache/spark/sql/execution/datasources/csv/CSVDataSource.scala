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

import java.nio.charset.{Charset, StandardCharsets}

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StructType

/**
 * Common functions for parsing CSV files
 */
abstract class CSVDataSource extends Serializable {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into [[InternalRow]] instances.
   */
  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      schema: StructType): Iterator[InternalRow]

  /**
   * Infers the schema from `inputPaths` files.
   */
  final def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      Some(infer(sparkSession, inputPaths, parsedOptions))
    } else {
      None
    }
  }

  protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType

  /**
   * Generates a header from the given row which is null-safe and duplicate-safe.
   */
  protected def makeSafeHeader(
      row: Array[String],
      caseSensitive: Boolean,
      options: CSVOptions): Array[String] = {
    if (options.headerFlag) {
      val duplicates = {
        val headerNames = row.filter(_ != null)
          .map(name => if (caseSensitive) name else name.toLowerCase)
        headerNames.diff(headerNames.distinct).distinct
      }

      row.zipWithIndex.map { case (value, index) =>
        if (value == null || value.isEmpty || value == options.nullValue) {
          // When there are empty strings or the values set in `nullValue`, put the
          // index as the suffix.
          s"_c$index"
        } else if (!caseSensitive && duplicates.contains(value.toLowerCase)) {
          // When there are case-insensitive duplicates, put the index as the suffix.
          s"$value$index"
        } else if (duplicates.contains(value)) {
          // When there are duplicates, put the index as the suffix.
          s"$value$index"
        } else {
          value
        }
      }
    } else {
      row.zipWithIndex.map { case (_, index) =>
        // Uses default column names, "_c#" where # is its position of fields
        // when header option is disabled.
        s"_c$index"
      }
    }
  }
}

object CSVDataSource {
  def apply(options: CSVOptions): CSVDataSource = {
    if (options.multiLine) {
      MultiLineCSVDataSource
    } else {
      TextInputCSVDataSource
    }
  }
}

object TextInputCSVDataSource extends CSVDataSource {
  override val isSplitable: Boolean = true

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      schema: StructType): Iterator[InternalRow] = {
    val lines = {
      val linesReader = new HadoopFileLinesReader(file, conf)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }

    val shouldDropHeader = parser.options.headerFlag && file.start == 0
    UnivocityParser.parseIterator(lines, shouldDropHeader, parser, schema)
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    val csv = createBaseDataset(sparkSession, inputPaths, parsedOptions)
    val maybeFirstLine = CSVUtils.filterCommentAndEmpty(csv, parsedOptions).take(1).headOption
    inferFromDataset(sparkSession, csv, maybeFirstLine, parsedOptions)
  }

  /**
   * Infers the schema from `Dataset` that stores CSV string records.
   */
  def inferFromDataset(
      sparkSession: SparkSession,
      csv: Dataset[String],
      maybeFirstLine: Option[String],
      parsedOptions: CSVOptions): StructType = maybeFirstLine match {
    case Some(firstLine) =>
      val firstRow = new CsvParser(parsedOptions.asParserSettings).parseLine(firstLine)
      val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
      val header = makeSafeHeader(firstRow, caseSensitive, parsedOptions)
      val tokenRDD = csv.rdd.mapPartitions { iter =>
        val filteredLines = CSVUtils.filterCommentAndEmpty(iter, parsedOptions)
        val linesWithoutHeader =
          CSVUtils.filterHeaderLine(filteredLines, firstLine, parsedOptions)
        val parser = new CsvParser(parsedOptions.asParserSettings)
        linesWithoutHeader.map(parser.parseLine)
      }
      CSVInferSchema.infer(tokenRDD, header, parsedOptions)
    case None =>
      // If the first line could not be read, just return the empty schema.
      StructType(Nil)
  }

  private def createBaseDataset(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: CSVOptions): Dataset[String] = {
    val paths = inputPaths.map(_.getPath.toString)
    if (Charset.forName(options.charset) == StandardCharsets.UTF_8) {
      sparkSession.baseRelationToDataFrame(
        DataSource.apply(
          sparkSession,
          paths = paths,
          className = classOf[TextFileFormat].getName
        ).resolveRelation(checkFilesExist = false))
        .select("value").as[String](Encoders.STRING)
    } else {
      val charset = options.charset
      val rdd = sparkSession.sparkContext
        .hadoopFile[LongWritable, Text, TextInputFormat](paths.mkString(","))
        .mapPartitions(_.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
      sparkSession.createDataset(rdd)(Encoders.STRING)
    }
  }
}

object MultiLineCSVDataSource extends CSVDataSource {
  override val isSplitable: Boolean = false

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      schema: StructType): Iterator[InternalRow] = {
    UnivocityParser.parseStream(
      CodecStreams.createInputStreamWithCloseResource(conf, file.filePath),
      parser.options.headerFlag,
      parser,
      schema)
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    val csv = createBaseRdd(sparkSession, inputPaths, parsedOptions)
    csv.flatMap { lines =>
      UnivocityParser.tokenizeStream(
        CodecStreams.createInputStreamWithCloseResource(lines.getConfiguration, lines.getPath()),
        shouldDropHeader = false,
        new CsvParser(parsedOptions.asParserSettings))
    }.take(1).headOption match {
      case Some(firstRow) =>
        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        val header = makeSafeHeader(firstRow, caseSensitive, parsedOptions)
        val tokenRDD = csv.flatMap { lines =>
          UnivocityParser.tokenizeStream(
            CodecStreams.createInputStreamWithCloseResource(
              lines.getConfiguration,
              lines.getPath()),
            parsedOptions.headerFlag,
            new CsvParser(parsedOptions.asParserSettings))
        }
        CSVInferSchema.infer(tokenRDD, header, parsedOptions)
      case None =>
        // If the first row could not be read, just return the empty schema.
        StructType(Nil)
    }
  }

  private def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: CSVOptions): RDD[PortableDataStream] = {
    val paths = inputPaths.map(_.getPath)
    val name = paths.mkString(",")
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConf())
    FileInputFormat.setInputPaths(job, paths: _*)
    val conf = job.getConfiguration

    val rdd = new BinaryFileRDD(
      sparkSession.sparkContext,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      conf,
      sparkSession.sparkContext.defaultMinPartitions)

    // Only returns `PortableDataStream`s without paths.
    rdd.setName(s"CSVFile: $name").values
  }
}

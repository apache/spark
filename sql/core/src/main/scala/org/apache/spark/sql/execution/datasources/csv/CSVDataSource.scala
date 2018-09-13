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

import java.net.URI
import java.nio.charset.{Charset, StandardCharsets}

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.internal.Logging
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
      requiredSchema: StructType,
      // Actual schema of data in the csv file
      dataSchema: StructType,
      caseSensitive: Boolean,
      columnPruning: Boolean): Iterator[InternalRow]

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

object CSVDataSource extends Logging {
  def apply(options: CSVOptions): CSVDataSource = {
    if (options.multiLine) {
      MultiLineCSVDataSource
    } else {
      TextInputCSVDataSource
    }
  }

  /**
   * Checks that column names in a CSV header and field names in the schema are the same
   * by taking into account case sensitivity.
   *
   * @param schema - provided (or inferred) schema to which CSV must conform.
   * @param columnNames - names of CSV columns that must be checked against to the schema.
   * @param fileName - name of CSV file that are currently checked. It is used in error messages.
   * @param enforceSchema - if it is `true`, column names are ignored otherwise the CSV column
   *                        names are checked for conformance to the schema. In the case if
   *                        the column name don't conform to the schema, an exception is thrown.
   * @param caseSensitive - if it is set to `false`, comparison of column names and schema field
   *                        names is not case sensitive.
   */
  def checkHeaderColumnNames(
      schema: StructType,
      columnNames: Array[String],
      fileName: String,
      enforceSchema: Boolean,
      caseSensitive: Boolean): Unit = {
    if (columnNames != null) {
      val fieldNames = schema.map(_.name).toIndexedSeq
      val (headerLen, schemaSize) = (columnNames.size, fieldNames.length)
      var errorMessage: Option[String] = None

      if (headerLen == schemaSize) {
        var i = 0
        while (errorMessage.isEmpty && i < headerLen) {
          var (nameInSchema, nameInHeader) = (fieldNames(i), columnNames(i))
          if (!caseSensitive) {
            nameInSchema = nameInSchema.toLowerCase
            nameInHeader = nameInHeader.toLowerCase
          }
          if (nameInHeader != nameInSchema) {
            errorMessage = Some(
              s"""|CSV header does not conform to the schema.
                  | Header: ${columnNames.mkString(", ")}
                  | Schema: ${fieldNames.mkString(", ")}
                  |Expected: ${fieldNames(i)} but found: ${columnNames(i)}
                  |CSV file: $fileName""".stripMargin)
          }
          i += 1
        }
      } else {
        errorMessage = Some(
          s"""|Number of column in CSV header is not equal to number of fields in the schema:
              | Header length: $headerLen, schema size: $schemaSize
              |CSV file: $fileName""".stripMargin)
      }

      errorMessage.foreach { msg =>
        if (enforceSchema) {
          logWarning(msg)
        } else {
          throw new IllegalArgumentException(msg)
        }
      }
    }
  }
}

object TextInputCSVDataSource extends CSVDataSource {
  override val isSplitable: Boolean = true

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      requiredSchema: StructType,
      dataSchema: StructType,
      caseSensitive: Boolean,
      columnPruning: Boolean): Iterator[InternalRow] = {
    val lines = {
      val linesReader = new HadoopFileLinesReader(file, conf)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }

    val hasHeader = parser.options.headerFlag && file.start == 0
    if (hasHeader) {
      // Checking that column names in the header are matched to field names of the schema.
      // The header will be removed from lines.
      // Note: if there are only comments in the first block, the header would probably
      // be not extracted.
      CSVUtils.extractHeader(lines, parser.options).foreach { header =>
        val schema = if (columnPruning) requiredSchema else dataSchema
        val columnNames = parser.tokenizer.parseLine(header)
        CSVDataSource.checkHeaderColumnNames(
          schema,
          columnNames,
          file.filePath,
          parser.options.enforceSchema,
          caseSensitive)
      }
    }

    UnivocityParser.parseIterator(lines, parser, requiredSchema)
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
      parsedOptions: CSVOptions): StructType = {
    val csvParser = new CsvParser(parsedOptions.asParserSettings)
    maybeFirstLine.map(csvParser.parseLine(_)) match {
      case Some(firstRow) if firstRow != null =>
        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        val header = makeSafeHeader(firstRow, caseSensitive, parsedOptions)
        val sampled: Dataset[String] = CSVUtils.sample(csv, parsedOptions)
        val tokenRDD = sampled.rdd.mapPartitions { iter =>
          val filteredLines = CSVUtils.filterCommentAndEmpty(iter, parsedOptions)
          val linesWithoutHeader =
            CSVUtils.filterHeaderLine(filteredLines, maybeFirstLine.get, parsedOptions)
          val parser = new CsvParser(parsedOptions.asParserSettings)
          linesWithoutHeader.map(parser.parseLine)
        }
        CSVInferSchema.infer(tokenRDD, header, parsedOptions)
      case _ =>
        // If the first line could not be read, just return the empty schema.
        StructType(Nil)
    }
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
          className = classOf[TextFileFormat].getName,
          options = options.parameters
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
      requiredSchema: StructType,
      dataSchema: StructType,
      caseSensitive: Boolean,
      columnPruning: Boolean): Iterator[InternalRow] = {
    def checkHeader(header: Array[String]): Unit = {
      val schema = if (columnPruning) requiredSchema else dataSchema
      CSVDataSource.checkHeaderColumnNames(
        schema,
        header,
        file.filePath,
        parser.options.enforceSchema,
        caseSensitive)
    }

    UnivocityParser.parseStream(
      CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath))),
      parser.options.headerFlag,
      parser,
      requiredSchema,
      checkHeader)
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    val csv = createBaseRdd(sparkSession, inputPaths, parsedOptions)
    csv.flatMap { lines =>
      val path = new Path(lines.getPath())
      UnivocityParser.tokenizeStream(
        CodecStreams.createInputStreamWithCloseResource(lines.getConfiguration, path),
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
              new Path(lines.getPath())),
            parsedOptions.headerFlag,
            new CsvParser(parsedOptions.asParserSettings))
        }
        val sampled = CSVUtils.sample(tokenRDD, parsedOptions)
        CSVInferSchema.infer(sampled, header, parsedOptions)
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
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConfWithOptions(
      options.parameters))
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

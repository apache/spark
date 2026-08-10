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
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVInferSchema
import org.apache.spark.sql.catalyst.fwf.{
  ExplicitFwfColSpecs, FixedWidthOptions, FixedWidthParser, FixedWidthUtils, InferFwfColSpecs}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.util.Utils

/**
 * Common functions for reading fixed-width files.
 */
object FixedWidthDataSource extends Logging {

  // Carries a `colspecs="infer"` result, resolved once here from sampled file content, from
  // `inferSchema` (which has file access) into `buildReader` (which only sees the already-resolved
  // `dataSchema`). Explicit colspecs/widths need no such threading.
  private val COLSPEC_FROM_KEY = "org.apache.spark.sql.fwf.from"
  private val COLSPEC_TO_KEY = "org.apache.spark.sql.fwf.to"

  def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: FixedWidthOptions): Option[StructType] = {
    if (inputPaths.isEmpty) {
      return None
    }
    val lines = readSampleLines(sparkSession, inputPaths, options)
    if (lines.isEmpty) {
      return Some(StructType(Nil))
    }

    val colspecs = options.colSpecs match {
      case ExplicitFwfColSpecs(specs) => specs
      case InferFwfColSpecs =>
        FixedWidthUtils.detectColspecs(lines.toIndexedSeq, options.comment, options.delimiter)
    }

    val tokenizedRows = lines
      .map(FixedWidthUtils.sliceLine(_, colspecs, options.delimiter))
      .filterNot(FixedWidthUtils.isLineEffectivelyEmpty)

    // A template row only needs the right *length*; when `header` is false `makeSafeHeader`
    // ignores its values and synthesizes "_c0".."_cN" anyway, so falling back to a synthetic
    // one when there's no sampled row at all still yields a correctly-sized header.
    val templateRow: Array[String] = tokenizedRows.headOption.getOrElse(
      colspecs.indices.map(i => s"_c$i").toArray)
    val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
    val header = CSVUtils.makeSafeHeader(templateRow, caseSensitive, options.toCSVOptionsShim)
    val dataRows =
      if (options.header && tokenizedRows.nonEmpty) tokenizedRows.tail else tokenizedRows

    val tokenRDD = sparkSession.sparkContext.parallelize(dataRows.toIndexedSeq)
    val inferred = new CSVInferSchema(options.toCSVOptionsShim).infer(tokenRDD, header)

    val withColspecMetadata = options.colSpecs match {
      case InferFwfColSpecs =>
        StructType(inferred.fields.zip(colspecs).map { case (field, (from, to)) =>
          val metadata = new MetadataBuilder()
            .withMetadata(field.metadata)
            .putLong(COLSPEC_FROM_KEY, from.toLong)
            .putLong(COLSPEC_TO_KEY, to.toLong)
            .build()
          field.copy(metadata = metadata)
        })
      case _: ExplicitFwfColSpecs => inferred
    }
    Some(withColspecMetadata)
  }

  /**
   * Resolves the concrete colspecs to use when actually reading data. Explicit colspecs/widths
   * are simply re-read from the static options. A `colspecs="infer"` result can only come from
   * this class's own `inferSchema`, which stamped the resolved boundaries into each field's
   * metadata (see [[COLSPEC_FROM_KEY]]); there is no file access here to infer them afresh, so
   * combining `colspecs="infer"` with a user-supplied schema (which skips `inferSchema`) is not
   * supported.
   */
  def resolveColspecsForRead(
      options: FixedWidthOptions,
      dataSchema: StructType): Seq[(Int, Int)] = options.colSpecs match {
    case ExplicitFwfColSpecs(specs) => specs
    case InferFwfColSpecs =>
      val hasMetadata = dataSchema.fields.nonEmpty && dataSchema.fields.forall { f =>
        f.metadata.contains(COLSPEC_FROM_KEY) && f.metadata.contains(COLSPEC_TO_KEY)
      }
      if (!hasMetadata) {
        throw QueryExecutionErrors.fwfColspecsInferWithUserSchemaError()
      }
      dataSchema.fields.map { f =>
        (f.metadata.getLong(COLSPEC_FROM_KEY).toInt, f.metadata.getLong(COLSPEC_TO_KEY).toInt)
      }.toSeq
  }

  private def readSampleLines(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: FixedWidthOptions): Array[String] = {
    val paths = inputPaths.map(_.getPath.toString)
    val df = sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        className = classOf[TextFileFormat].getName,
        options = Map(DataSource.GLOB_PATHS_KEY -> "false")
      ).resolveRelation(checkFilesExist = false))
      .select("value").as[String](Encoders.STRING)

    // Comment lines are dropped entirely, alongside `skipRows`, before header/data-row selection
    // -- see `FixedWidthUtils.isCommentLine`.
    if (options.skipRows.isEmpty) {
      var remaining = options.inferNrows
      val candidates = df.toLocalIterator()
      val buf = new scala.collection.mutable.ArrayBuffer[String]()
      while (remaining > 0 && candidates.hasNext) {
        val line = candidates.next()
        if (!FixedWidthUtils.isCommentLine(line, options.comment)) {
          buf += line
          remaining -= 1
        }
      }
      buf.toArray
    } else {
      // Skip the configured row numbers first, then sample `inferNrows` of what's left. Enough
      // candidate rows are pulled up front to guarantee that many survive even after the skipped
      // and comment rows are removed.
      val candidateCount =
        math.min(Int.MaxValue.toLong, options.inferNrows.toLong + options.skipRows.max + 1).toInt
      df.take(candidateCount).zipWithIndex
        .collect { case (line, idx) if !options.skipRows.contains(idx) => line }
        .filterNot(FixedWidthUtils.isCommentLine(_, options.comment))
        .take(options.inferNrows)
    }
  }

  /**
   * When fewer columns are required than exist, slices only the required colspecs, saving the
   * substring/strip work for columns nobody asked for and letting [[FixedWidthParser]] receive
   * tokens already sized and ordered to `requiredSchema` directly.
   */
  def effectiveColspecs(
      dataSchema: StructType,
      requiredSchema: StructType,
      colspecs: Seq[(Int, Int)]): Seq[(Int, Int)] = {
    if (requiredSchema.length < dataSchema.length) {
      requiredSchema.map(f => colspecs(dataSchema.indexOf(f)))
    } else {
      colspecs
    }
  }

  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      colspecs: Seq[(Int, Int)],
      parser: FixedWidthParser,
      options: FixedWidthOptions,
      requiredSchema: StructType): Iterator[InternalRow] = {
    val linesReader =
      Utils.createResourceUninterruptiblyIfInTaskThread(new HadoopFileLinesReader(file, conf))
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
    val lines = linesReader.map { line =>
      new String(line.getBytes, 0, line.getLength, options.charset)
    }

    val safeParser = new FailureSafeParser[Array[String]](
      input => parser.parse(input),
      options.parseMode,
      requiredSchema,
      options.columnNameOfCorruptRecord)

    // `skipRows` numbers rows from the start of the file, and the header (if any) is whichever
    // row remains first after skipping. `lineIndex` is each line's absolute position in the file
    // because `isSplitable` forces a whole-file partition whenever `skipRows` is non-empty.
    var lineIndex = -1
    var headerPending = file.start == 0 && options.header
    lines.flatMap { line =>
      lineIndex += 1
      if (options.skipRows.contains(lineIndex) ||
        FixedWidthUtils.isCommentLine(line, options.comment)) {
        Iterator.empty
      } else if (headerPending) {
        headerPending = false
        Iterator.empty
      } else if (FixedWidthUtils.isLineBlank(line, options.delimiter)) {
        Iterator.empty
      } else {
        val tokens = FixedWidthUtils.sliceLine(line, colspecs, options.delimiter)
        safeParser.parse(tokens)
      }
    }
  }
}

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

import java.io.{FileNotFoundException, InputStream, IOException}
import java.nio.charset.{Charset, StandardCharsets}
import java.util.regex.Pattern

import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.paths.SparkPath
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVInferSchema, CSVOptions, UnivocityParser}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Common functions for parsing CSV files
 */
abstract class CSVDataSource extends Serializable with Logging {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into [[InternalRow]] instances.
   */
  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      headerChecker: CSVHeaderChecker,
      requiredSchema: StructType): Iterator[InternalRow]

  /**
   * Infers the schema from `inputPaths` files.
   */
  final def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions,
      supportsArchiveScan: Boolean): Option[StructType] = {
    parsedOptions.singleVariantColumn match {
      case Some(columnName) => Some(StructType(Array(StructField(columnName, VariantType))))
      case None =>
        val hasArchive = parsedOptions.archiveFormatEnabled &&
          inputPaths.exists(f => ArchiveReader.isArchivePath(f.getPath))
        if (hasArchive && supportsArchiveScan) {
          // Archives (and any loose files alongside them) are inferred in a single CSVInferSchema
          // pass over all inputs -- archive entries are streamed, never unpacked -- so the result
          // matches what the scan returns for the same files.
          Some(inferWithArchives(sparkSession, inputPaths, parsedOptions))
        } else if (hasArchive) {
          // The caller's scan path cannot read archives (e.g. the DSv2 reader), so refuse to infer
          // a schema when any input is an archive: returning None raises UNABLE_TO_INFER_SCHEMA,
          // which fails loudly instead of letting the scan parse raw archive bytes as CSV.
          None
        } else if (inputPaths.nonEmpty) {
          Some(infer(sparkSession, inputPaths, parsedOptions))
        } else {
          None
        }
    }
  }

  protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType

  /**
   * Tokenizes one input's bytes -- a whole loose file or a single decompressed archive entry --
   * into CSV rows the same way this mode's scan reads it: line by line for
   * [[TextInputCSVDataSource]] (so a quoted field with an embedded newline splits across rows
   * exactly as the read does) and as one continuous stream for [[MultiLineCSVDataSource]]. This
   * input's own first row (its header) is dropped when `dropHeader` is set. Used only by
   * [[inferWithArchives]]; the stream is not closed here.
   */
  protected def tokenizeForInference(
      in: InputStream,
      dropHeader: Boolean,
      options: CSVOptions): Iterator[Array[String]]

  /**
   * Streams a tar archive (`.tar`/`.tar.gz`/`.tgz`) entry by entry through the CSV parser without
   * unpacking it to disk. The whole archive is a single split (see `CSVFileFormat.isSplitable`); a
   * fresh header checker and parser are built per entry so each entry is parsed exactly like a
   * standalone CSV file -- its header, if any, validated and dropped independently. The
   * mode-specific implementation turns one entry into rows via `parseStream` / `parseIterator`.
   *
   * @param getParser builds a fresh [[UnivocityParser]].
   * @param getHeaderChecker builds a fresh [[CSVHeaderChecker]] for `(isStartOfFile, source)`.
   * @param ignoredPathSegmentRegex the compiled effective `ignoredPathSegmentRegex` option, so
   *                           hidden entries are skipped exactly like Spark's file listing would.
   */
  def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      getParser: () => UnivocityParser,
      getHeaderChecker: (Boolean, String) => CSVHeaderChecker,
      requiredSchema: StructType,
      ignoredPathSegmentRegex: Pattern): Iterator[InternalRow]

  /**
   * Shared driver used by the [[readArchive]] implementations: streams each non-skipped entry's
   * `(parser, headerChecker, stream)` -- a fresh parser/header checker per entry -- through
   * `parseEntry`. The header checker `source` (`CSV archive entry: <archive>!/<entryName>`) names
   * the entry in error messages.
   */
  protected def streamArchiveEntries(
      conf: Configuration,
      file: PartitionedFile,
      getParser: () => UnivocityParser,
      getHeaderChecker: (Boolean, String) => CSVHeaderChecker,
      ignoredPathSegmentRegex: Pattern)(
      parseEntry: (UnivocityParser, CSVHeaderChecker, InputStream) => Iterator[InternalRow])
    : Iterator[InternalRow] = {
    ArchiveReader(file.toPath).readEntries(conf, ignoredPathSegmentRegex) { (entryName, in) =>
      val headerChecker =
        getHeaderChecker(true, s"CSV archive entry: ${file.urlEncodedPath}!/$entryName")
      val parser = getParser()
      headerChecker.setHeaderForSingleVariantColumn =
        CSVDataSource.setHeaderForSingleVariantColumn(conf, file, parser)
      parseEntry(parser, headerChecker, in)
    }
  }

  /**
   * Infers a CSV schema when at least one input is a tar archive. Every archive entry is streamed
   * (never unpacked to disk) and every loose file is read, each tokenized the same way this mode's
   * scan reads it (see [[tokenizeForInference]]), and all of them feed a single [[CSVInferSchema]]
   * pass keyed on the first input's header. The column count is fixed by the first header and
   * `NullType` columns survive to the final `toStructFields`, so the inferred schema matches what
   * the scan returns for the same inputs. A corrupt/missing input is skipped as a unit (a whole
   * archive or a whole file) when `ignoreCorruptFiles`/`ignoreMissingFiles` are set. Both the
   * header and sampling passes honor those flags, so archive inference is strictly more forgiving
   * here than [[MultiLineCSVDataSource.infer]], whose sampling pass has no such guard.
   */
  private def inferWithArchives(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    val baseRdd = CSVDataSource.createBaseRdd(sparkSession, inputPaths, parsedOptions)
    def tokens(dropHeader: Boolean): RDD[Array[String]] = baseRdd.flatMap { stream =>
      val path = new Path(stream.getPath())
      try {
        if (ArchiveReader.isArchivePath(path)) {
          ArchiveReader(path).readEntries(stream.getConfiguration) { (_, in) =>
            tokenizeForInference(in, dropHeader, parsedOptions)
          }
        } else {
          tokenizeForInference(
            CodecStreams.createInputStreamWithCloseResource(stream.getConfiguration, path),
            dropHeader, parsedOptions)
        }
      } catch {
        case e: FileNotFoundException if parsedOptions.ignoreMissingFiles =>
          logWarning(log"Skipped missing input: ${MDC(PATH, stream.getPath())}", e)
          Iterator.empty
        case e: FileNotFoundException => throw e
        case e @ (_: RuntimeException | _: IOException) if parsedOptions.ignoreCorruptFiles =>
          logWarning(log"Skipped the corrupted input: ${MDC(PATH, stream.getPath())}", e)
          Iterator.empty
        case NonFatal(e) =>
          throw QueryExecutionErrors.cannotReadFilesError(
            e, SparkPath.fromPathString(stream.getPath()).urlEncoded)
      }
    }
    tokens(dropHeader = false).take(1).headOption match {
      case Some(firstRow) =>
        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        val header = CSVUtils.makeSafeHeader(firstRow, caseSensitive, parsedOptions)
        val sampled = CSVUtils.sample(tokens(dropHeader = parsedOptions.headerFlag), parsedOptions)
        SQLExecution.withSQLConfPropagated(sparkSession) {
          new CSVInferSchema(parsedOptions).infer(sampled, header)
        }
      case None =>
        StructType(Nil)
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
   * One `PortableDataStream` per input file (the whole file, never split), shared by the multiLine
   * schema-inference path and the archive inference path.
   */
  private[csv] def createBaseRdd(
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

  /**
   * Returns a function that sets the header column names used in singleVariantColumn mode. The
   * returned function takes an optional input, which is the header column names potentially read by
   * `CSVHeaderChecker`. The function only needs to read the file when the input is empty (e.g.,
   * `CSVHeaderChecker` won't read anything when the partition is not at the file start).
   *
   * We need to return a function here instead of letting `CSVHeaderChecker` call this function
   * directly, because this package (also the `CSVUtils` class) depends on `CSVHeaderChecker`.
   */
  def setHeaderForSingleVariantColumn(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser): Option[Option[Array[String]] => Unit] =
    if (parser.options.needHeaderForSingleVariantColumn) {
      Some(headerColumnNames => {
        parser.headerColumnNames = headerColumnNames.orElse {
          CSVUtils.readHeaderLine(file.toPath, parser.options, conf).map { line =>
            new CsvParser(parser.options.asParserSettings).parseLine(line)
          }
        }
      })
    } else {
      None
    }
}

object TextInputCSVDataSource extends CSVDataSource {
  override val isSplitable: Boolean = true

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      headerChecker: CSVHeaderChecker,
      requiredSchema: StructType): Iterator[InternalRow] = {
    val lines = {
      val linesReader = Utils.createResourceUninterruptiblyIfInTaskThread(
        new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
      )
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }

    headerChecker.setHeaderForSingleVariantColumn =
      CSVDataSource.setHeaderForSingleVariantColumn(conf, file, parser)
    UnivocityParser.parseIterator(lines, parser, headerChecker, requiredSchema)
  }

  override def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      getParser: () => UnivocityParser,
      getHeaderChecker: (Boolean, String) => CSVHeaderChecker,
      requiredSchema: StructType,
      ignoredPathSegmentRegex: Pattern): Iterator[InternalRow] =
    // Stream each tar entry through the line-based parser, treating the entry exactly like a
    // standalone CSV file (a fresh parser/header checker is built per entry).
    streamArchiveEntries(conf, file, getParser, getHeaderChecker, ignoredPathSegmentRegex) {
      (parser, headerChecker, in) =>
        UnivocityParser.parseIterator(
          entryLines(in, parser.options), parser, headerChecker, requiredSchema)
    }

  /**
   * Decodes one archive entry's bytes into the same CSV line strings the non-archive [[readFile]]
   * path feeds to the parser: [[ArchiveReader.lineIterator]] splits the entry into lines (honoring
   * a custom line separator) and each line is decoded with the configured charset. Like `readFile`,
   * the decoded lines are fed to `UnivocityParser.parseIterator` without a re-appended terminator.
   *
   * @param in bytes of one already-decompressed archive entry; not closed here (the archive owns
   *           the underlying stream).
   * @param options CSV options supplying the read line separator and charset.
   * @return an iterator over the entry's lines.
   */
  private def entryLines(in: InputStream, options: CSVOptions): Iterator[String] = {
    ArchiveReader.lineIterator(in, options.lineSeparatorInRead).map { line =>
      new String(line.getBytes, 0, line.getLength, options.charset)
    }
  }

  override protected def tokenizeForInference(
      in: InputStream,
      dropHeader: Boolean,
      options: CSVOptions): Iterator[Array[String]] = {
    // Match the line-based scan (`readArchive` -> `entryLines` -> `parseIterator`): split into
    // lines, drop comments/blanks, drop this input's header line, then parse each line on its own,
    // so a quoted field with an embedded newline splits across rows exactly as the read does.
    val lines = CSVUtils.filterCommentAndEmpty(entryLines(in, options), options)
    val rows = if (dropHeader && lines.hasNext) { lines.next(); lines } else lines
    val parser = new CsvParser(options.asParserSettings)
    rows.map(parser.parseLine)
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
    maybeFirstLine.map(UnivocityParser.parseLine(csvParser, _)) match {
      case Some(firstRow) if firstRow != null =>
        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        val header = CSVUtils.makeSafeHeader(firstRow, caseSensitive, parsedOptions)
        val sampled: Dataset[String] = CSVUtils.sample(csv, parsedOptions)
        val tokenRDD = sampled.rdd.mapPartitions { iter =>
          val filteredLines = CSVUtils.filterCommentAndEmpty(iter, parsedOptions)
          val linesWithoutHeader =
            CSVUtils.filterHeaderLine(filteredLines, maybeFirstLine.get, parsedOptions)
          val parser = new CsvParser(parsedOptions.asParserSettings)
          linesWithoutHeader.map(UnivocityParser.parseLine(parser, _))
        }
        SQLExecution.withSQLConfPropagated(csv.sparkSession) {
          new CSVInferSchema(parsedOptions).infer(tokenRDD, header)
        }
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
    val df = sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        className = classOf[TextFileFormat].getName,
        options = options.parameters ++ Map(DataSource.GLOB_PATHS_KEY -> "false")
      ).resolveRelation(checkFilesExist = false))
      .select("value").as[String](Encoders.STRING)

    if (Charset.forName(options.charset) == StandardCharsets.UTF_8) {
      df
    } else {
      val charset = options.charset
      sparkSession.createDataset(df.queryExecution.toRdd.map { row =>
        val bytes = row.getBinary(0)
        new String(bytes, 0, bytes.length, charset)
      })(Encoders.STRING)
    }
  }
}

object MultiLineCSVDataSource extends CSVDataSource {
  override val isSplitable: Boolean = false

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      headerChecker: CSVHeaderChecker,
      requiredSchema: StructType): Iterator[InternalRow] = {
    headerChecker.setHeaderForSingleVariantColumn =
      CSVDataSource.setHeaderForSingleVariantColumn(conf, file, parser)
    UnivocityParser.parseStream(
      CodecStreams.createInputStreamWithCloseResource(conf, file.toPath),
      parser,
      headerChecker,
      requiredSchema)
  }

  override def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      getParser: () => UnivocityParser,
      getHeaderChecker: (Boolean, String) => CSVHeaderChecker,
      requiredSchema: StructType,
      ignoredPathSegmentRegex: Pattern): Iterator[InternalRow] =
    // Stream each tar entry whole through the multi-line parser (a fresh parser/header checker is
    // built per entry).
    streamArchiveEntries(conf, file, getParser, getHeaderChecker, ignoredPathSegmentRegex) {
      (parser, headerChecker, in) =>
        UnivocityParser.parseStream(in, parser, headerChecker, requiredSchema)
    }

  override protected def tokenizeForInference(
      in: InputStream,
      dropHeader: Boolean,
      options: CSVOptions): Iterator[Array[String]] =
    // The whole input is one continuous stream, exactly as the multi-line scan reads it.
    UnivocityParser.tokenizeStream(
      in, dropHeader, new CsvParser(options.asParserSettings), options.charset)

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    val csv = CSVDataSource.createBaseRdd(sparkSession, inputPaths, parsedOptions)
    val ignoreCorruptFiles = parsedOptions.ignoreCorruptFiles
    val ignoreMissingFiles = parsedOptions.ignoreMissingFiles
    csv.flatMap { lines =>
      try {
        val path = new Path(lines.getPath())
        UnivocityParser.tokenizeStream(
          CodecStreams.createInputStreamWithCloseResource(lines.getConfiguration, path),
          shouldDropHeader = false,
          new CsvParser(parsedOptions.asParserSettings),
          encoding = parsedOptions.charset)
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(log"Skipped missing file: ${MDC(PATH, lines.getPath())}", e)
          Array.empty[Array[String]]
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(log"Skipped the rest of the content in the corrupted file: " +
            log"${MDC(PATH, lines.getPath())}", e)
          Array.empty[Array[String]]
        case NonFatal(e) =>
          val path = SparkPath.fromPathString(lines.getPath())
          throw QueryExecutionErrors.cannotReadFilesError(e, path.urlEncoded)
      }
    }.take(1).headOption match {
      case Some(firstRow) =>
        val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
        val header = CSVUtils.makeSafeHeader(firstRow, caseSensitive, parsedOptions)
        val tokenRDD = csv.flatMap { lines =>
          UnivocityParser.tokenizeStream(
            CodecStreams.createInputStreamWithCloseResource(
              lines.getConfiguration,
              new Path(lines.getPath())),
            parsedOptions.headerFlag,
            new CsvParser(parsedOptions.asParserSettings),
            encoding = parsedOptions.charset)
        }
        val sampled = CSVUtils.sample(tokenRDD, parsedOptions)
        SQLExecution.withSQLConfPropagated(sparkSession) {
          new CSVInferSchema(parsedOptions).infer(sampled, header)
        }
      case None =>
        // If the first row could not be read, just return the empty schema.
        StructType(Nil)
    }
  }
}

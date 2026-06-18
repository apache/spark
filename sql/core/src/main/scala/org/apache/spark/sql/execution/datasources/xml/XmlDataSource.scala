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

package org.apache.spark.sql.execution.datasources.xml

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream, IOException}
import java.nio.charset.{Charset, StandardCharsets}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, StaxXMLRecordReader, XmlInferSchema, XmlOptions}
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.{StructField, StructType, VariantType}
import org.apache.spark.util.Utils

/**
 * Common functions for parsing XML files
 */
abstract class XmlDataSource extends Serializable with Logging {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into [[InternalRow]] instances.
   */
  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: StaxXmlParser,
      schema: StructType): Iterator[InternalRow]

  /**
   * Streams a tar archive (`.tar`/`.tar.gz`/`.tgz`) entry by entry through the XML parser without
   * unpacking it to disk. The whole archive is a single split (see `XmlFileFormat.isSplitable`);
   * each entry's bytes are parsed exactly like a standalone XML file. Single-line and multi-line
   * parse an entry's bytes differently (mirroring [[readFile]]), so each data source overrides it.
   *
   * Kept separate from [[readFile]] (rather than dispatched inside it) because only the V1
   * `XmlFileFormat` read path supports archives; XML has no DSv2 reader.
   *
   * @param parser builds a fresh XML parser for each entry, so every entry is parsed with its own
   *               parser instance -- matching the per-file parser of a non-archive read.
   */
  def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      parser: () => StaxXmlParser,
      schema: StructType): Iterator[InternalRow]

  /**
   * Infers the schema from `inputPaths` files.
   */
  final def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: XmlOptions): Option[StructType] = {
    parsedOptions.singleVariantColumn match {
      case Some(columnName) => Some(StructType(Array(StructField(columnName, VariantType))))
      case None =>
        // When any input is a tar archive, infer over all inputs in a single pass -- archive
        // entries are streamed (never unpacked to disk) and tokenized as XML records alongside any
        // loose files -- so the result matches a directory read of the same files. XML has no DSv2
        // reader, so this archive scan is always V1.
        val hasArchive = parsedOptions.archiveFormatEnabled &&
          inputPaths.exists(f => ArchiveReader.isArchivePath(f.getPath))
        if (hasArchive) {
          Some(inferWithArchives(sparkSession, inputPaths, parsedOptions))
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
      parsedOptions: XmlOptions): StructType

  /**
   * Infers an XML schema when at least one input is a tar archive (`.tar`/`.tar.gz`/`.tgz`). Every
   * archive entry (streamed through `ArchiveReader`, never unpacked to disk) and every loose file
   * is tokenized into records and fed to a single [[XmlInferSchema]] pass, exactly as a directory
   * of the same files would infer. Tokenization is per-mode so it matches this mode's scan:
   * multi-line splits the whole stream into `rowTag`-delimited records, single-line treats each
   * line as a record (mirroring [[readFile]] and JSON's `inferWithArchives`).
   */
  private def inferWithArchives(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: XmlOptions): StructType = {
    val baseRdd = createBaseRdd(sparkSession, inputPaths, parsedOptions)
    val ignoreCorruptFiles = parsedOptions.ignoreCorruptFiles
    val ignoreMissingFiles = parsedOptions.ignoreMissingFiles

    // Applies `perEntry` to each input -- an archive entry by entry (streamed, so only one entry's
    // bytes are in flight at a time), a loose file directly -- skipping a whole input when it is
    // corrupt/missing and the ignore flags are set.
    def perInput(perEntry: InputStream => Iterator[String]): RDD[String] = baseRdd.flatMap {
      stream =>
        val path = new Path(stream.getPath())
        try {
          if (ArchiveReader.isArchivePath(path)) {
            ArchiveReader(path).readEntries(stream.getConfiguration) { (_, in) => perEntry(in) }
          } else {
            perEntry(
              CodecStreams.createInputStreamWithCloseResource(stream.getConfiguration, path))
          }
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning("Skipped missing file", e)
            Iterator.empty[String]
          case NonFatal(e) =>
            Utils.getRootCause(e) match {
              case root @ (_: AccessControlException | _: BlockMissingException) => throw root
              case _: RuntimeException | _: IOException if ignoreCorruptFiles =>
                logWarning("Skipped the rest of the content in the corrupted file", e)
                Iterator.empty[String]
              case other => throw other
            }
        }
    }

    // Tokenize each input the way this mode's scan reads records, so the inferred schema matches a
    // directory read: multi-line splits the whole stream into rowTag-delimited records, single-line
    // treats each line as a record (mirroring TextInputXmlDataSource.readFile).
    val tokenRDD: RDD[String] = if (parsedOptions.multiLine) {
      perInput(in => StaxXmlParser.tokenizeStream(in, parsedOptions))
    } else {
      val charset = parsedOptions.charset
      perInput(in => ArchiveReader.lineIterator(in, None).map { line =>
        new String(line.getBytes, 0, line.getLength, charset)
      })
    }
    SQLExecution.withSQLConfPropagated(sparkSession) {
      new XmlInferSchema(parsedOptions, sparkSession.sessionState.conf.caseSensitiveAnalysis)
        .infer(tokenRDD)
    }
  }

  protected def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: XmlOptions): RDD[PortableDataStream] = {
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
    rdd.setName(s"XMLFile: $name").values
  }
}

object XmlDataSource extends Logging {
  def apply(options: XmlOptions): XmlDataSource = {
    if (options.multiLine) {
      MultiLineXmlDataSource
    } else {
      TextInputXmlDataSource
    }
  }
}

object TextInputXmlDataSource extends XmlDataSource {
  override val isSplitable: Boolean = true

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: StaxXmlParser,
      schema: StructType): Iterator[InternalRow] = {
    val lines = {
      val linesReader = Utils.createResourceUninterruptiblyIfInTaskThread(
        new HadoopFileLinesReader(file, None, conf)
      )
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }

    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    lines.flatMap(safeParser.parse)
  }

  /**
   * Mirrors [[readFile]] for archive entries: split each entry into lines and run each line through
   * a [[FailureSafeParser]], so a single-line archive entry gets the same per-record corrupt-record
   * handling as a non-archive single-line read. (Whole-stream parsing, as the multi-line override
   * uses, would bypass that handling for single-line input.)
   */
  override def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      parser: () => StaxXmlParser,
      schema: StructType): Iterator[InternalRow] =
    ArchiveReader(file.toPath).readEntries(conf) { (_, in) =>
      val entryParser = parser()
      val lines = ArchiveReader.lineIterator(in, None).map { line =>
        new String(line.getBytes, 0, line.getLength, entryParser.options.charset)
      }
      val safeParser = new FailureSafeParser[String](
        input => entryParser.parse(input),
        entryParser.options.parseMode,
        schema,
        entryParser.options.columnNameOfCorruptRecord)
      lines.flatMap(safeParser.parse)
    }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: XmlOptions): StructType = {
    val xml = createBaseDataset(sparkSession, inputPaths, parsedOptions)
    inferFromDataset(xml, parsedOptions)
  }

  /**
   * Infers the schema from `Dataset` that stores CSV string records.
   */
  def inferFromDataset(
      xml: Dataset[String],
      parsedOptions: XmlOptions): StructType = {
    SQLExecution.withSQLConfPropagated(xml.sparkSession) {
      new XmlInferSchema(parsedOptions, xml.sparkSession.sessionState.conf.caseSensitiveAnalysis)
        .infer(xml.rdd)
    }
  }

  private def createBaseDataset(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: XmlOptions): Dataset[String] = {
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

object MultiLineXmlDataSource extends XmlDataSource {
  override val isSplitable: Boolean = false

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: StaxXmlParser,
      requiredSchema: StructType): Iterator[InternalRow] = {
    if (parser.options.useLegacyXMLParser) {
      parser.parseStream(
        CodecStreams.createInputStreamWithCloseResource(conf, file.toPath),
        requiredSchema)
    } else {
      parser.parseStreamOptimized(
        () => CodecStreams.createInputStreamWithCloseResource(conf, file.toPath),
        requiredSchema)
    }
  }

  /**
   * Parses each archive entry as a single XML document, mirroring [[readFile]]: the optimized
   * parser re-reads its input (to echo the corrupt-record text on a parse failure), which a
   * single-use entry stream cannot do, so the entry's bytes are buffered and re-opened over; the
   * legacy parser reads the entry stream directly. Buffering one whole entry in memory is an
   * intended trade-off here -- the optimized parser requires a re-readable input, so a single very
   * large XML document packed in an archive is materialized in full (a non-archive read streams
   * from and re-opens the file instead). Entries are still read one at a time, so archive size
   * itself stays bounded.
   */
  override def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      parser: () => StaxXmlParser,
      schema: StructType): Iterator[InternalRow] =
    ArchiveReader(file.toPath).readEntries(conf) { (_, in) =>
      val entryParser = parser()
      if (entryParser.options.useLegacyXMLParser) {
        entryParser.parseStream(in, schema)
      } else {
        val bytes = in.readAllBytes()
        entryParser.parseStreamOptimized(() => new ByteArrayInputStream(bytes), schema)
      }
    }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: XmlOptions): StructType = {

    if (!parsedOptions.useLegacyXMLParser) {
      return inferOptimized(sparkSession, inputPaths, parsedOptions)
    }

    val xml = createBaseRdd(sparkSession, inputPaths, parsedOptions)

    val tokenRDD: RDD[String] =
      xml.flatMap { portableDataStream =>
        try {
          StaxXmlParser.tokenizeStream(
            CodecStreams.createInputStreamWithCloseResource(
              portableDataStream.getConfiguration,
              new Path(portableDataStream.getPath())),
            parsedOptions)
        } catch {
          case e: FileNotFoundException if parsedOptions.ignoreMissingFiles =>
            logWarning("Skipped missing file", e)
            Iterator.empty[String]
          case NonFatal(e) =>
            Utils.getRootCause(e) match {
              case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
              case _: RuntimeException | _: IOException if parsedOptions.ignoreCorruptFiles =>
                logWarning("Skipped the rest of the content in the corrupted file", e)
                Iterator.empty[String]
              case o => throw o
            }
        }
      }
    SQLExecution.withSQLConfPropagated(sparkSession) {
      val schema =
        new XmlInferSchema(parsedOptions, sparkSession.sessionState.conf.caseSensitiveAnalysis)
          .infer(tokenRDD)
      schema
    }
  }

  private def inferOptimized(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: XmlOptions): StructType = {

    val xml = createBaseRdd(sparkSession, inputPaths, parsedOptions)

    val xmlParserRdd: RDD[StaxXMLRecordReader] =
      xml.flatMap { portableDataStream =>
        val inputStream = () =>
          CodecStreams.createInputStreamWithCloseResource(
            portableDataStream.getConfiguration,
            new Path(portableDataStream.getPath())
          )
        StaxXmlParser.convertStream(inputStream, parsedOptions)(identity)
      }

    SQLExecution.withSQLConfPropagated(sparkSession) {
      val schema =
        new XmlInferSchema(parsedOptions, sparkSession.sessionState.conf.caseSensitiveAnalysis)
          .inferFromReaders(xmlParserRdd)
      schema
    }
  }
}

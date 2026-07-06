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

package org.apache.spark.sql.execution.datasources.json

import java.io.{ByteArrayInputStream, FileNotFoundException, InputStream, IOException}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Text
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
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JsonInferSchema, JSONOptions}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Common functions for parsing JSON files
 */
abstract class JsonDataSource extends Serializable with Logging {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into 0 or more [[InternalRow]] instances
   */
  def readFile(
    conf: Configuration,
    file: PartitionedFile,
    parser: JacksonParser,
    schema: StructType): Iterator[InternalRow]

  /**
   * Parse a single already-open [[InputStream]] -- one decompressed archive entry -- into 0 or more
   * [[InternalRow]] instances, the same way this mode reads a standalone file: line by line for
   * [[TextInputJsonDataSource]], as one whole document for [[MultiLineJsonDataSource]]. Used only
   * by [[readArchive]]; the stream is not closed here.
   */
  protected def readStream(
    in: InputStream,
    parser: JacksonParser,
    schema: StructType): Iterator[InternalRow]

  /**
   * Streams a tar archive (`.tar`/`.tar.gz`/`.tgz`) entry by entry through the JSON parser without
   * unpacking it to disk. The whole archive is a single split (see `JsonFileFormat.isSplitable`);
   * each entry's bytes are parsed exactly like a standalone JSON file via [[readStream]], so this
   * is mode-agnostic (line-delimited and multi-line both flow through `readStream`). Each entry is
   * parsed with its own parser -- matching the per-file parser of a non-archive read -- and unlike
   * CSV there is no per-entry header to rebuild. Kept apart from [[readFile]] because only the V1
   * `JsonFileFormat` read path supports archives; the V2 data source calls [[readFile]] directly
   * and is intentionally left untouched.
   *
   * @param parser builds a fresh JSON parser for each entry.
   */
  def readArchive(
      conf: Configuration,
      file: PartitionedFile,
      parser: () => JacksonParser,
      schema: StructType): Iterator[InternalRow] =
    ArchiveReader(file.toPath).readEntries(conf) { (_, in) =>
      readStream(in, parser(), schema)
    }

  final def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions,
      supportsArchiveScan: Boolean): Option[StructType] = {
    parsedOptions.singleVariantColumn.orElse(parsedOptions.explodeEmbeddedArray) match {
      case Some(columnName) => Some(StructType(Array(StructField(columnName, VariantType))))
      case None =>
        val hasArchive = parsedOptions.archiveFormatEnabled &&
          inputPaths.exists(f => ArchiveReader.isArchivePath(f.getPath))
        if (hasArchive && supportsArchiveScan) {
          // Archives (and any loose files alongside them) are inferred in a single JsonInferSchema
          // pass over all inputs -- archive entries are streamed, never unpacked -- so the result
          // matches what the scan returns for the same files.
          Some(inferWithArchives(sparkSession, inputPaths, parsedOptions))
        } else if (hasArchive) {
          // The caller's scan path cannot read archives (e.g. the DSv2 reader), so refuse to infer
          // a schema when any input is an archive: returning None raises UNABLE_TO_INFER_SCHEMA,
          // which fails loudly instead of letting the scan parse raw archive bytes as JSON.
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
      parsedOptions: JSONOptions): StructType

  /**
   * Infers a JSON schema when at least one input is a tar archive. Every archive entry (streamed
   * via `ArchiveReader`, never unpacked to disk) and every loose file is read as JSON records --
   * each line is a record, or the whole input is one document in multi-line mode -- and all of them
   * feed a single [[JsonInferSchema]] pass, exactly as a directory of the same files would infer.
   * Because [[JsonInferSchema]] already merges every record's type by field name across all inputs,
   * one pass is itself the union: a field empty in one input but typed in another widens to the
   * real type, and a `NullType` field survives to the single final canonicalization rather than
   * being collapsed per-input. A corrupt/missing input is skipped as a unit (a whole archive or a
   * whole file) when `ignoreCorruptFiles`/`ignoreMissingFiles` are set.
   */
  private def inferWithArchives(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): StructType = {
    val baseRdd = JsonDataSource.createBaseRdd(sparkSession, inputPaths, parsedOptions)
    val multiLine = parsedOptions.multiLine
    val lineSeparator = parsedOptions.lineSeparatorInRead
    val encoding = parsedOptions.encoding
    val ignoreCorruptFiles = parsedOptions.ignoreCorruptFiles
    val ignoreMissingFiles = parsedOptions.ignoreMissingFiles

    // Applies `perEntry` to each input -- once per archive entry, once for a loose file -- skipping
    // a whole input when it is corrupt/missing and the ignore flags are set. The entry/file stream
    // is consumed lazily by `perEntry`, never buffered whole; mirrors CSV's `inferWithArchives`.
    //
    // An archive entry's stream is a `CloseShieldInputStream` view over the one shared
    // `TarArchiveInputStream` cursor, valid only until `readEntries` advances to the next entry
    // (`getNextEntry` skips the prior entry's unread bytes). A consumer that emits the raw stream
    // (the multiLine path below) must therefore fully consume each element before the iterator
    // advances; `JsonInferSchema.infer` does, parsing each record before pulling the next.
    // Buffering, look-ahead, or parallelizing the per-partition consumption would read from an
    // advanced cursor and infer a wrong schema. The line-delimited path sidesteps this by
    // materializing each record (`copyBytes()`).
    def perInput[T: ClassTag](perEntry: InputStream => Iterator[T]): RDD[T] = baseRdd.flatMap {
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
            logWarning(log"Skipped missing input: ${MDC(PATH, stream.getPath())}", e)
            Iterator.empty
          case e: FileNotFoundException => throw e
          case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
            logWarning(log"Skipped the corrupted input: ${MDC(PATH, stream.getPath())}", e)
            Iterator.empty
          case NonFatal(e) =>
            throw QueryExecutionErrors.cannotReadFilesError(
              e, SparkPath.fromPathString(stream.getPath()).urlEncoded)
        }
    }

    SQLExecution.withSQLConfPropagated(sparkSession) {
      val inferSchema = new JsonInferSchema(parsedOptions)
      if (multiLine) {
        // Each input/entry is one JSON document: hand its stream straight to the parser
        // (`CreateJacksonParser.inputStream`, matching MultiLineJsonDataSource and its charset
        // auto-detect) so the document is parsed incrementally rather than buffered.
        val docs = perInput(in => Iterator.single(in))
        val docParser: (JsonFactory, InputStream) => JsonParser = encoding
          .map(enc => CreateJacksonParser.inputStream(enc, _: JsonFactory, _: InputStream))
          .getOrElse(CreateJacksonParser.inputStream(_: JsonFactory, _: InputStream))
        inferSchema.infer[InputStream](
          JsonUtils.sample(docs, parsedOptions), docParser, isReadFile = true)
      } else {
        // Line-delimited: each line is a record, copied off the reused line buffer and parsed from
        // its bytes (`CreateJacksonParser.bytes`, matching TextInputJsonDataSource).
        val lines = perInput(in => ArchiveReader.lineIterator(in, lineSeparator).map(_.copyBytes()))
        val lineParser: (JsonFactory, Array[Byte]) => JsonParser = encoding
          .map(enc => CreateJacksonParser.bytes(enc, _: JsonFactory, _: Array[Byte]))
          .getOrElse(CreateJacksonParser.bytes(_: JsonFactory, _: Array[Byte]))
        inferSchema.infer[Array[Byte]](
          JsonUtils.sample(lines, parsedOptions), lineParser, isReadFile = false)
      }
    }
  }
}

object JsonDataSource {
  def apply(options: JSONOptions): JsonDataSource = {
    if (options.explodeEmbeddedArray.isDefined) {
      EmbeddedArrayJsonDataSource
    } else if (options.multiLine) {
      MultiLineJsonDataSource
    } else {
      TextInputJsonDataSource
    }
  }

  /**
   * One `PortableDataStream` per input file (the whole file, never split), shared by the multiLine
   * schema-inference path and the archive inference path.
   */
  private[json] def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): RDD[PortableDataStream] = {
    val paths = inputPaths.map(_.getPath)
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConfWithOptions(
      parsedOptions.parameters))
    val conf = job.getConfiguration
    val name = paths.mkString(",")
    FileInputFormat.setInputPaths(job, paths: _*)
    new BinaryFileRDD(
      sparkSession.sparkContext,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      conf,
      sparkSession.sparkContext.defaultMinPartitions)
      .setName(s"JsonFile: $name")
      .values
  }
}

object TextInputJsonDataSource extends JsonDataSource {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    true
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): StructType = {
    val json: Dataset[String] = createBaseDataset(sparkSession, inputPaths, parsedOptions)

    inferFromDataset(json, parsedOptions)
  }

  def inferFromDataset(json: Dataset[String], parsedOptions: JSONOptions): StructType = {
    val sampled: Dataset[String] = JsonUtils.sample(json, parsedOptions)
    val rdd: RDD[InternalRow] = sampled.queryExecution.toRdd
    val rowParser = parsedOptions.encoding.map { enc =>
      CreateJacksonParser.internalRow(enc, _: JsonFactory, _: InternalRow)
    }.getOrElse(CreateJacksonParser.internalRow(_: JsonFactory, _: InternalRow))

    SQLExecution.withSQLConfPropagated(json.sparkSession) {
      new JsonInferSchema(parsedOptions).infer(rdd, rowParser)
    }
  }

  private def createBaseDataset(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): Dataset[String] = {
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = inputPaths.map(_.getPath.toString),
        className = classOf[TextFileFormat].getName,
        options = parsedOptions.parameters ++ Map(DataSource.GLOB_PATHS_KEY -> "false")
      ).resolveRelation(checkFilesExist = false))
      .select("value").as(Encoders.STRING)
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    val linesReader = Utils.createResourceUninterruptiblyIfInTaskThread(
      new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
    )
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
    val textParser = parser.options.encoding
      .map(enc => CreateJacksonParser.text(enc, _: JsonFactory, _: Text))
      .getOrElse(CreateJacksonParser.text(_: JsonFactory, _: Text))

    val safeParser = new FailureSafeParser[Text](
      input => parser.parse(input, textParser, textToUTF8String),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    linesReader.flatMap(safeParser.parse)
  }

  override protected def readStream(
      in: InputStream,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    val textParser = parser.options.encoding
      .map(enc => CreateJacksonParser.text(enc, _: JsonFactory, _: Text))
      .getOrElse(CreateJacksonParser.text(_: JsonFactory, _: Text))

    val safeParser = new FailureSafeParser[Text](
      input => parser.parse(input, textParser, textToUTF8String),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    ArchiveReader.lineIterator(in, parser.options.lineSeparatorInRead).flatMap(safeParser.parse)
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }
}

object MultiLineJsonDataSource extends JsonDataSource {
  override val isSplitable: Boolean = {
    false
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): StructType = {
    val json: RDD[PortableDataStream] =
      JsonDataSource.createBaseRdd(sparkSession, inputPaths, parsedOptions)
    val sampled: RDD[PortableDataStream] = JsonUtils.sample(json, parsedOptions)
    val parser = parsedOptions.encoding
      .map(enc => createParser(enc, _: JsonFactory, _: PortableDataStream))
      .getOrElse(createParser(_: JsonFactory, _: PortableDataStream))

    SQLExecution.withSQLConfPropagated(sparkSession) {
      new JsonInferSchema(parsedOptions)
        .infer[PortableDataStream](sampled, parser, isReadFile = true)
    }
  }

  private def dataToInputStream(dataStream: PortableDataStream): InputStream = {
    val path = new Path(dataStream.getPath())
    CodecStreams.createInputStreamWithCloseResource(dataStream.getConfiguration, path)
  }

  private def createParser(jsonFactory: JsonFactory, stream: PortableDataStream): JsonParser = {
    CreateJacksonParser.inputStream(jsonFactory, dataToInputStream(stream))
  }

  private def createParser(enc: String, jsonFactory: JsonFactory,
      stream: PortableDataStream): JsonParser = {
    CreateJacksonParser.inputStream(enc, jsonFactory, dataToInputStream(stream))
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    def partitionedFileString(ignored: Any): UTF8String = {
      Utils.tryWithResource {
        Utils.createResourceUninterruptiblyIfInTaskThread {
          CodecStreams.createInputStreamWithCloseResource(conf, file.toPath)
        }
      } { inputStream =>
        UTF8String.fromBytes(inputStream.readAllBytes())
      }
    }
    val streamParser = parser.options.encoding
      .map(enc => CreateJacksonParser.inputStream(enc, _: JsonFactory, _: InputStream))
      .getOrElse(CreateJacksonParser.inputStream(_: JsonFactory, _: InputStream))

    val safeParser = new FailureSafeParser[InputStream](
      input => parser.parse[InputStream](input, streamParser, partitionedFileString),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    safeParser.parse(
      CodecStreams.createInputStreamWithCloseResource(conf, file.toPath))
  }

  override protected def readStream(
      in: InputStream,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    // The entry is a single JSON document. Buffer its bytes so the corrupt-record column can echo
    // the whole document on a parse failure, mirroring `readFile`'s `partitionedFileString`.
    val bytes = in.readAllBytes()
    val streamParser = parser.options.encoding
      .map(enc => CreateJacksonParser.inputStream(enc, _: JsonFactory, _: InputStream))
      .getOrElse(CreateJacksonParser.inputStream(_: JsonFactory, _: InputStream))

    val safeParser = new FailureSafeParser[InputStream](
      input => parser.parse[InputStream](input, streamParser, _ => UTF8String.fromBytes(bytes)),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    safeParser.parse(new ByteArrayInputStream(bytes))
  }
}

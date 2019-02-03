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

import java.io.InputStream
import java.net.URI

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JsonInferSchema, JSONOptions}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Common functions for parsing JSON files
 */
abstract class JsonDataSource extends Serializable {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into 0 or more [[InternalRow]] instances
   */
  def readFile(
    conf: Configuration,
    file: PartitionedFile,
    parser: JacksonParser,
    schema: StructType): Iterator[InternalRow]

  final def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      Some(infer(sparkSession, inputPaths, parsedOptions))
    } else {
      None
    }
  }

  protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): StructType
}

object JsonDataSource {
  def apply(options: JSONOptions): JsonDataSource = {
    if (options.multiLine) {
      MultiLineJsonDataSource
    } else {
      TextInputJsonDataSource
    }
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
        options = parsedOptions.parameters
      ).resolveRelation(checkFilesExist = false))
      .select("value").as(Encoders.STRING)
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser,
      schema: StructType): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
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
    val json: RDD[PortableDataStream] = createBaseRdd(sparkSession, inputPaths, parsedOptions)
    val sampled: RDD[PortableDataStream] = JsonUtils.sample(json, parsedOptions)
    val parser = parsedOptions.encoding
      .map(enc => createParser(enc, _: JsonFactory, _: PortableDataStream))
      .getOrElse(createParser(_: JsonFactory, _: PortableDataStream))

    SQLExecution.withSQLConfPropagated(sparkSession) {
      new JsonInferSchema(parsedOptions).infer[PortableDataStream](sampled, parser)
    }
  }

  private def createBaseRdd(
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
        CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath)))
      } { inputStream =>
        UTF8String.fromBytes(ByteStreams.toByteArray(inputStream))
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
      CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath))))
  }
}

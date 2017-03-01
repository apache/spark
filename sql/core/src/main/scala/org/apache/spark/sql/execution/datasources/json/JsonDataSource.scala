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

import scala.reflect.ClassTag

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}

import org.apache.spark.TaskContext
import org.apache.spark.input.{PortableDataStream, StreamInputFormat}
import org.apache.spark.rdd.{BinaryFileRDD, RDD}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.execution.datasources.{CodecStreams, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Common functions for parsing JSON files
 * @tparam T A datatype containing the unparsed JSON, such as [[Text]] or [[String]]
 */
abstract class JsonDataSource[T] extends Serializable {
  def isSplitable: Boolean

  /**
   * Parse a [[PartitionedFile]] into 0 or more [[InternalRow]] instances
   */
  def readFile(
    conf: Configuration,
    file: PartitionedFile,
    parser: JacksonParser): Iterator[InternalRow]

  /**
   * Create an [[RDD]] that handles the preliminary parsing of [[T]] records
   */
  protected def createBaseRdd(
    sparkSession: SparkSession,
    inputPaths: Seq[FileStatus]): RDD[T]

  /**
   * A generic wrapper to invoke the correct [[JsonFactory]] method to allocate a [[JsonParser]]
   * for an instance of [[T]]
   */
  def createParser(jsonFactory: JsonFactory, value: T): JsonParser

  final def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: JSONOptions): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      val jsonSchema = JsonInferSchema.infer(
        createBaseRdd(sparkSession, inputPaths),
        parsedOptions,
        createParser)
      checkConstraints(jsonSchema)
      Some(jsonSchema)
    } else {
      None
    }
  }

  /** Constraints to be imposed on schema to be stored. */
  private def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to JSON format")
    }
  }
}

object JsonDataSource {
  def apply(options: JSONOptions): JsonDataSource[_] = {
    if (options.wholeFile) {
      WholeFileJsonDataSource
    } else {
      TextInputJsonDataSource
    }
  }

  /**
   * Create a new [[RDD]] via the supplied callback if there is at least one file to process,
   * otherwise an [[org.apache.spark.rdd.EmptyRDD]] will be returned.
   */
  def createBaseRdd[T : ClassTag](
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus])(
      fn: (Configuration, String) => RDD[T]): RDD[T] = {
    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      val job = Job.getInstance(sparkSession.sessionState.newHadoopConf())
      FileInputFormat.setInputPaths(job, paths: _*)
      fn(job.getConfiguration, paths.mkString(","))
    } else {
      sparkSession.sparkContext.emptyRDD[T]
    }
  }
}

object TextInputJsonDataSource extends JsonDataSource[Text] {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    true
  }

  override protected def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus]): RDD[Text] = {
    JsonDataSource.createBaseRdd(sparkSession, inputPaths) {
      case (conf, name) =>
        sparkSession.sparkContext.newAPIHadoopRDD(
          conf,
          classOf[TextInputFormat],
          classOf[LongWritable],
          classOf[Text])
          .setName(s"JsonLines: $name")
          .values // get the text column
    }
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
    linesReader.flatMap(parser.parse(_, createParser, textToUTF8String))
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }

  override def createParser(jsonFactory: JsonFactory, value: Text): JsonParser = {
    CreateJacksonParser.text(jsonFactory, value)
  }
}

object WholeFileJsonDataSource extends JsonDataSource[PortableDataStream] {
  override val isSplitable: Boolean = {
    false
  }

  override protected def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus]): RDD[PortableDataStream] = {
    JsonDataSource.createBaseRdd(sparkSession, inputPaths) {
      case (conf, name) =>
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

  private def createInputStream(config: Configuration, path: String): InputStream = {
    val inputStream = CodecStreams.createInputStream(config, new Path(path))
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => inputStream.close()))
    inputStream
  }

  override def createParser(jsonFactory: JsonFactory, record: PortableDataStream): JsonParser = {
    CreateJacksonParser.inputStream(
      jsonFactory,
      createInputStream(record.getConfiguration, record.getPath()))
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: JacksonParser): Iterator[InternalRow] = {
    def partitionedFileString(ignored: Any): UTF8String = {
      Utils.tryWithResource(createInputStream(conf, file.filePath)) { inputStream =>
        UTF8String.fromBytes(ByteStreams.toByteArray(inputStream))
      }
    }

    parser.parse(
      createInputStream(conf, file.filePath),
      CreateJacksonParser.inputStream,
      partitionedFileString).toIterator
  }
}

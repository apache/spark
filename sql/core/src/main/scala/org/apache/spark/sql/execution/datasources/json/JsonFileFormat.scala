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

import java.io.CharArrayWriter

import com.fasterxml.jackson.core.JsonFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class JsonFileFormat extends FileFormat with DataSourceRegister {

  override def shortName(): String = "json"

  override def toString: String = "JSON"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      None
    } else {
      val parsedOptions: JSONOptions = new JSONOptions(options)
      val columnNameOfCorruptRecord =
        parsedOptions.columnNameOfCorruptRecord
          .getOrElse(sparkSession.sessionState.conf.columnNameOfCorruptRecord)
      val jsonFiles = files.filterNot { status =>
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray

      val jsonSchema = InferSchema.infer(
        createBaseRdd(sparkSession, jsonFiles),
        columnNameOfCorruptRecord,
        parsedOptions)
      checkConstraints(jsonSchema)

      Some(jsonSchema)
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    JsonFileFormat.prepareConfForWriting(conf, options)

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        // Returns a 'batch' JsonOutputWriter
        new JsonOutputWriterBase(dataSchema, context) {
          private[json] override val recordWriter: RecordWriter[NullWritable, Text] = {
            new TextOutputFormat[NullWritable, Text]() {
              override def getDefaultWorkFile(
                  context: TaskAttemptContext, extension: String): Path = {
                val conf = context.getConfiguration
                val uniqueWriteJobId = conf.get(CreateDataSourceTableUtils.DATASOURCE_WRITEJOBUUID)
                val taskAttemptId = context.getTaskAttemptID
                val split = taskAttemptId.getTaskID.getId
                val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
                new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$bucketString.json$extension")
              }
            }.getRecordWriter(context)
          }
        }
      }
    }
  }

  override def buildWriter(
      sqlContext: SQLContext,
      dataSchema: StructType,
      options: Map[String, String]): OutputWriterFactory = {
    new StreamingJsonOutputWriterFactory(
      sqlContext.conf,
      dataSchema,
      sqlContext.sparkContext.hadoopConfiguration,
      options)
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
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val parsedOptions: JSONOptions = new JSONOptions(options)
    val columnNameOfCorruptRecord = parsedOptions.columnNameOfCorruptRecord
      .getOrElse(sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    (file: PartitionedFile) => {
      val lines = new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value).map(_.toString)

      JacksonParser.parseJson(
        lines,
        requiredSchema,
        columnNameOfCorruptRecord,
        parsedOptions)
    }
  }

  private def createBaseRdd(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus]): RDD[String] = {
    val job = Job.getInstance(sparkSession.sessionState.newHadoopConf())
    val conf = job.getConfiguration

    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }

    sparkSession.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString) // get the text line
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

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[JsonFileFormat]
}

/**
 * A factory for generating [[OutputWriter]]s for writing json files. This is implemented different
 * from the 'batch' JsonOutputWriter as this does not use any [[OutputCommitter]]. It simply
 * writes the data to the path used to generate the output writer. Callers of this factory
 * has to ensure which files are to be considered as committed.
 */
private[json] class StreamingJsonOutputWriterFactory(
    sqlConf: SQLConf,
    dataSchema: StructType,
    hadoopConf: Configuration,
    options: Map[String, String]) extends StreamingOutputWriterFactory {

  private val serializableConf = {
    val conf = Job.getInstance(hadoopConf).getConfiguration
    JsonFileFormat.prepareConfForWriting(conf, options)
    new SerializableConfiguration(conf)
  }

  /**
   * Returns a [[OutputWriter]] that writes data to the give path without using an
   * [[OutputCommitter]].
   */
  override private[sql] def newWriter(path: String): OutputWriter = {
    val hadoopTaskAttempId = new TaskAttemptID(new TaskID(new JobID, TaskType.MAP, 0), 0)
    val hadoopAttemptContext =
      new TaskAttemptContextImpl(serializableConf.value, hadoopTaskAttempId)
    // Returns a 'streaming' JsonOutputWriter
    new JsonOutputWriterBase(dataSchema, hadoopAttemptContext) {
      override private[json] val recordWriter: RecordWriter[NullWritable, Text] =
        createNoCommitterTextRecordWriter(
          path,
          hadoopAttemptContext,
          (c: TaskAttemptContext, ext: String) => { new Path(s"$path.json$ext") })
    }
  }
}

/**
 * Base JsonOutputWriter class for 'batch' JsonOutputWriter and 'streaming' JsonOutputWriter. The
 * writing logic to a single file resides in this base class.
 */
private[json] abstract class JsonOutputWriterBase(
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private[this] val writer = new CharArrayWriter()
  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
  private[this] val result = new Text()

  // different subclass may provide different record writers
  private[json] val recordWriter: RecordWriter[NullWritable, Text]

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    JacksonGenerator(dataSchema, gen)(row)
    gen.flush()

    result.set(writer.toString)
    writer.reset()

    recordWriter.write(NullWritable.get(), result)
  }

  override def close(): Unit = {
    gen.close()
    recordWriter.close(context)
  }
}

private[json] object JsonFileFormat {
  /**
   * Setup writing configurations into the given [[Configuration]].
   * Both continuous-queries writing process and non-continuous-queries writing process will
   * call this function.
   */
  private[json] def prepareConfForWriting(
      conf: Configuration,
      options: Map[String, String]): Unit = {
    val parsedOptions: JSONOptions = new JSONOptions(options)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }
  }
}

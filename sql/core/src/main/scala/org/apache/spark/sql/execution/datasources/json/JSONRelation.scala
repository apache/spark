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
import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

class DefaultSource extends FileFormat with DataSourceRegister {

  override def shortName(): String = "json"

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      None
    } else {
      val parsedOptions: JSONOptions = new JSONOptions(options)
      val jsonFiles = files.filterNot { status =>
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray

      val jsonSchema = InferSchema.infer(
        createBaseRdd(sqlContext, jsonFiles),
        sqlContext.conf.columnNameOfCorruptRecord,
        parsedOptions)
      checkConstraints(jsonSchema)

      Some(jsonSchema)
    }
  }

  override def prepareWrite(
      sqlContext: SQLContext,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val parsedOptions: JSONOptions = new JSONOptions(options)
    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, bucketId, dataSchema, context)
      }
    }
  }

  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {
    // TODO: Filter files for all formats before calling buildInternalScan.
    val jsonFiles = inputFiles.filterNot(_.getPath.getName startsWith "_")

    val parsedOptions: JSONOptions = new JSONOptions(options)
    val requiredDataSchema = StructType(requiredColumns.map(dataSchema(_)))
    val rows = JacksonParser.parse(
      createBaseRdd(sqlContext, jsonFiles),
      requiredDataSchema,
      sqlContext.conf.columnNameOfCorruptRecord,
      parsedOptions)

    rows.mapPartitions { iterator =>
      val unsafeProjection = UnsafeProjection.create(requiredDataSchema)
      iterator.map(unsafeProjection)
    }
  }

  private def createBaseRdd(sqlContext: SQLContext, inputPaths: Array[FileStatus]): RDD[String] = {
    val job = Job.getInstance(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }

    sqlContext.sparkContext.hadoopRDD(
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

  override def toString: String = "JSON"
  override def equals(other: Any): Boolean = other.isInstanceOf[DefaultSource]
}

private[json] class JsonOutputWriter(
    path: String,
    bucketId: Option[Int],
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private[this] val writer = new CharArrayWriter()
  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
  private[this] val result = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = context.getConfiguration
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
        val taskAttemptId = context.getTaskAttemptID
        val split = taskAttemptId.getTaskID.getId
        val bucketString = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$bucketString.json$extension")
      }
    }.getRecordWriter(context)
  }

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

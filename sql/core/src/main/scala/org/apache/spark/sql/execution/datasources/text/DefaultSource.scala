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

package org.apache.spark.sql.execution.datasources.text

import com.google.common.base.Objects
import org.apache.hadoop.fs.{Path, FileStatus}
import org.apache.hadoop.io.{NullWritable, Text, LongWritable}
import org.apache.hadoop.mapred.{TextInputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeRowWriter, BufferHolder}
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * A data source for reading text files.
 */
class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    dataSchema.foreach(verifySchema)
    new TextRelation(None, dataSchema, partitionColumns, paths)(sqlContext)
  }

  override def shortName(): String = "text"

  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw new AnalysisException(
        s"Text data source supports only a single column, and you have ${schema.size} columns.")
    }
    val tpe = schema(0).dataType
    if (tpe != StringType) {
      throw new AnalysisException(
        s"Text data source supports only a string column, but you have ${tpe.simpleString}.")
    }
  }
}

private[sql] class TextRelation(
    val maybePartitionSpec: Option[PartitionSpec],
    val textSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    override val paths: Array[String] = Array.empty[String],
    parameters: Map[String, String] = Map.empty[String, String])
    (@transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec, parameters) {

  /** Data schema is always a single column, named "value" if original Data source has no schema. */
  override def dataSchema: StructType =
    textSchema.getOrElse(new StructType().add("value", StringType))
  /** This is an internal data source that outputs internal row format. */
  override val needConversion: Boolean = false


  override private[sql] def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    val paths = inputPaths.map(_.getPath).sortBy(_.toUri)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }

    sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf], classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
      .mapPartitions { iter =>
        val bufferHolder = new BufferHolder
        val unsafeRowWriter = new UnsafeRowWriter
        val unsafeRow = new UnsafeRow

        iter.map { case (_, line) =>
          // Writes to an UnsafeRow directly
          bufferHolder.reset()
          unsafeRowWriter.initialize(bufferHolder, 1)
          unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
          unsafeRow.pointTo(bufferHolder.buffer, 1, bufferHolder.totalSize())
          unsafeRow
        }
      }
  }

  /** Write path. */
  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, context)
      }
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: TextRelation =>
      paths.toSet == that.paths.toSet && partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(paths.toSet, partitionColumns)
  }
}

class TextOutputWriter(path: String, dataSchema: StructType, context: TaskAttemptContext)
  extends OutputWriter
  with SparkHadoopMapRedUtil {

  private[this] val buffer = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
        val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    val utf8string = row.getUTF8String(0)
    buffer.set(utf8string.getBytes)
    recordWriter.write(NullWritable.get(), buffer)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}

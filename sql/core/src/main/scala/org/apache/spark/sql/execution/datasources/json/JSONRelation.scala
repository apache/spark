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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration


class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "json"

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {

    new JSONRelation(
      inputRDD = None,
      maybeDataSchema = dataSchema,
      maybePartitionSpec = None,
      userDefinedPartitionColumns = partitionColumns,
      paths = paths,
      parameters = parameters)(sqlContext)
  }
}

private[sql] class JSONRelation(
    val inputRDD: Option[RDD[String]],
    val maybeDataSchema: Option[StructType],
    val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    override val paths: Array[String] = Array.empty[String],
    parameters: Map[String, String] = Map.empty[String, String])
    (@transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec, parameters) {

  val options: JSONOptions = JSONOptions.createFromConfigMap(parameters)

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

  override val needConversion: Boolean = false

  private def createBaseRdd(inputPaths: Array[FileStatus]): RDD[String] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

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

  override lazy val dataSchema: StructType = {
    val jsonSchema = maybeDataSchema.getOrElse {
      val files = cachedLeafStatuses().filterNot { status =>
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray
      InferSchema.infer(
        inputRDD.getOrElse(createBaseRdd(files)),
        sqlContext.conf.columnNameOfCorruptRecord,
        options)
    }
    checkConstraints(jsonSchema)

    jsonSchema
  }

  override private[sql] def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    val requiredDataSchema = StructType(requiredColumns.map(dataSchema(_)))
    val rows = JacksonParser.parse(
      inputRDD.getOrElse(createBaseRdd(inputPaths)),
      requiredDataSchema,
      sqlContext.conf.columnNameOfCorruptRecord,
      options)

    rows.mapPartitions { iterator =>
      val unsafeProjection = UnsafeProjection.create(requiredDataSchema)
      iterator.map(unsafeProjection)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: JSONRelation =>
      ((inputRDD, that.inputRDD) match {
        case (Some(thizRdd), Some(thatRdd)) => thizRdd eq thatRdd
        case (None, None) => true
        case _ => false
      }) && paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      inputRDD,
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, dataSchema, context)
      }
    }
  }
}

private[json] class JsonOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with SparkHadoopMapRedUtil with Logging {

  private[this] val writer = new CharArrayWriter()
  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null)
  private[this] val result = new Text()

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

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

import java.io.{IOException, CharArrayWriter}

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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
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
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
    val primitivesAsString = parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)

    new JSONRelation(
      samplingRatio,
      primitivesAsString,
      dataSchema,
      None,
      partitionColumns,
      paths)(sqlContext)
  }
}

private[json] trait JSONSchemaCheck {

  /** Constraints to be imposed on schema to be stored. */
  def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to JSON format")
    }
  }
}

private[sql] class JSONRelation(
    val samplingRatio: Double,
    val primitivesAsString: Boolean,
    val maybeDataSchema: Option[StructType],
    val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    override val paths: Array[String] = Array.empty[String])(@transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec) with JSONSchemaCheck {

  override val needConversion: Boolean = false

  private def createBaseRdd(inputPaths: Array[FileStatus]): RDD[String] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    val rddInputPaths = inputPaths.map(_.getPath)

    if (rddInputPaths.nonEmpty) {
      FileInputFormat.setInputPaths(job, rddInputPaths: _*)
    } else {
      throw new IOException("Input paths do not exist or are empty directories, "
            + "Input Paths=" + paths.mkString(","))
    }

    sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString) // get the text line
  }

  override lazy val dataSchema = {
    val jsonSchema = maybeDataSchema.getOrElse {
      val files = cachedLeafStatuses().filterNot { status =>
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray
      InferSchema(
        createBaseRdd(files),
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord,
        primitivesAsString)
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
    val rows = JacksonParser(
      createBaseRdd(inputPaths),
      requiredDataSchema,
      sqlContext.conf.columnNameOfCorruptRecord)

    rows.mapPartitions { iterator =>
      val unsafeProjection = UnsafeProjection.create(requiredDataSchema)
      iterator.map(unsafeProjection)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: JSONRelation =>
        paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
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

private[sql] class JSONRDDRelation (
    val inputRDD: RDD[String],
    val samplingRatio: Double,
    val primitivesAsString: Boolean,
    val maybeDataSchema: Option[StructType])(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with JSONSchemaCheck {

  override def schema = {
    val jsonSchema = maybeDataSchema.getOrElse {
      InferSchema(
        inputRDD,
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord,
        primitivesAsString)
    }
    checkConstraints(jsonSchema)
    jsonSchema
  }


  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredDataSchema = StructType(requiredColumns.map(schema(_)))
    val rows = JacksonParser(
      inputRDD,
      requiredDataSchema,
      sqlContext.conf.columnNameOfCorruptRecord)

    rows.mapPartitions { iterator =>
      val converter = CatalystTypeConverters.createToScalaConverter(requiredDataSchema)
      iterator.map(converter(_).asInstanceOf[Row])
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: JSONRDDRelation =>
      (inputRDD eq that.inputRDD) && schema == that.schema
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      inputRDD,
      schema)
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

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

package org.apache.spark.sql.sources

import java.text.NumberFormat
import java.util.UUID

import com.google.common.base.Objects
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
 * A simple example [[FSBasedRelationProvider]].
 */
class SimpleTextSource extends FSBasedRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): FSBasedRelation = {
    new SimpleTextRelation(
      Array(parameters("path")), schema,
      partitionColumns.getOrElse(StructType(Array.empty[StructField])), parameters)(sqlContext)
  }
}

class AppendingTextOutputFormat(outputFile: Path) extends TextOutputFormat[NullWritable, Text] {
  val numberFormat = NumberFormat.getInstance()

  numberFormat.setMinimumIntegerDigits(5)
  numberFormat.setGroupingUsed(false)

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val split = context.getTaskAttemptID.getTaskID.getId
    val name = FileOutputFormat.getOutputName(context)
    new Path(outputFile, s"$name-${numberFormat.format(split)}-${UUID.randomUUID()}")
  }
}

class SimpleTextOutputWriter extends OutputWriter {
  private var recordWriter: RecordWriter[NullWritable, Text] = _
  private var taskAttemptContext: TaskAttemptContext = _

  override def init(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): Unit = {
    recordWriter = new AppendingTextOutputFormat(new Path(path)).getRecordWriter(context)
    taskAttemptContext = context
  }

  override def write(row: Row): Unit = {
    val serialized = row.toSeq.map(_.toString).mkString(",")
    recordWriter.write(null, new Text(serialized))
  }

  override def close(): Unit = recordWriter.close(taskAttemptContext)
}

/**
 * A simple example [[FSBasedRelation]], used for testing purposes.  Data are stored as comma
 * separated string lines.  When scanning data, schema must be explicitly provided via data source
 * option `"dataSchema"`.
 */
class SimpleTextRelation(
    paths: Array[String],
    val maybeDataSchema: Option[StructType],
    partitionsSchema: StructType,
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends FSBasedRelation(paths, partitionsSchema) {

  import sqlContext.sparkContext

  override val dataSchema: StructType =
    maybeDataSchema.getOrElse(DataType.fromJson(parameters("dataSchema")).asInstanceOf[StructType])

  override def equals(other: Any): Boolean = other match {
    case that: SimpleTextRelation =>
      this.paths.sameElements(that.paths) &&
        this.maybeDataSchema == that.maybeDataSchema &&
        this.dataSchema == that.dataSchema &&
        this.partitionColumns == that.partitionColumns

    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(paths, maybeDataSchema, dataSchema)

  override def outputWriterClass: Class[_ <: OutputWriter] =
    classOf[SimpleTextOutputWriter]

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {
    val fields = dataSchema.fields
    sparkContext.textFile(inputPaths.mkString(",")).map { record =>
      val valueMap = record.split(",").zip(fields).map {
        case (value, StructField(name, dataType, _, _)) =>
          name -> Cast(Literal(value), dataType).eval()
      }.toMap

      // This mocks a simple projection
      Row.fromSeq(requiredColumns.map(valueMap).toSeq)
    }
  }
}

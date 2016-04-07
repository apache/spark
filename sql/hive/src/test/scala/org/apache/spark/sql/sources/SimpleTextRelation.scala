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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{sources, Row, SQLContext}
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, InterpretedPredicate, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

class SimpleTextSource extends FileFormat with DataSourceRegister {
  override def shortName(): String = "test"

  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(DataType.fromJson(options("dataSchema")).asInstanceOf[StructType])
  }

  override def prepareWrite(
      sqlContext: SQLContext,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = new OutputWriterFactory {
    override private[sql] def newInstance(
        path: String,
        bucketId: Option[Int],
        dataSchema: StructType,
        context: TaskAttemptContext): OutputWriter = {
      new SimpleTextOutputWriter(path, context)
    }
  }

  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Seq[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {

    val fields = dataSchema.map(_.dataType)
    val inputAttributes = dataSchema.toAttributes
    val outputAttributes = requiredColumns.flatMap(name => inputAttributes.find(_.name == name))

    val inputPaths = inputFiles.map(_.getPath).mkString(",")
    sqlContext.sparkContext.textFile(inputPaths).mapPartitions { iterator =>
      // Constructs a filter predicate to simulate filter push-down
      val predicate = {
        val filterCondition: Expression = filters.collect {
          // According to `unhandledFilters`, `SimpleTextRelation` only handles `GreaterThan` filter
          case sources.GreaterThan(column, value) =>
            val dataType = dataSchema(column).dataType
            val literal = Literal.create(value, dataType)
            val attribute = inputAttributes.find(_.name == column).get
            expressions.GreaterThan(attribute, literal)
        }.reduceOption(expressions.And).getOrElse(Literal(true))
        InterpretedPredicate.create(filterCondition, inputAttributes)
      }

      // Uses a simple projection to simulate column pruning
      val projection = GenerateUnsafeProjection.generate(outputAttributes, inputAttributes)

      iterator.map { record =>
        val row = new GenericInternalRow(record.split(",", -1).zip(fields).map {
          case (v, dataType) =>
            val value = if (v == "") null else v
            // `Cast`ed values are always of internal types (e.g. UTF8String instead of String)
            Cast(Literal(value), dataType).eval()
        })
        row
      }.filter { row =>
        predicate(row)
      }.map { row =>
        projection(row)
      }
    }
  }
}

class SimpleTextOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter {
  private val recordWriter: RecordWriter[NullWritable, Text] =
    new AppendingTextOutputFormat(new Path(path)).getRecordWriter(context)

  override def write(row: Row): Unit = {
    val serialized = row.toSeq.map { v =>
      if (v == null) "" else v.toString
    }.mkString(",")
    recordWriter.write(null, new Text(serialized))
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}

class AppendingTextOutputFormat(outputFile: Path) extends TextOutputFormat[NullWritable, Text] {
  val numberFormat = NumberFormat.getInstance()

  numberFormat.setMinimumIntegerDigits(5)
  numberFormat.setGroupingUsed(false)

  override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
    val configuration = context.getConfiguration
    val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = context.getTaskAttemptID
    val split = taskAttemptId.getTaskID.getId
    val name = FileOutputFormat.getOutputName(context)
    new Path(outputFile, s"$name-${numberFormat.format(split)}-$uniqueWriteJobId")
  }
}

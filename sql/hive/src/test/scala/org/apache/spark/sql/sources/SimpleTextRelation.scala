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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, InterpretedProjection, JoinedRow, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.Utils

case class SimpleTextSource() extends TextBasedFileFormat with DataSourceRegister {
  override def shortName(): String = "test"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(DataType.fromJson(options("dataSchema")).asInstanceOf[StructType])
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    SimpleTextRelation.lastHadoopConf = Option(job.getConfiguration)
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new SimpleTextOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = ""
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    SimpleTextRelation.lastHadoopConf = Option(hadoopConf)
    SimpleTextRelation.requiredColumns = requiredSchema.fieldNames.toImmutableArraySeq
    SimpleTextRelation.pushedFilters = filters.toSet

    val fieldTypes = dataSchema.map(_.dataType)
    val inputAttributes = DataTypeUtils.toAttributes(dataSchema)
    val outputAttributes = requiredSchema.flatMap { field =>
      inputAttributes.find(_.name == field.name)
    }

    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)

    (file: PartitionedFile) => {
      val predicate = {
        val filterCondition: Expression = filters.collect {
          // According to `unhandledFilters`, `SimpleTextRelation` only handles `GreaterThan` filter
          case sources.GreaterThan(column, value) =>
            val dataType = dataSchema(column).dataType
            val literal = Literal.create(value, dataType)
            val attribute = inputAttributes.find(_.name == column).get
            expressions.GreaterThan(attribute, literal)
        }.reduceOption(expressions.And).getOrElse(Literal(true))
        Predicate.create(filterCondition, inputAttributes)
      }

      // Uses a simple projection to simulate column pruning
      val projection = new InterpretedProjection(outputAttributes, inputAttributes)

      val unsafeRowIterator = Utils.createResourceUninterruptiblyIfInTaskThread(
        new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value)
      ).map { line =>
          val record = line.toString
          new GenericInternalRow(record.split(",", -1).zip(fieldTypes).map {
            case (v, dataType) =>
              val value = if (v == "") null else v
              // `Cast`ed values are always of internal types (e.g. UTF8String instead of String)
              Cast(Literal(value), dataType).eval()
          })
        }.filter(predicate.eval).map(projection)

      // Appends partition values
      val fullOutput = DataTypeUtils.toAttributes(requiredSchema) ++
        DataTypeUtils.toAttributes(partitionSchema)
      val joinedRow = new JoinedRow()
      val appendPartitionColumns = GenerateUnsafeProjection.generate(fullOutput, fullOutput)

      unsafeRowIterator.map { dataRow =>
        appendPartitionColumns(joinedRow(dataRow, file.partitionValues))
      }
    }
  }
}

class SimpleTextOutputWriter(val path: String, dataSchema: StructType, context: TaskAttemptContext)
  extends OutputWriter {

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

  override def write(row: InternalRow): Unit = {
    val serialized = row.toSeq(dataSchema).map { v =>
      if (v == null) "" else v.toString
    }.mkString(",")

    writer.write(serialized)
    writer.write('\n')
  }

  override def close(): Unit = {
    writer.close()
  }
}

object SimpleTextRelation {
  // Used to test column pruning
  var requiredColumns: Seq[String] = Nil

  // Used to test filter push-down
  var pushedFilters: Set[Filter] = Set.empty

  // Used to test failed committer
  var failCommitter = false

  // Used to test failed writer
  var failWriter = false

  // Used to test failure callback
  var callbackCalled = false

  // Used by the test case to check the value propagated in the hadoop confs.
  var lastHadoopConf: Option[Configuration] = None
}

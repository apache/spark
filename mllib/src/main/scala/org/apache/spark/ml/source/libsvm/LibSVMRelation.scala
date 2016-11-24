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

package org.apache.spark.ml.source.libsvm

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[libsvm] class LibSVMOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter {

  private[this] val buffer = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = context.getConfiguration
        val uniqueWriteJobId = configuration.get(CreateDataSourceTableUtils.DATASOURCE_WRITEJOBUUID)
        val taskAttemptId = context.getTaskAttemptID
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = {
    val label = row.get(0)
    val vector = row.get(1).asInstanceOf[Vector]
    val sb = new StringBuilder(label.toString)
    vector.foreachActive { case (i, v) =>
      sb += ' '
      sb ++= s"${i + 1}:$v"
    }
    buffer.set(sb.mkString)
    recordWriter.write(NullWritable.get(), buffer)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}

/** @see [[LibSVMDataSource]] for public documentation. */
// If this is moved or renamed, please update DataSource's backwardCompatibilityMap.
private[libsvm] class LibSVMFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "libsvm"

  override def toString: String = "LibSVM"

  private def verifySchema(dataSchema: StructType): Unit = {
    if (
      dataSchema.size != 2 ||
        !dataSchema(0).dataType.sameType(DataTypes.DoubleType) ||
        !dataSchema(1).dataType.sameType(new VectorUDT()) ||
        !(dataSchema(1).metadata.getLong("numFeatures").toInt > 0)
    ) {
      throw new IOException(s"Illegal schema for libsvm data, schema=$dataSchema")
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val numFeatures: Int = options.get("numFeatures").map(_.toInt).filter(_ > 0).getOrElse {
      // Infers number of features if the user doesn't specify (a valid) one.
      val dataFiles = files.filterNot(_.getPath.getName startsWith "_")
      val path = if (dataFiles.length == 1) {
        dataFiles.head.getPath.toUri.toString
      } else if (dataFiles.isEmpty) {
        throw new IOException("No input path specified for libsvm data")
      } else {
        throw new IOException("Multiple input paths are not supported for libsvm data.")
      }

      val sc = sparkSession.sparkContext
      val parsed = MLUtils.parseLibSVMFile(sc, path, sc.defaultParallelism)
      MLUtils.computeNumFeatures(parsed)
    }

    val featuresMetadata = new MetadataBuilder()
      .putLong("numFeatures", numFeatures)
      .build()

    Some(
      StructType(
        StructField("label", DoubleType, nullable = false) ::
        StructField("features", new VectorUDT(), nullable = false, featuresMetadata) :: Nil))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        if (bucketId.isDefined) { sys.error("LibSVM doesn't support bucketing") }
        new LibSVMOutputWriter(path, dataSchema, context)
      }
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
    verifySchema(dataSchema)
    val numFeatures = dataSchema("features").metadata.getLong("numFeatures").toInt
    assert(numFeatures > 0)

    val sparse = options.getOrElse("vectorType", "sparse") == "sparse"

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val linesReader = new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))

      val points = linesReader
          .map(_.toString.trim)
          .filterNot(line => line.isEmpty || line.startsWith("#"))
          .map { line =>
            val (label, indices, values) = MLUtils.parseLibSVMRecord(line)
            LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
          }

      val converter = RowEncoder(dataSchema)
      val fullOutput = dataSchema.map { f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
      val requiredOutput = fullOutput.filter { a =>
        requiredSchema.fieldNames.contains(a.name)
      }

      val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

      points.map { pt =>
        val features = if (sparse) pt.features.toSparse else pt.features.toDense
        requiredColumns(converter.toRow(Row(pt.label, features)))
      }
    }
  }
}

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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection.BitSet

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
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
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

/**
 * `libsvm` package implements Spark SQL data source API for loading LIBSVM data as [[DataFrame]].
 * The loaded [[DataFrame]] has two columns: `label` containing labels stored as doubles and
 * `features` containing feature vectors stored as [[Vector]]s.
 *
 * To use LIBSVM data source, you need to set "libsvm" as the format in [[DataFrameReader]] and
 * optionally specify options, for example:
 * {{{
 *   // Scala
 *   val df = sqlContext.read.format("libsvm")
 *     .option("numFeatures", "780")
 *     .load("data/mllib/sample_libsvm_data.txt")
 *
 *   // Java
 *   DataFrame df = sqlContext.read().format("libsvm")
 *     .option("numFeatures, "780")
 *     .load("data/mllib/sample_libsvm_data.txt");
 * }}}
 *
 * LIBSVM data source supports the following options:
 *  - "numFeatures": number of features.
 *    If unspecified or nonpositive, the number of features will be determined automatically at the
 *    cost of one additional pass.
 *    This is also useful when the dataset is already split into multiple files and you want to load
 *    them separately, because some features may not present in certain files, which leads to
 *    inconsistent feature dimensions.
 *  - "vectorType": feature vector type, "sparse" (default) or "dense".
 *
 *  @see [[https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/ LIBSVM datasets]]
 */
@Since("1.6.0")
class DefaultSource extends FileFormat with DataSourceRegister {

  @Since("1.6.0")
  override def shortName(): String = "libsvm"

  private def verifySchema(dataSchema: StructType): Unit = {
    if (dataSchema.size != 2 ||
      (!dataSchema(0).dataType.sameType(DataTypes.DoubleType)
        || !dataSchema(1).dataType.sameType(new VectorUDT()))) {
      throw new IOException(s"Illegal schema for libsvm data, schema=${dataSchema}")
    }
  }
  override def inferSchema(
      sqlContext: SQLContext,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(
      StructType(
        StructField("label", DoubleType, nullable = false) ::
        StructField("features", new VectorUDT(), nullable = false) :: Nil))
  }

  override def prepareWrite(
      sqlContext: SQLContext,
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

  override def buildInternalScan(
      sqlContext: SQLContext,
      dataSchema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      bucketSet: Option[BitSet],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration],
      options: Map[String, String]): RDD[InternalRow] = {
    // TODO: This does not handle cases where column pruning has been performed.

    verifySchema(dataSchema)
    val dataFiles = inputFiles.filterNot(_.getPath.getName startsWith "_")

    val path = if (dataFiles.length == 1) dataFiles(0).getPath.toUri.toString
    else if (dataFiles.isEmpty) throw new IOException("No input path specified for libsvm data")
    else throw new IOException("Multiple input paths are not supported for libsvm data.")

    val numFeatures = options.getOrElse("numFeatures", "-1").toInt
    val vectorType = options.getOrElse("vectorType", "sparse")

    val sc = sqlContext.sparkContext
    val baseRdd = MLUtils.loadLibSVMFile(sc, path, numFeatures)
    val sparse = vectorType == "sparse"
    baseRdd.map { pt =>
      val features = if (sparse) pt.features.toSparse else pt.features.toDense
      Row(pt.label, features)
    }.mapPartitions { externalRows =>
      val converter = RowEncoder(dataSchema)
      externalRows.map(converter.toRow)
    }
  }
}

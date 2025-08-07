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

package org.apache.spark.ml.source.image

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

private[image] case class ImageFileFormat() extends FileFormat with DataSourceRegister {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(ImageSchema.imageSchema)

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for image data source")
  }

  override def shortName(): String = "image"

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Image data source only produces a single data column named \"image\".")

    val broadcastedHadoopConf =
      SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)

    val imageSourceOptions = new ImageOptions(options)

    (file: PartitionedFile) => {
      val emptyUnsafeRow = new UnsafeRow(0)
      if (!imageSourceOptions.dropInvalid && requiredSchema.isEmpty) {
        Iterator(emptyUnsafeRow)
      } else {
        val origin = file.urlEncodedPath
        val path = file.toPath
        val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
        val stream = fs.open(path)
        val bytes = try {
          ByteStreams.toByteArray(stream)
        } finally {
          Closeables.close(stream, true)
        }
        val resultOpt = ImageSchema.decode(origin, bytes)
        val filteredResult = if (imageSourceOptions.dropInvalid) {
          resultOpt.iterator
        } else {
          Iterator(resultOpt.getOrElse(ImageSchema.invalidImageRow(origin)))
        }

        if (requiredSchema.isEmpty) {
          filteredResult.map(_ => emptyUnsafeRow)
        } else {
          val toRow = ExpressionEncoder(requiredSchema).createSerializer()
          filteredResult.map(row => toRow(row))
        }
      }
    }
  }
}

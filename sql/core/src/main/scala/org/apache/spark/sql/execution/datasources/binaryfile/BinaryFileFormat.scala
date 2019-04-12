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

package org.apache.spark.sql.execution.datasources.binaryfile

import java.sql.Timestamp

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobFilter, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration


private[binaryfile] class BinaryFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(BinaryFileDataSource.binaryFileSchema)

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException("Write is not supported for binary file data source")
  }

  override def shortName(): String = "binaryFile"

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val binaryFileSourceOptions = new BinaryFileSourceOptions(options)

    val pathFilterRegex = binaryFileSourceOptions.pathFilterRegex
    val globFilter = if (pathFilterRegex.isEmpty) { null } else {
      new GlobFilter(pathFilterRegex)
    }

    (file: PartitionedFile) => {
      val path = file.filePath
      val fsPath = new Path(path)

      if (globFilter == null || globFilter.accept(fsPath)) {
        val fs = fsPath.getFileSystem(broadcastedHadoopConf.value.value)
        val fileStatus = fs.getFileStatus(fsPath)
        val length = fileStatus.getLen()
        val modificationTime = new Timestamp(fileStatus.getModificationTime())
        val stream = fs.open(fsPath)
        val content = try {
          ByteStreams.toByteArray(stream)
        } finally {
          Closeables.close(stream, true)
        }

        val converter = RowEncoder(dataSchema)
        val fullOutput = dataSchema.map { f =>
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        }
        val requiredOutput = fullOutput.filter { a =>
          requiredSchema.fieldNames.contains(a.name)
        }

        val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

        val row = Row(Row(path, modificationTime, length), content)

        Iterator(requiredColumns(converter.toRow(row)))
      } else {
        Iterator.empty
      }
    }
  }
}

private[binaryfile] class BinaryFileSourceOptions(
    @transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * only include files with path matching the regex pattern.
   */
  val pathFilterRegex = parameters.getOrElse("pathFilterRegex", "").toString
}

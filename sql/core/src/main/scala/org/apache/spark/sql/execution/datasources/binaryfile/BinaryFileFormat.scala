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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
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

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    false
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

    val pathGlobPattern = binaryFileSourceOptions.pathGlobFilter

    (file: PartitionedFile) => {
      val path = file.filePath
      val fsPath = new Path(path)

      // TODO: Improve performance here: each file will recompile the glob pattern here.
      val globFilter = pathGlobPattern.map(new GlobFilter(_))
      if (!globFilter.isDefined || globFilter.get.accept(fsPath)) {
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

        val fullOutput = dataSchema.map { f =>
          AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
        }
        val requiredOutput = fullOutput.filter { a =>
          requiredSchema.fieldNames.contains(a.name)
        }

        val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

        val internalRow = InternalRow(
          content,
          InternalRow(
            UTF8String.fromString(path),
            DateTimeUtils.fromJavaTimestamp(modificationTime),
            length
          )
        )

        Iterator(requiredColumns(internalRow))
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
   * only include files with path matching the glob pattern.
   */
  val pathGlobFilter: Option[String] = {
    val filter = parameters.getOrElse("pathGlobFilter", null)
    if (filter != null) Some(filter) else None
  }
}

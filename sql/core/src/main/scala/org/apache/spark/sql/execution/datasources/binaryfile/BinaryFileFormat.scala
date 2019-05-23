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

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf.SOURCES_BINARY_FILE_MAX_LENGTH
import org.apache.spark.sql.sources.{And, DataSourceRegister, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Not, Or}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration


/**
 * The binary file data source.
 *
 * It reads binary files and converts each file into a single record that contains the raw content
 * and metadata of the file.
 *
 * Example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("binaryFile")
 *     .load("/path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("binaryFile")
 *     .load("/path/to/fileDir");
 * }}}
 */
class BinaryFileFormat extends FileFormat with DataSourceRegister {

  import BinaryFileFormat._

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(schema)

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

  override def shortName(): String = BINARY_FILE

  override protected def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    require(dataSchema.sameType(schema),
      s"""
         |Binary file data source expects dataSchema: $schema,
         |but got: $dataSchema.
        """.stripMargin)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val filterFuncs = filters.map(filter => createFilterFunction(filter))
    val maxLength = sparkSession.conf.get(SOURCES_BINARY_FILE_MAX_LENGTH)

    file: PartitionedFile => {
      val path = new Path(file.filePath)
      val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
      val status = fs.getFileStatus(path)
      if (filterFuncs.forall(_.apply(status))) {
        val writer = new UnsafeRowWriter(requiredSchema.length)
        writer.resetRowWriter()
        requiredSchema.fieldNames.zipWithIndex.foreach {
          case (PATH, i) => writer.write(i, UTF8String.fromString(status.getPath.toString))
          case (LENGTH, i) => writer.write(i, status.getLen)
          case (MODIFICATION_TIME, i) =>
            writer.write(i, DateTimeUtils.fromMillis(status.getModificationTime))
          case (CONTENT, i) =>
            if (status.getLen > maxLength) {
              throw new SparkException(
                s"The length of ${status.getPath} is ${status.getLen}, " +
                  s"which exceeds the max length allowed: ${maxLength}.")
            }
            val stream = fs.open(status.getPath)
            try {
              writer.write(i, ByteStreams.toByteArray(stream))
            } finally {
              Closeables.close(stream, true)
            }
          case (other, _) =>
            throw new RuntimeException(s"Unsupported field name: ${other}")
        }
        Iterator.single(writer.getRow)
      } else {
        Iterator.empty
      }
    }
  }
}

object BinaryFileFormat {

  private[binaryfile] val PATH = "path"
  private[binaryfile] val MODIFICATION_TIME = "modificationTime"
  private[binaryfile] val LENGTH = "length"
  private[binaryfile] val CONTENT = "content"
  private[binaryfile] val BINARY_FILE = "binaryFile"

  /**
   * Schema for the binary file data source.
   *
   * Schema:
   *  - path (StringType): The path of the file.
   *  - modificationTime (TimestampType): The modification time of the file.
   *    In some Hadoop FileSystem implementation, this might be unavailable and fallback to some
   *    default value.
   *  - length (LongType): The length of the file in bytes.
   *  - content (BinaryType): The content of the file.
   */
  val schema = StructType(
    StructField(PATH, StringType, false) ::
    StructField(MODIFICATION_TIME, TimestampType, false) ::
    StructField(LENGTH, LongType, false) ::
    StructField(CONTENT, BinaryType, true) :: Nil)

  private[binaryfile] def createFilterFunction(filter: Filter): FileStatus => Boolean = {
    filter match {
      case And(left, right) =>
        s => createFilterFunction(left)(s) && createFilterFunction(right)(s)
      case Or(left, right) =>
        s => createFilterFunction(left)(s) || createFilterFunction(right)(s)
      case Not(child) =>
        s => !createFilterFunction(child)(s)

      case LessThan(LENGTH, value: Long) =>
        _.getLen < value
      case LessThanOrEqual(LENGTH, value: Long) =>
        _.getLen <= value
      case GreaterThan(LENGTH, value: Long) =>
        _.getLen > value
      case GreaterThanOrEqual(LENGTH, value: Long) =>
        _.getLen >= value
      case EqualTo(LENGTH, value: Long) =>
        _.getLen == value

      case LessThan(MODIFICATION_TIME, value: Timestamp) =>
        _.getModificationTime < value.getTime
      case LessThanOrEqual(MODIFICATION_TIME, value: Timestamp) =>
        _.getModificationTime <= value.getTime
      case GreaterThan(MODIFICATION_TIME, value: Timestamp) =>
        _.getModificationTime > value.getTime
      case GreaterThanOrEqual(MODIFICATION_TIME, value: Timestamp) =>
        _.getModificationTime >= value.getTime
      case EqualTo(MODIFICATION_TIME, value: Timestamp) =>
        _.getModificationTime == value.getTime

      case _ => (_ => true)
    }
  }
}


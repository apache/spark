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

import scala.collection.mutable

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobFilter, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{And, DataSourceRegister, Filter, GreaterThan, GreaterThanOrEqual,
  LessThan, LessThanOrEqual}
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
 *     .option("pathGlobFilter", "*.png")
 *     .load("/path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("binaryFile")
 *     .option("pathGlobFilter", "*.png")
 *     .load("/path/to/fileDir");
 * }}}
 */
class BinaryFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(BinaryFileFormat.schema)

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

    val modificationTimeFilters = new mutable.ListBuffer[Long => Boolean]
    val lengthFilters = new mutable.ListBuffer[Long => Boolean]

    def parseFilter(filter: Filter): Unit = {
      filter match {
        case andFilter: And =>
          parseFilter(andFilter.left)
          parseFilter(andFilter.right)
        case lessThan: LessThan =>
          if (lessThan.attribute == "length"
            && lessThan.value.isInstanceOf[Long]) {
            lengthFilters.append(
              (x: Long) => x < lessThan.value.asInstanceOf[Long]
            )
          } else if (lessThan.attribute == "modificationTime"
            && lessThan.value.isInstanceOf[Timestamp]) {
            modificationTimeFilters.append(
              (x: Long) => x < lessThan.value.asInstanceOf[Timestamp].getTime
            )
          }
        case lessThanOrEq: LessThanOrEqual =>
          if (lessThanOrEq.attribute == "length"
            && lessThanOrEq.value.isInstanceOf[Long]) {
            lengthFilters.append(
              (x: Long) => x <= lessThanOrEq.value.asInstanceOf[Long]
            )
          } else if (lessThanOrEq.attribute == "modificationTime"
            && lessThanOrEq.value.isInstanceOf[Timestamp]) {
            modificationTimeFilters.append(
              (x: Long) => x <= lessThanOrEq.value.asInstanceOf[Timestamp].getTime
            )
          }
        case greaterThan: GreaterThan =>
          if (greaterThan.attribute == "length"
            && greaterThan.value.isInstanceOf[Long]) {
            lengthFilters.append(
              (x: Long) => x > greaterThan.value.asInstanceOf[Long]
            )
          } else if (greaterThan.attribute == "modificationTime"
            && greaterThan.value.isInstanceOf[Timestamp]) {
            modificationTimeFilters.append(
              (x: Long) => x > greaterThan.value.asInstanceOf[Timestamp].getTime
            )
          }
        case greaterThanOrEq: GreaterThanOrEqual =>
          if (greaterThanOrEq.attribute == "length"
            && greaterThanOrEq.value.isInstanceOf[Long]) {
            lengthFilters.append(
              (x: Long) => x >= greaterThanOrEq.value.asInstanceOf[Long]
            )
          } else if (greaterThanOrEq.attribute == "modificationTime"
            && greaterThanOrEq.value.isInstanceOf[Timestamp]) {
            modificationTimeFilters.append(
              (x: Long) => x >= greaterThanOrEq.value.asInstanceOf[Timestamp].getTime
            )
          }
        case _ => () // Do nothing
      }
    }

    filters.foreach(parseFilter(_))

    (file: PartitionedFile) => {
      val path = file.filePath
      val fsPath = new Path(path)

      // TODO: Improve performance here: each file will recompile the glob pattern here.
      val globFilter = pathGlobPattern.map(new GlobFilter(_))
      if (!globFilter.isDefined || globFilter.get.accept(fsPath)) {
        val fs = fsPath.getFileSystem(broadcastedHadoopConf.value.value)
        val fileStatus = fs.getFileStatus(fsPath)
        val length = fileStatus.getLen()
        val modificationTime = fileStatus.getModificationTime()

        if (
          (modificationTimeFilters.size == 0
            || modificationTimeFilters.map(_.apply(modificationTime)).reduce(_ && _))
          &&
            (lengthFilters.size == 0
              || lengthFilters.map(_.apply(length)).reduce(_ && _))
        ) {
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

          // TODO: Add column pruning
          // currently it still read the file content even if content column is not required.
          val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

          val internalRow = InternalRow(
            UTF8String.fromString(path),
            DateTimeUtils.fromMillis(modificationTime),
            length,
            content
          )

          Iterator(requiredColumns(internalRow))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }
  }
}

object BinaryFileFormat {

  private val fileStatusSchema = StructType(
    StructField("path", StringType, false) ::
      StructField("modificationTime", TimestampType, false) ::
      StructField("length", LongType, false) :: Nil)

  /**
   * Schema for the binary file data source.
   *
   * Schema:
   *  - content (BinaryType): The content of the file.
   *  - status (StructType): The status of the file.
   *    - path (StringType): The path of the file.
   *    - modificationTime (TimestampType): The modification time of the file.
   *      In some Hadoop FileSystem implementation, this might be unavailable and fallback to some
   *      default value.
   *    - length (LongType): The length of the file in bytes.
   */
  val schema = StructType(
    StructField("path", StringType, false) ::
    StructField("modificationTime", TimestampType, false) ::
    StructField("length", LongType, false) ::
    StructField("content", BinaryType, true) :: Nil)
}

class BinaryFileSourceOptions(
    @transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * An optional glob pattern to only include files with paths matching the pattern.
   * The syntax follows [[org.apache.hadoop.fs.GlobFilter]].
   */
  val pathGlobFilter: Option[String] = parameters.get("pathGlobFilter")
}

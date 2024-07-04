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

package org.apache.spark.sql.avro

import java.io._

import scala.util.control.NonFatal

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.LogicalType
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[sql] class AvroFileFormat extends FileFormat
  with DataSourceRegister with Logging with Serializable {

  AvroFileFormat.registerCustomAvroTypes()

  override def equals(other: Any): Boolean = other match {
    case _: AvroFileFormat => true
    case _ => false
  }

  // Dummy hashCode() to appease ScalaStyle.
  override def hashCode(): Int = super.hashCode()

  override def inferSchema(
      spark: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    AvroUtils.inferSchema(spark, options, files)
  }

  override def shortName(): String = "avro"

  override def toString(): String = "Avro"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = true

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    AvroUtils.prepareWrite(spark.sessionState.conf, job, options, dataSchema)
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val broadcastedConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val parsedOptions = new AvroOptions(options, hadoopConf)
    val datetimeRebaseModeInRead = parsedOptions.datetimeRebaseModeInRead

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      val userProvidedSchema = parsedOptions.schema

      // TODO Removes this check once `FileFormat` gets a general file filtering interface method.
      // Doing input file filtering is improper because we may generate empty tasks that process no
      // input files but stress the scheduler. We should probably add a more general input file
      // filtering mechanism for `FileFormat` data sources. See SPARK-16317.
      if (parsedOptions.ignoreExtension || file.urlEncodedPath.endsWith(".avro")) {
        val reader = {
          val in = new FsInput(file.toPath, conf)
          try {
            val datumReader = userProvidedSchema match {
              case Some(userSchema) => new GenericDatumReader[GenericRecord](userSchema)
              case _ => new GenericDatumReader[GenericRecord]()
            }
            DataFileReader.openReader(in, datumReader)
          } catch {
            case NonFatal(e) =>
              logError("Exception while opening DataFileReader", e)
              in.close()
              throw e
          }
        }

        // Ensure that the reader is closed even if the task fails or doesn't consume the entire
        // iterator of records.
        Option(TaskContext.get()).foreach { taskContext =>
          taskContext.addTaskCompletionListener[Unit] { _ =>
            reader.close()
          }
        }

        reader.sync(file.start)

        val datetimeRebaseMode = DataSourceUtils.datetimeRebaseSpec(
          reader.asInstanceOf[DataFileReader[_]].getMetaString,
          datetimeRebaseModeInRead)

        val avroFilters = if (SQLConf.get.avroFilterPushDown) {
          new OrderedFilters(filters, requiredSchema)
        } else {
          new NoopFilters
        }

        new Iterator[InternalRow] with AvroUtils.RowReader {
          override val fileReader = reader
          override val deserializer = new AvroDeserializer(
            userProvidedSchema.getOrElse(reader.getSchema),
            requiredSchema,
            parsedOptions.positionalFieldMatching,
            datetimeRebaseMode,
            avroFilters,
            parsedOptions.useStableIdForUnionType,
            parsedOptions.stableIdPrefixForUnionType)
          override val stopPosition = file.start + file.length

          override def hasNext: Boolean = hasNextRow
          override def next(): InternalRow = nextRow
        }
      } else {
        Iterator.empty
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean = AvroUtils.supportsDataType(dataType)

  override def supportFieldName(name: String): Boolean = {
    if (name.length == 0) {
      false
    } else {
      name.zipWithIndex.forall {
        case (c, 0) if !Character.isLetter(c) && c != '_' => false
        case (c, _) if !Character.isLetterOrDigit(c) && c != '_' => false
        case _ => true
      }
    }
  }
}

private[avro] object AvroFileFormat {
  val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"

  /**
   * Register Spark defined custom Avro types.
   */
  def registerCustomAvroTypes(): Unit = {
    // Register the customized decimal type backed by long.
    LogicalTypes.register(CustomDecimal.TYPE_NAME, new LogicalTypes.LogicalTypeFactory {
      override def fromSchema(schema: Schema): LogicalType = {
        new CustomDecimal(schema)
      }
    })
  }

  registerCustomAvroTypes()
}

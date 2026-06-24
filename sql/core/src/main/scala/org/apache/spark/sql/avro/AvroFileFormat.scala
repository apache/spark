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

import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.avro.file.{DataFileReader, DataFileStream}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.execution.datasources.{ArchiveReader, DataSourceUtils, FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.internal.{SessionStateHelper, SQLConf}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[sql] class AvroFileFormat extends FileFormat
  with DataSourceRegister
  with SessionStateHelper
  with Logging
  with Serializable {

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
      path: Path): Boolean = {
    val parsedOptions = new AvroOptions(options, sparkSession.sessionState.newHadoopConf())
    if (parsedOptions.archiveFormatEnabled && ArchiveReader.isArchivePath(path)) {
      // A tar archive is read as one sequential stream (entry by entry), so it is never split.
      return false
    }
    true
  }

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    AvroUtils.prepareWrite(getSqlConf(spark), job, options, dataSchema)
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
      SerializableConfiguration.broadcast(spark.sparkContext, hadoopConf)
    val parsedOptions = new AvroOptions(options, hadoopConf)
    val datetimeRebaseModeInRead = parsedOptions.datetimeRebaseModeInRead

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      if (parsedOptions.archiveFormatEnabled && ArchiveReader.isArchivePath(file.toPath)) {
        // A tar archive (always a single split, see `isSplitable`) is streamed entry by entry when
        // archive reads are enabled; otherwise the file is read directly. The V2 data source has
        // no archive support, so this dispatch lives here.
        readArchive(file, conf, parsedOptions, requiredSchema, filters)
      } else {
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
            parsedOptions.stableIdPrefixForUnionType,
            parsedOptions.recursiveFieldMaxDepth)
          override val stopPosition = file.start + file.length

          override def hasNext: Boolean = hasNextRow
          override def next(): InternalRow = nextRow
        }
      } else {
        Iterator.empty
      }
      }
    }
  }

  /**
   * Streams a tar archive (`.tar`/`.tar.gz`/`.tgz`) entry by entry, deserializing each entry like a
   * standalone Avro file via a forward-only [[DataFileStream]] (so the archive is never unpacked to
   * disk and memory stays bounded). The whole archive is a single split (see `isSplitable`). A
   * fresh datum reader and deserializer are built per entry, since each entry carries its own
   * writer schema in its header.
   *
   * Kept separate from the per-file reader (rather than dispatched inside it) because only this V1
   * read path supports archives; the V2 data source is intentionally left untouched.
   */
  private def readArchive(
      file: PartitionedFile,
      conf: Configuration,
      parsedOptions: AvroOptions,
      requiredSchema: StructType,
      filters: Seq[Filter]): Iterator[InternalRow] = {
    val userProvidedSchema = parsedOptions.schema
    // Filters depend only on (filters, requiredSchema), which are constant across entries, so build
    // them once. The datum reader, writer schema, and rebase mode are per-entry (each entry carries
    // its own header schema).
    val avroFilters = if (SQLConf.get.avroFilterPushDown) {
      new OrderedFilters(filters, requiredSchema)
    } else {
      new NoopFilters
    }
    ArchiveReader(file.toPath).readEntries(conf) { (_, in) =>
      val datumReader = userProvidedSchema match {
        case Some(schema) => new GenericDatumReader[GenericRecord](schema)
        case None => new GenericDatumReader[GenericRecord]()
      }
      val stream = new DataFileStream[GenericRecord](in, datumReader)
      val avroSchema = userProvidedSchema.getOrElse(stream.getSchema)
      val datetimeRebaseMode = DataSourceUtils.datetimeRebaseSpec(
        stream.getMetaString, parsedOptions.datetimeRebaseModeInRead)
      val deserializer = new AvroDeserializer(
        avroSchema,
        requiredSchema,
        parsedOptions.positionalFieldMatching,
        datetimeRebaseMode,
        avroFilters,
        parsedOptions.useStableIdForUnionType,
        parsedOptions.stableIdPrefixForUnionType,
        parsedOptions.recursiveFieldMaxDepth)
      // The record is deserialized eagerly in `hasNext` because `AvroDeserializer#deserialize` may
      // filter rows (returning None); the stream is closed once its records are exhausted.
      new Iterator[InternalRow] with Closeable {
        private var nextRow: Option[InternalRow] = None

        private def advance(): Unit = {
          while (nextRow.isEmpty && stream.hasNext) {
            nextRow = deserializer.deserialize(stream.next()).asInstanceOf[Option[InternalRow]]
          }
          if (nextRow.isEmpty) close()
        }

        override def hasNext: Boolean = {
          advance()
          nextRow.isDefined
        }

        override def next(): InternalRow = {
          advance()
          val row = nextRow.getOrElse(throw new NoSuchElementException("next on empty iterator"))
          nextRow = None
          row
        }

        override def close(): Unit = stream.close()
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

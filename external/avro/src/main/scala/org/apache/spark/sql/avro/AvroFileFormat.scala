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
import java.net.URI

import scala.util.control.NonFatal

import org.apache.avro.Schema
import org.apache.avro.file.DataFileConstants._
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils}

private[avro] class AvroFileFormat extends FileFormat
  with DataSourceRegister with Logging with Serializable {

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
    val conf = spark.sessionState.newHadoopConf()
    val parsedOptions = new AvroOptions(options, conf)

    // User can specify an optional avro json schema.
    val avroSchema = parsedOptions.schema
      .map(new Schema.Parser().parse)
      .getOrElse {
        inferAvroSchemaFromFiles(files, conf, parsedOptions.ignoreExtension,
          spark.sessionState.conf.ignoreCorruptFiles)
    }

    SchemaConverters.toSqlType(avroSchema).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""Avro schema cannot be converted to a Spark SQL StructType:
           |
           |${avroSchema.toString(true)}
           |""".stripMargin)
    }
  }

  private def inferAvroSchemaFromFiles(
      files: Seq[FileStatus],
      conf: Configuration,
      ignoreExtension: Boolean,
      ignoreCorruptFiles: Boolean): Schema = {
    // Schema evolution is not supported yet. Here we only pick first random readable sample file to
    // figure out the schema of the whole dataset.
    val avroReader = files.iterator.map { f =>
      val path = f.getPath
      if (!ignoreExtension && !path.getName.endsWith(".avro")) {
        None
      } else {
        Utils.tryWithResource {
          new FsInput(path, conf)
        } { in =>
          try {
            Some(DataFileReader.openReader(in, new GenericDatumReader[GenericRecord]()))
          } catch {
            case e: IOException =>
              if (ignoreCorruptFiles) {
                logWarning(s"Skipped the footer in the corrupted file: $path", e)
                None
              } else {
                throw new SparkException(s"Could not read file: $path", e)
              }
          }
        }
      }
    }.collectFirst {
      case Some(reader) => reader
    }

    avroReader match {
      case Some(reader) =>
        try {
          reader.getSchema
        } finally {
          reader.close()
        }
      case None =>
        throw new FileNotFoundException(
          "No Avro files found. If files don't have .avro extension, set ignoreExtension to true")
    }
  }

  override def shortName(): String = "avro"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = true

  override def prepareWrite(
      spark: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val parsedOptions = new AvroOptions(options, spark.sessionState.newHadoopConf())
    val outputAvroSchema: Schema = parsedOptions.schema
      .map(new Schema.Parser().parse)
      .getOrElse(SchemaConverters.toAvroType(dataSchema, nullable = false,
        parsedOptions.recordName, parsedOptions.recordNamespace))

    AvroJob.setOutputKeySchema(job, outputAvroSchema)

    if (parsedOptions.compression == "uncompressed") {
      job.getConfiguration.setBoolean("mapred.output.compress", false)
    } else {
      job.getConfiguration.setBoolean("mapred.output.compress", true)
      logInfo(s"Compressing Avro output using the ${parsedOptions.compression} codec")
      val codec = parsedOptions.compression match {
        case DEFLATE_CODEC =>
          val deflateLevel = spark.sessionState.conf.avroDeflateLevel
          logInfo(s"Avro compression level $deflateLevel will be used for $DEFLATE_CODEC codec.")
          job.getConfiguration.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, deflateLevel)
          DEFLATE_CODEC
        case codec @ (SNAPPY_CODEC | BZIP2_CODEC | XZ_CODEC) => codec
        case unknown => throw new IllegalArgumentException(s"Invalid compression codec: $unknown")
      }
      job.getConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, codec)
    }

    new AvroOutputWriterFactory(dataSchema, outputAvroSchema.toString)
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

    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      val userProvidedSchema = parsedOptions.schema.map(new Schema.Parser().parse)

      // TODO Removes this check once `FileFormat` gets a general file filtering interface method.
      // Doing input file filtering is improper because we may generate empty tasks that process no
      // input files but stress the scheduler. We should probably add a more general input file
      // filtering mechanism for `FileFormat` data sources. See SPARK-16317.
      if (parsedOptions.ignoreExtension || file.filePath.endsWith(".avro")) {
        val reader = {
          val in = new FsInput(new Path(new URI(file.filePath)), conf)
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
        val stop = file.start + file.length

        val deserializer =
          new AvroDeserializer(userProvidedSchema.getOrElse(reader.getSchema), requiredSchema)

        new Iterator[InternalRow] {
          private[this] var completed = false

          override def hasNext: Boolean = {
            if (completed) {
              false
            } else {
              val r = reader.hasNext && !reader.pastSync(stop)
              if (!r) {
                reader.close()
                completed = true
              }
              r
            }
          }

          override def next(): InternalRow = {
            if (!hasNext) {
              throw new NoSuchElementException("next on empty iterator")
            }
            val record = reader.next()
            deserializer.deserialize(record).asInstanceOf[InternalRow]
          }
        }
      } else {
        Iterator.empty
      }
    }
  }
}

private[avro] object AvroFileFormat {
  val IgnoreFilesWithoutExtensionProperty = "avro.mapred.ignore.inputs.without.extension"
}

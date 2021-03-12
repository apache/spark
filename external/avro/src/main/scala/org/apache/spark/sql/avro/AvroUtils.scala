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

import java.io.{FileNotFoundException, IOException}

import org.apache.avro.Schema
import org.apache.avro.file.DataFileConstants.{BZIP2_CODEC, DEFLATE_CODEC, SNAPPY_CODEC, XZ_CODEC}
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.{AvroOutputFormat, FsInput}
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroOptions.ignoreExtensionKey
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[sql] object AvroUtils extends Logging {
  def inferSchema(
      spark: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new AvroOptions(options, conf)

    if (parsedOptions.parameters.contains(ignoreExtensionKey)) {
      logWarning(s"Option $ignoreExtensionKey is deprecated. Please use the " +
        "general data source option pathGlobFilter for filtering file names.")
    }
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

  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val parsedOptions = new AvroOptions(options, job.getConfiguration)
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
          val deflateLevel = sqlConf.avroDeflateLevel
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
}

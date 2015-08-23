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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException

import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.{FileMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.{Footer, ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.schema.MessageType

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._


private[parquet] object ParquetTypesConverter extends Logging {
  def isPrimitiveType(ctype: DataType): Boolean = ctype match {
    case _: NumericType | BooleanType | DateType | TimestampType | StringType | BinaryType => true
    case _ => false
  }

  /**
   * Compute the FIXED_LEN_BYTE_ARRAY length needed to represent a given DECIMAL precision.
   */
  private[parquet] val BYTES_FOR_PRECISION = Array.tabulate[Int](38) { precision =>
    var length = 1
    while (math.pow(2.0, 8 * length - 1) < math.pow(10.0, precision)) {
      length += 1
    }
    length
  }

  def convertFromAttributes(attributes: Seq[Attribute]): MessageType = {
    val converter = new CatalystSchemaConverter()
    converter.convert(StructType.fromAttributes(attributes))
  }

  def convertFromString(string: String): Seq[Attribute] = {
    Try(DataType.fromJson(string)).getOrElse(DataType.fromCaseClassString(string)) match {
      case s: StructType => s.toAttributes
      case other => sys.error(s"Can convert $string to row")
    }
  }

  def convertToString(schema: Seq[Attribute]): String = {
    schema.map(_.name).foreach(CatalystSchemaConverter.checkFieldName)
    StructType.fromAttributes(schema).json
  }

  def writeMetaData(attributes: Seq[Attribute], origPath: Path, conf: Configuration): Unit = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to write Parquet metadata: path is null")
    }
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to write Parquet metadata: path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (fs.exists(path) && !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(s"Expected to write to directory $path but found file")
    }
    val metadataPath = new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)
    if (fs.exists(metadataPath)) {
      try {
        fs.delete(metadataPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(s"Unable to delete previous PARQUET_METADATA_FILE at $metadataPath")
      }
    }
    val extraMetadata = new java.util.HashMap[String, String]()
    extraMetadata.put(
      CatalystReadSupport.SPARK_METADATA_KEY,
      ParquetTypesConverter.convertToString(attributes))
    // TODO: add extra data, e.g., table name, date, etc.?

    val parquetSchema: MessageType = ParquetTypesConverter.convertFromAttributes(attributes)
    val metaData: FileMetaData = new FileMetaData(
      parquetSchema,
      extraMetadata,
      "Spark")

    ParquetFileWriter.writeMetadataFile(
      conf,
      path,
      new Footer(path, new ParquetMetadata(metaData, Nil)) :: Nil)
  }

  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @param configuration The Hadoop configuration to use.
   * @return The `ParquetMetadata` containing among other things the schema.
   */
  def readMetaData(origPath: Path, configuration: Option[Configuration]): ParquetMetadata = {
    if (origPath == null) {
      throw new IllegalArgumentException("Unable to read Parquet metadata: path is null")
    }
    val job = new Job()
    val conf = configuration.getOrElse(ContextUtil.getConfiguration(job))
    val fs: FileSystem = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(s"Incorrectly formatted Parquet metadata path $origPath")
    }
    val path = origPath.makeQualified(fs)

    val children =
      fs
        .globStatus(path)
        .flatMap { status => if (status.isDir) fs.listStatus(status.getPath) else List(status) }
        .filterNot { status =>
          val name = status.getPath.getName
          (name(0) == '.' || name(0) == '_') && name != ParquetFileWriter.PARQUET_METADATA_FILE
        }

    // NOTE (lian): Parquet "_metadata" file can be very slow if the file consists of lots of row
    // groups. Since Parquet schema is replicated among all row groups, we only need to touch a
    // single row group to read schema related metadata. Notice that we are making assumptions that
    // all data in a single Parquet file have the same schema, which is normally true.
    children
      // Try any non-"_metadata" file first...
      .find(_.getPath.getName != ParquetFileWriter.PARQUET_METADATA_FILE)
      // ... and fallback to "_metadata" if no such file exists (which implies the Parquet file is
      // empty, thus normally the "_metadata" file is expected to be fairly small).
      .orElse(children.find(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE))
      .map(ParquetFileReader.readFooter(conf, _, ParquetMetadataConverter.NO_FILTER))
      .getOrElse(
        throw new IllegalArgumentException(s"Could not find Parquet metadata at path $path"))
  }
}

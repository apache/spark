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

package org.apache.spark.sql.execution.datasources.orc

import java.io.IOException

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.orc.{OrcFile, TypeDescription}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._

object OrcUtils extends Logging {

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    "NONE" -> "",
    "SNAPPY" -> ".snappy",
    "ZLIB" -> ".zlib",
    "LZO" -> ".lzo")

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDirectory)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))
    paths
  }

  private[orc] def readSchema(file: Path, conf: Configuration): Option[TypeDescription] = {
    try {
      val fs = file.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val reader = OrcFile.createReader(file, readerOptions)
      val schema = reader.getSchema
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        Some(schema)
      }
    } catch {
      case _: IOException => None
    }
  }

  private[orc] def readSchema(sparkSession: SparkSession, files: Seq[FileStatus])
      : Option[StructType] = {
    val conf = sparkSession.sessionState.newHadoopConf()
    // TODO: We need to support merge schema. Please see SPARK-11412.
    files.map(_.getPath).flatMap(readSchema(_, conf)).headOption.map { schema =>
      logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
      CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }

  private[orc] def getSchemaString(schema: StructType): String = {
    schema.fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
  }

  private[orc] def getTypeDescription(dataType: DataType) = dataType match {
    case st: StructType => TypeDescription.fromString(getSchemaString(st))
    case _ => TypeDescription.fromString(dataType.catalogString)
  }

  /**
   * Return a missing schema in a give ORC file.
   */
  private[orc] def getMissingSchema(
      isCaseSensitive: Boolean,
      dataSchema: StructType,
      partitionSchema: StructType,
      file: Path,
      conf: Configuration): Option[StructType] = {
    val resolver = if (isCaseSensitive) caseSensitiveResolution else caseInsensitiveResolution
    try {
      val fs = file.getFileSystem(conf)
      val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
      val reader = OrcFile.createReader(file, readerOptions)
      val schema = reader.getSchema
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        val orcSchema = if (schema.getFieldNames.asScala.forall(_.startsWith("_col"))) {
          logInfo("Recover ORC schema with data schema")
          var schemaString = schema.toString
          dataSchema.zipWithIndex.foreach { case (field: StructField, index: Int) =>
            schemaString = schemaString.replace(s"_col$index:", s"${field.name}:")
          }
          TypeDescription.fromString(schemaString)
        } else {
          schema
        }

        var missingSchema = new StructType
        if (dataSchema.length > orcSchema.getFieldNames.size) {
          dataSchema.filter(x => partitionSchema.getFieldIndex(x.name).isEmpty).foreach { f =>
            if (!orcSchema.getFieldNames.asScala.exists(resolver(_, f.name))) {
              missingSchema = missingSchema.add(f)
            }
          }
        }
        Some(missingSchema)
      }
    } catch {
      case _: IOException => None
    }
  }
}

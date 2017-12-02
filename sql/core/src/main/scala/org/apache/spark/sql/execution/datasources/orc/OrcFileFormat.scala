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

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.orc.{OrcFile, TypeDescription}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType

private[sql] object OrcFileFormat extends Logging {
  private def checkFieldName(name: String): Unit = {
    try {
      TypeDescription.fromString(s"struct<$name:int>")
    } catch {
      case _: IllegalArgumentException =>
        throw new AnalysisException(
          s"""Column name "$name" contains invalid character(s).
             |Please use alias to rename it.
           """.stripMargin.split("\n").mkString(" ").trim)
    }
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  def getSchemaString(schema: StructType): String = {
    schema.fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
  }

  private def readSchema(file: Path, conf: Configuration, fs: FileSystem) = {
    try {
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

  def readSchema(sparkSession: SparkSession, files: Seq[FileStatus]): Option[StructType] = {
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    files.map(_.getPath).flatMap(readSchema(_, conf, fs)).headOption.map { schema =>
      logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
      CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }
}

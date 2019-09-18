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
package org.apache.spark.sql.execution.datasources.v2.text

import java.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TextDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[TextFileFormat]

  override def shortName(): String = "text"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(paths)
    TextTable(tableName, sparkSession, options, paths, None, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(paths)
    TextTable(tableName, sparkSession, options, paths, Some(schema), None, fallbackFileFormat)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (schema != StructType(Seq(StructField("value", StringType)))) {
      throw new AnalysisException(
        "Text table format requires a single string column with the name value.")
    }
    val options = toOptions(ident)
    val paths = getPaths(options)
    val tableName = getTableName(paths)
    TextTable(
      tableName, sparkSession, options, paths, Some(schema), Some(partitions), fallbackFileFormat)
  }
}


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
package org.apache.spark.sql.execution.datasources.v2

import java.util

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.v2.{Identifier, PathCatalog, TableCatalog, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.Table
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait FileCatalog extends PathCatalog with DataSourceRegister {
  private var options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()
  private var _name: String = shortName()

  def getTableProvider: FileDataSourceV2

  override def loadTable(ident: Identifier): Table = {
    val tableProvider = getTableProvider
    val tableOptions = DataSourceV2Utils.extractSessionConfigs(
      tableProvider, SparkSession.active.sessionState.conf)
    val extraOptions = options.asCaseSensitiveMap().asScala.toMap
    val caseInsensitiveStringMap =
      new CaseInsensitiveStringMap((tableOptions ++ extraOptions).asJava)
    if (options.containsKey(DataSourceUtils.USER_SPECIFIED_SCHEMA_KEY)) {
      val schemaString = options.get(DataSourceUtils.USER_SPECIFIED_SCHEMA_KEY)
      val userSpecifiedSchema = StructType.fromString(schemaString)
      tableProvider.getTable(caseInsensitiveStringMap, userSpecifiedSchema)
    } else {
      tableProvider.getTable(caseInsensitiveStringMap)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val path = new Path(ident.name)
    val fs = getFileSystem(path)
    fs.mkdirs(path)
    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val path = new Path(ident.name)
    val fs = getFileSystem(path)
    if (!fs.exists(path)) {
      false
    } else {
      fs.delete(path, true)
      true
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] =
    throw new UnsupportedOperationException("")

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("")

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this._name = name
    this.options = options
  }

  override def name(): String = _name

  private def getFileSystem(path: Path): FileSystem = {
    val sparkSession = SparkSession.active
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    path.getFileSystem(sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap))
  }
}

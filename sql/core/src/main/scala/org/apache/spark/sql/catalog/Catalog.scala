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
package org.apache.spark.sql.catalog

import java.util

import org.apache.spark.sql.{api, DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

/** @inheritdoc */
abstract class Catalog extends api.Catalog[Dataset] {
  /** @inheritdoc */
  override def listDatabases(): Dataset[Database]

  /** @inheritdoc */
  override def listDatabases(pattern: String): Dataset[Database]

  /** @inheritdoc */
  override def listTables(): Dataset[Table]

  /** @inheritdoc */
  override def listTables(dbName: String): Dataset[Table]

  /** @inheritdoc */
  override def listTables(dbName: String, pattern: String): Dataset[Table]

  /** @inheritdoc */
  override def listFunctions(): Dataset[Function]

  /** @inheritdoc */
  override def listFunctions(dbName: String): Dataset[Function]

  /** @inheritdoc */
  override def listFunctions(dbName: String, pattern: String): Dataset[Function]

  /** @inheritdoc */
  override def listColumns(tableName: String): Dataset[Column]

  /** @inheritdoc */
  override def listColumns(dbName: String, tableName: String): Dataset[Column]

  /** @inheritdoc */
  override def createTable(tableName: String, path: String): DataFrame

  /** @inheritdoc */
  override def createTable(tableName: String, path: String, source: String): DataFrame

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      description: String,
      options: Map[String, String]): DataFrame

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]): DataFrame

  /** @inheritdoc */
  override def listCatalogs(): Dataset[CatalogMetadata]

  /** @inheritdoc */
  override def listCatalogs(pattern: String): Dataset[CatalogMetadata]

  /** @inheritdoc */
  override def createExternalTable(tableName: String, path: String): DataFrame =
    super.createExternalTable(tableName, path)

  /** @inheritdoc */
  override def createExternalTable(tableName: String, path: String, source: String): DataFrame =
    super.createExternalTable(tableName, path, source)

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      options: util.Map[String, String]): DataFrame =
    super.createExternalTable(tableName, source, options)

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      options: util.Map[String, String]): DataFrame =
    super.createTable(tableName, source, options)

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame =
    super.createExternalTable(tableName, source, options)

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: util.Map[String, String]): DataFrame =
    super.createExternalTable(tableName, source, schema, options)

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      description: String,
      options: util.Map[String, String]): DataFrame =
    super.createTable(tableName, source, description, options)

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: util.Map[String, String]): DataFrame =
    super.createTable(tableName, source, schema, options)

  /** @inheritdoc */
  override def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame =
    super.createExternalTable(tableName, source, schema, options)

  /** @inheritdoc */
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: util.Map[String, String]): DataFrame =
    super.createTable(tableName, source, schema, description, options)
}

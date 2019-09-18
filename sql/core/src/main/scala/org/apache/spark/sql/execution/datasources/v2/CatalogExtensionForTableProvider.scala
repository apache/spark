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

import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class CatalogExtensionForTableProvider extends DelegatingCatalogExtension {

  private val conf = SQLConf.get

  override def loadTable(ident: Identifier): Table = {
    val table = super.loadTable(ident)
    tryResolveTableProvider(table)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val provider = properties.getOrDefault("provider", conf.defaultDataSourceName)
    val maybeProvider = DataSource.lookupDataSourceV2(provider, conf)
    val (actualSchema, actualPartitioning) = if (maybeProvider.isDefined && schema.isEmpty) {
      // A sanity check. The parser should guarantee it.
      assert(partitions.isEmpty)
      // If `CREATE TABLE ... USING` does not specify table metadata, get the table metadata from
      // data source first.
      val table = maybeProvider.get.loadTable(properties)
      table.schema() -> table.partitioning()
    } else {
      schema -> partitions
    }
    super.createTable(ident, actualSchema, actualPartitioning, properties)
    // call `loadTable` to make sure the schema/partitioning specified in `CREATE TABLE ... USING`
    // matches the actual data schema/partitioning. If error happens during table loading, drop
    // the table.
    try {
      loadTable(ident)
    } catch {
      case NonFatal(e) =>
        dropTable(ident)
        throw e
    }
  }

  private def tryResolveTableProvider(table: Table): Table = {
    val providerName = table.properties().get("provider")
    assert(providerName != null)
    DataSource.lookupDataSourceV2(providerName, conf).map { provider =>
      // TODO: support file source v2 in CREATE TABLE USING.
      if (provider.isInstanceOf[FileDataSourceV2]) {
        table
      } else {
        val loaded = provider.loadTable(table.schema, table.partitioning, table.properties)
        if (loaded.schema().asNullable != table.schema.asNullable) {
          throw new AnalysisException(s"Table provider '$providerName' returns a table " +
            "which has inappropriate schema:\n" +
            s"schema in Spark meta-store:\t${table.schema}\n" +
            s"schema from table provider:\t${loaded.schema()}")
        }
        if (!loaded.partitioning().sameElements(table.partitioning)) {
          throw new AnalysisException(s"Table provider '$providerName' returns a table " +
            "which has inappropriate partitioning:\n" +
            s"partitioning in Spark meta-store:\t${table.partitioning.mkString(", ")}\n" +
            s"partitioning from table provider:\t${loaded.partitioning.mkString(", ")}")
        }
        loaded
      }
    }.getOrElse(table)
  }
}

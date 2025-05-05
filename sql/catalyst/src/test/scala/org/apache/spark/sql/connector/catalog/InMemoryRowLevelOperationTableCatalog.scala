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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.expressions.Transform

class InMemoryRowLevelOperationTableCatalog extends InMemoryTableCatalog {
  import CatalogV2Implicits._

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }

    InMemoryTableCatalog.maybeSimulateFailedTableCreation(properties)

    val tableName = s"$name.${ident.quoted}"
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    val table = new InMemoryRowLevelOperationTable(tableName, schema, partitions, properties)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    createTable(ident, tableInfo.columns(), tableInfo.partitions(), tableInfo.properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
    val properties = CatalogV2Util.applyPropertiesChanges(table.properties, changes)
    val schema = CatalogV2Util.applySchemaChanges(
      table.schema,
      changes,
      tableProvider = Some("in-memory"),
      statementType = "ALTER TABLE")
    val partitioning = CatalogV2Util.applyClusterByChanges(table.partitioning, schema, changes)
    val constraints = CatalogV2Util.collectConstraintChanges(table, changes)

    // fail if the last column in the schema was dropped
    if (schema.fields.isEmpty) {
      throw new IllegalArgumentException(s"Cannot drop all fields")
    }

    val newTable = new InMemoryRowLevelOperationTable(
      name = table.name,
      schema = schema,
      partitioning = partitioning,
      properties = properties,
      constraints = constraints)
    newTable.withData(table.data)

    tables.put(ident, newTable)

    newTable
  }
}

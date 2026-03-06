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

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException

class InMemoryRowLevelOperationTableCatalog extends InMemoryTableCatalog {
  import CatalogV2Implicits._

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }

    InMemoryTableCatalog.maybeSimulateFailedTableCreation(tableInfo.properties)

    val tableName = s"$name.${ident.quoted}"
    val schema = CatalogV2Util.v2ColumnsToStructType(tableInfo.columns)
    val table = new InMemoryRowLevelOperationTable(
      tableName, schema, tableInfo.partitions, tableInfo.properties, tableInfo.constraints())
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
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
    newTable.alterTableWithData(table.data, schema)

    tables.put(ident, newTable)

    newTable
  }
}

/**
 * A catalog that silently ignores schema changes in alterTable (e.g. AddColumn).
 */
class PartialSchemaEvolutionCatalog extends InMemoryRowLevelOperationTableCatalog {

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident).asInstanceOf[InMemoryRowLevelOperationTable]
    // Only apply property changes; ignore all schema/column changes so that the table
    // schema remains unchanged. This simulates a catalog that accepts alterTable but
    // does not support some requested changes.
    val propertyChanges = changes.filter {
      case _: TableChange.SetProperty => true
      case _: TableChange.RemoveProperty => true
      case _ => false
    }
    val properties = CatalogV2Util.applyPropertiesChanges(table.properties, propertyChanges)
    val newTable = new InMemoryRowLevelOperationTable(
      name = table.name,
      schema = table.schema,
      partitioning = table.partitioning,
      properties = properties,
      constraints = table.constraints)
    newTable.alterTableWithData(table.data, table.schema)
    tables.put(ident, newTable)
    newTable
  }
}

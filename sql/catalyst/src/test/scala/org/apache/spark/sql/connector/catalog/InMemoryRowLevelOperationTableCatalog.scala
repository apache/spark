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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.transactions.{Transaction, TransactionInfo}
import org.apache.spark.sql.types.StructType

class InMemoryRowLevelOperationTableCatalog
    extends InMemoryTableCatalog
    with TransactionalCatalogPlugin {
  import CatalogV2Implicits._

  // The current active transaction.
  var transaction: Txn = _
  // The last completed transaction.
  var lastTransaction: Txn = _
  // All transactions in order (committed and aborted), allowing per-statement
  // validation in SQL scripting tests.
  val observedTransactions: ArrayBuffer[Txn] = new ArrayBuffer[Txn]()

  override def beginTransaction(info: TransactionInfo): Transaction = {
    assert(transaction == null || transaction.currentState != Active)
    this.transaction = new Txn(new TxnTableCatalog(this))
    transaction
  }

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
    val schema = computeAlterTableSchema(table.schema, changes.toSeq)
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
      constraints = constraints,
      tableId = table.id)
    newTable.alterTableWithData(table.data, schema)

    tables.put(ident, newTable)

    newTable
  }

  /**
   * Computes the schema that would result from applying `changes` to `currentSchema`.
   * Can be overridden by subclasses to simulate catalogs that selectively ignore changes
   * (e.g. [[PartialSchemaEvolutionCatalog]]).
   */
  def computeAlterTableSchema(currentSchema: StructType, changes: Seq[TableChange]): StructType = {
    CatalogV2Util.applySchemaChanges(
      currentSchema, changes, tableProvider = Some("in-memory"), statementType = "ALTER TABLE")
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
    val schema = computeAlterTableSchema(table.schema, changes.toSeq)
    val newTable = new InMemoryRowLevelOperationTable(
      name = table.name,
      schema = schema,
      partitioning = table.partitioning,
      properties = properties,
      constraints = table.constraints)
    newTable.alterTableWithData(table.data, table.schema)
    tables.put(ident, newTable)
    newTable
  }

  // Ignores all schema changes and returns the current schema unchanged.
  override def computeAlterTableSchema(
      currentSchema: StructType,
      changes: Seq[TableChange]): StructType = currentSchema
}

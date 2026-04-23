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

import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.connector.ColumnImpl

/**
 * An [[InMemoryTableCatalog]] that strips column IDs from all columns
 * ([[Column.id]] returns null). This simulates connectors that do not
 * support column identity tracking.
 *
 * Tables are stored as [[NullColumnIdInMemoryTable]] instances that
 * override [[columns]] to strip IDs. Data is copied from the table
 * created by the parent [[InMemoryTableCatalog]].
 *
 * When column IDs are null, [[V2TableUtil.validateColumnIds]]
 * skips validation entirely, meaning drop/re-add of a column is NOT
 * detected via column IDs.
 */
class NullColumnIdInMemoryTableCatalog extends InMemoryTableCatalog {

  private def toNullColumnIdTable(table: InMemoryTable): NullColumnIdInMemoryTable = {
    val nullColIdTable = new NullColumnIdInMemoryTable(
      name = table.name,
      columns = table.columns(),
      partitioning = table.partitioning,
      properties = table.properties,
      constraints = table.constraints,
      id = table.id)
    nullColIdTable.alterTableWithData(table.data, table.schema)
    nullColIdTable
  }

  override def createTable(
      ident: Identifier,
      info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val nullColIdTable = toNullColumnIdTable(table)
    tables.put(ident, nullColIdTable)
    nullColIdTable
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]
    val nullColIdTable = toNullColumnIdTable(table)
    tables.put(ident, nullColIdTable)
    nullColIdTable
  }
}

/**
 * An [[InMemoryTable]] whose [[columns]] method always returns null
 * column IDs, simulating a connector that does not support column
 * identity tracking.
 */
class NullColumnIdInMemoryTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: java.util.Map[String, String],
    constraints: Array[Constraint] = Array.empty,
    override val id: String = java.util.UUID.randomUUID().toString)
  extends InMemoryTable(
    name = name,
    columns = columns,
    partitioning = partitioning,
    properties = properties,
    constraints = constraints,
    id = id) {

  override def columns(): Array[Column] = {
    super.columns().map(_.asInstanceOf[ColumnImpl].copy(id = null))
  }

  override def copy(): Table = {
    val copiedTable = new NullColumnIdInMemoryTable(
      name,
      columns(),
      partitioning,
      properties,
      constraints,
      id)
    dataMap.synchronized {
      copiedTable.alterTableWithData(data, schema)
    }
    copiedTable
  }
}

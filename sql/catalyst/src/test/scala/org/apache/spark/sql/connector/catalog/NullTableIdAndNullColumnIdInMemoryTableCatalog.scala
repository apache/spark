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
 * An [[InMemoryTableCatalog]] that strips both table IDs ([[Table.id]]
 * returns null) and column IDs ([[Column.id]] returns null). This simulates
 * connectors that support neither table nor column identity tracking.
 *
 * When both IDs are null, neither [[V2TableRefreshUtil.validateTableIdentity]]
 * nor [[V2TableUtil.validateColumnIds]] fires, so drop/recreate of a table
 * or drop/re-add of a column goes undetected.
 */
class NullTableIdAndNullColumnIdInMemoryTableCatalog extends InMemoryTableCatalog {

  private def toNullIdsTable(
      table: InMemoryTable): NullTableIdAndNullColumnIdInMemoryTable = {
    val nullTable = new NullTableIdAndNullColumnIdInMemoryTable(
      name = table.name,
      columns = table.columns(),
      partitioning = table.partitioning,
      properties = table.properties,
      constraints = table.constraints)
    nullTable.alterTableWithData(table.data, table.schema)
    nullTable.setVersionAndValidatedVersionFrom(table)
    nullTable
  }

  override def createTable(
      ident: Identifier,
      info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val nullTable = toNullIdsTable(table)
    tables.put(ident, nullTable)
    nullTable
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]
    val nullTable = toNullIdsTable(table)
    tables.put(ident, nullTable)
    nullTable
  }
}

/**
 * An [[InMemoryTable]] with both null table ID and null column IDs,
 * simulating a connector that supports neither identity tracking mechanism.
 */
class NullTableIdAndNullColumnIdInMemoryTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: java.util.Map[String, String],
    constraints: Array[Constraint] = Array.empty)
  extends InMemoryTable(
    name = name,
    columns = columns,
    partitioning = partitioning,
    properties = properties,
    constraints = constraints,
    id = null) {

  override def columns(): Array[Column] = {
    super.columns().map(_.asInstanceOf[ColumnImpl].copy(id = null))
  }

  override def copy(): Table = {
    val copiedTable = new NullTableIdAndNullColumnIdInMemoryTable(
      name,
      columns(),
      partitioning,
      properties,
      constraints)
    dataMap.synchronized {
      copiedTable.alterTableWithData(data, schema)
    }
    copiedTable.setVersionAndValidatedVersionFrom(this)
    copiedTable
  }
}

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

import org.apache.spark.sql.internal.connector.ColumnImpl

/**
 * An [[InMemoryTableCatalog]] that strips column IDs from all columns
 * ([[Column.id]] returns null). This simulates connectors that do not
 * support column identity tracking.
 *
 * When column IDs are null, [[validateCapturedColumnIds]] skips
 * validation entirely, meaning drop/re-add of a column is NOT
 * detected via column IDs.
 */
class NullColumnIdInMemoryTableCatalog extends InMemoryTableCatalog {

  private def stripColumnIds(columns: Array[Column]): Array[Column] = {
    columns.map(_.asInstanceOf[ColumnImpl].copy(id = null))
  }

  override def createTable(
      ident: Identifier,
      info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val nullColIdTable = new InMemoryTable(
      table.name,
      stripColumnIds(table.columns()),
      table.partitioning,
      table.properties,
      table.constraints)
    tables.put(ident, nullColIdTable)
    nullColIdTable
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]
    val nullColIdTable = new InMemoryTable(
      table.name,
      stripColumnIds(table.columns()),
      table.partitioning,
      table.properties,
      table.constraints)
    tables.put(ident, nullColIdTable)
    nullColIdTable
  }
}

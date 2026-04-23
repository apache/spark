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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.connector.ColumnImpl

/**
 * An [[InMemoryTableCatalog]] that selectively strips column IDs based on
 * column names listed in [[MixedColumnIdTableCatalog.nullIdColumnNames]].
 * This simulates connectors that support column IDs for some columns but
 * not others, or that transition between supporting and not supporting
 * column IDs over time.
 *
 * Tests manipulate [[MixedColumnIdTableCatalog.nullIdColumnNames]] between
 * operations to control which columns have null IDs at any given point.
 * The set is snapshotted each time a table is created, altered, or copied.
 * Therefore, changes to the set after that point do not affect existing table instances.
 */
class MixedColumnIdTableCatalog extends InMemoryTableCatalog {

  private def toMixedIdTable(table: InMemoryTable): MixedColumnIdInMemoryTable = {
    val snapshot = MixedColumnIdTableCatalog.nullIdColumnNames.toSet
    val mixedTable = new MixedColumnIdInMemoryTable(
      name = table.name,
      columns = table.columns(),
      partitioning = table.partitioning,
      properties = table.properties,
      constraints = table.constraints,
      id = table.id,
      nullIdNames = snapshot)
    mixedTable.alterTableWithData(table.data, table.schema)
    mixedTable
  }

  override def createTable(
      ident: Identifier,
      info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val mixedTable = toMixedIdTable(table)
    tables.put(ident, mixedTable)
    mixedTable
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]
    val mixedTable = toMixedIdTable(table)
    tables.put(ident, mixedTable)
    mixedTable
  }
}

object MixedColumnIdTableCatalog {
  /** Column names (lowercase) whose IDs should be nullified. */
  val nullIdColumnNames: mutable.Set[String] = mutable.Set.empty

  def reset(): Unit = nullIdColumnNames.clear()
}

/**
 * An [[InMemoryTable]] that selectively strips column IDs for columns
 * whose names appear in the snapshotted [[nullIdNames]] set.
 */
class MixedColumnIdInMemoryTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: java.util.Map[String, String],
    constraints: Array[Constraint] = Array.empty,
    override val id: String = java.util.UUID.randomUUID().toString,
    nullIdNames: Set[String] = Set.empty)
  extends InMemoryTable(
    name = name,
    columns = columns,
    partitioning = partitioning,
    properties = properties,
    constraints = constraints,
    id = id) {

  override def columns(): Array[Column] = {
    super.columns().map { col =>
      val impl = col.asInstanceOf[ColumnImpl]
      if (nullIdNames.contains(impl.name.toLowerCase(Locale.ROOT))) {
        impl.copy(id = null)
      } else {
        impl
      }
    }
  }

  override def copy(): Table = {
    val copiedTable = new MixedColumnIdInMemoryTable(
      name,
      columns(),
      partitioning,
      properties,
      constraints,
      id,
      nullIdNames)
    dataMap.synchronized {
      copiedTable.alterTableWithData(data, schema)
    }
    copiedTable
  }
}

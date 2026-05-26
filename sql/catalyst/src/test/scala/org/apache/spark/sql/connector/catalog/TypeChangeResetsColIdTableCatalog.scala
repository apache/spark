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

import org.apache.spark.sql.internal.connector.ColumnImpl

/**
 * An [[InMemoryTableCatalog]] that assigns fresh column IDs when the
 * column's data type changes. This is the inverse of the default
 * [[InMemoryBaseTable.assignMissingIds]] behavior, which preserves IDs
 * across type changes.
 *
 * Use this catalog for tests that need a type change to produce a new
 * column ID (e.g., verifying that adding a nested field to a container
 * type triggers a column ID mismatch).
 */
class TypeChangeResetsColIdTableCatalog extends InMemoryTableCatalog {

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val oldColumns = loadTable(ident).columns()
    val alteredTable = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]

    // The parent alterTable preserves column IDs across type changes.
    // Assign fresh IDs for columns whose type changed, so the column ID
    // check detects the type change instead of the data columns validation.
    val oldColsByName = oldColumns
      .filter(_.id() != null)
      .map(oldCol => oldCol.name().toLowerCase(Locale.ROOT) -> oldCol)
      .toMap
    val newColsWithResetIds: Array[Column] = alteredTable.columns().map { newCol =>
      val key = newCol.name().toLowerCase(Locale.ROOT)
      oldColsByName.get(key) match {
        case Some(oldCol) if oldCol.dataType() != newCol.dataType() =>
          newCol.asInstanceOf[ColumnImpl].copy(
            id = InMemoryBaseTable.nextColumnId().toString): Column
        case _ =>
          newCol
      }
    }

    val tableWithResetIds = new InMemoryTable(
      alteredTable.name,
      newColsWithResetIds,
      alteredTable.partitioning,
      alteredTable.properties,
      alteredTable.constraints,
      id = alteredTable.id)
    tableWithResetIds.alterTableWithData(alteredTable.data, alteredTable.schema)
    tables.put(ident, tableWithResetIds)
    tableWithResetIds
  }
}

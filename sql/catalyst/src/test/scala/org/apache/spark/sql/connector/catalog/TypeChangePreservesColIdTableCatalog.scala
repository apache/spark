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
 * An [[InMemoryTableCatalog]] that preserves column IDs across type
 * changes, matching only by name. This simulates connectors that track
 * column identity independently of column type (e.g., a connector that
 * keeps the same column ID when a nested field is added to a struct).
 *
 * When the column ID stays the same but the type changes, the column
 * ID check passes and the schema validation ([[COLUMNS_MISMATCH]])
 * catches the incompatible change instead.
 */
class TypeChangePreservesColIdTableCatalog extends InMemoryTableCatalog {

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val oldColumns = loadTable(ident).columns()
    val alteredTable = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]

    // The parent alterTable assigns new column IDs when a column's type changes.
    // Preserve the old column IDs by matching on name, so the column ID check
    // passes and the [[validateDataColumns]] data columns validation catches the
    // incompatible type change instead.
    val oldIdsByName = oldColumns
      .filter(_.id() != null)
      .map(oldCol => oldCol.name().toLowerCase(Locale.ROOT) -> oldCol.id())
      .toMap
    val newColsWithPreservedIds = alteredTable.columns().map { newCol =>
      oldIdsByName.get(newCol.name().toLowerCase(Locale.ROOT)) match {
        case Some(oldId) =>
          newCol.asInstanceOf[ColumnImpl].copy(id = oldId)
        case None =>
          newCol
      }
    }

    val tableWithPreservedIds = new InMemoryTable(
      alteredTable.name,
      newColsWithPreservedIds,
      alteredTable.partitioning,
      alteredTable.properties,
      alteredTable.constraints,
      id = alteredTable.id)
    tableWithPreservedIds.alterTableWithData(alteredTable.data, alteredTable.schema)
    tables.put(ident, tableWithPreservedIds)
    tableWithPreservedIds
  }
}

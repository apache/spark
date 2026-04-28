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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.sql.internal.connector.ColumnImpl
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * An [[InMemoryTableCatalog]] that tracks IDs at every nesting level
 * (struct fields, array elements, map keys/values) and encodes the full
 * subtree into each top-level [[Column.id]] string.
 *
 * This demonstrates how a connector that wants to detect nested changes
 * can encode nested IDs into the top-level [[Column.id]] string.
 * Any nested change (drop+re-add a struct field, rename, etc.) produces a
 * different encoded top-level string, so [[V2TableUtil.validateColumnIds]]
 * detects it without Spark needing to traverse below the top level.
 *
 * Example: for a column `person STRUCT<name: STRING, age: INT>` with
 * root ID 5 and nested field IDs name=10, age=11, the composed
 * [[Column.id]] string is `"5[age:11,name:10]"`. If `age` is dropped
 * and re-added, the new age gets ID 12, producing `"5[age:12,name:10]"`.
 * Spark sees different strings and fires `COLUMN_ID_MISMATCH`.
 */
class ComposedColumnIdTableCatalog extends InMemoryTableCatalog {

  // Per-table nested ID maps.
  // Structure: tableIdentifier -> (columnName -> nestedFieldIdMap)
  // where nestedFieldIdMap maps a field path to its assigned ID.
  //
  // For column `person STRUCT<name: STRING, addr: STRUCT<city: STRING>>`:
  //   "person" -> {
  //     Seq("name") -> 10,
  //     Seq("addr") -> 11,
  //     Seq("addr", "city") -> 12
  //   }
  private val nestedIdMaps =
    new ConcurrentHashMap[Identifier, mutable.Map[String, mutable.Map[Seq[String], Long]]]()

  override def createTable(
      ident: Identifier, info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val allColumnNestedIds = mutable.Map[String, mutable.Map[Seq[String], Long]]()

    val composedColumns: Array[Column] = table.columns().map { column =>
      val nestedFieldIds = mutable.Map[Seq[String], Long]()
      assignNestedIds(column.dataType(), parentPath = Seq.empty, nestedFieldIds)
      val columnName = column.name().toLowerCase(Locale.ROOT)
      allColumnNestedIds(columnName) = nestedFieldIds
      val composedId = encodeComposedId(column.id(), nestedFieldIds)
      column.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, allColumnNestedIds)

    val composedTable = new InMemoryTable(
      table.name,
      composedColumns,
      table.partitioning,
      table.properties,
      table.constraints,
      id = table.id)
    composedTable.alterTableWithData(table.data, table.schema)
    tables.put(ident, composedTable)
    composedTable
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val oldTable = loadTable(ident).asInstanceOf[InMemoryTable]
    val oldColumnNestedIds = Option(nestedIdMaps.get(ident))
      .getOrElse(mutable.Map[String, mutable.Map[Seq[String], Long]]())

    val alteredTable = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]

    val allColumnNestedIds = mutable.Map[String, mutable.Map[Seq[String], Long]]()
    val composedColumns: Array[Column] = alteredTable.columns().map { newColumn =>
      val columnName = newColumn.name().toLowerCase(Locale.ROOT)
      val oldNestedFieldIds =
        oldColumnNestedIds.getOrElse(columnName, mutable.Map[Seq[String], Long]())

      // Find the old column to compare data types for merging nested IDs
      val oldColumnOpt = oldTable.columns()
        .find(oldCol => oldCol.name().toLowerCase(Locale.ROOT) == columnName)

      val newNestedFieldIds = oldColumnOpt match {
        case Some(oldColumn) =>
          // Column existed before: preserve IDs for paths that still exist,
          // assign fresh IDs for new paths (e.g. a re-added nested field)
          mergeNestedIds(oldNestedFieldIds, oldColumn.dataType(), newColumn.dataType())
        case None =>
          // Brand new column: assign fresh IDs to all nested positions
          val freshIds = mutable.Map[Seq[String], Long]()
          assignNestedIds(newColumn.dataType(), parentPath = Seq.empty, freshIds)
          freshIds
      }

      allColumnNestedIds(columnName) = newNestedFieldIds

      // The root ID (before composition) was already assigned by super.alterTable
      val rootId = alteredTable.columns()
        .find(col => col.name().toLowerCase(Locale.ROOT) == columnName)
        .map(_.id()).getOrElse(newColumn.id())
      val composedId = encodeComposedId(rootId, newNestedFieldIds)
      newColumn.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, allColumnNestedIds)

    val composedTable = new InMemoryTable(
      alteredTable.name,
      composedColumns,
      alteredTable.partitioning,
      alteredTable.properties,
      alteredTable.constraints,
      id = alteredTable.id)
    composedTable.alterTableWithData(alteredTable.data, alteredTable.schema)
    tables.put(ident, composedTable)
    composedTable
  }

  /**
   * Recursively assigns fresh IDs to every nested position in a data type:
   * struct fields, array elements, map keys, and map values.
   *
   * Each position is identified by a field path from the column root:
   *
   * `STRUCT<name: STRING, addr: STRUCT<city: STRING>>` produces:
   *   - Seq("name") -> id1
   *   - Seq("addr") -> id2
   *   - Seq("addr", "city") -> id3
   *
   * `ARRAY<STRUCT<x: INT>>` produces:
   *   - Seq("__element__") -> id1
   *   - Seq("__element__", "x") -> id2
   *
   * `MAP<STRING, STRUCT<v: INT>>` produces:
   *   - Seq("__key__") -> id1
   *   - Seq("__value__") -> id2
   *   - Seq("__value__", "v") -> id3
   */
  private def assignNestedIds(
      dataType: DataType,
      parentPath: Seq[String],
      nestedFieldIds: mutable.Map[Seq[String], Long]): Unit = {
    dataType match {
      case structType: StructType =>
        structType.fields.foreach { field =>
          val fieldPath = parentPath :+ field.name.toLowerCase(Locale.ROOT)
          nestedFieldIds(fieldPath) = InMemoryBaseTable.nextColumnId()
          assignNestedIds(field.dataType, fieldPath, nestedFieldIds)
        }
      case ArrayType(elementType, _) =>
        val elementPath = parentPath :+ "__element__"
        nestedFieldIds(elementPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(elementType, elementPath, nestedFieldIds)
      case MapType(keyType, valueType, _) =>
        val keyPath = parentPath :+ "__key__"
        nestedFieldIds(keyPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(keyType, keyPath, nestedFieldIds)
        val valuePath = parentPath :+ "__value__"
        nestedFieldIds(valuePath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(valueType, valuePath, nestedFieldIds)
      case _ => // primitive types have no nested structure
    }
  }

  /**
   * Merges nested IDs from old to new: preserves IDs for field paths that
   * exist in both old and new types, assigns fresh IDs for new field paths.
   *
   * For example, if the old type is `STRUCT<name: STRING, age: INT>` with
   * IDs {name->10, age->11}, and the new type is `STRUCT<name: STRING, age: INT>`
   * after drop+re-add of `age`, then `age` gets a fresh ID 12 because its
   * field path was removed and re-added, while `name` keeps ID 10.
   */
  private def mergeNestedIds(
      oldFieldIds: mutable.Map[Seq[String], Long],
      oldType: DataType,
      newType: DataType): mutable.Map[Seq[String], Long] = {
    val mergedFieldIds = mutable.Map[Seq[String], Long]()
    walkAndMerge(newType, parentPath = Seq.empty, mergedFieldIds, oldFieldIds)
    mergedFieldIds
  }

  /**
   * Walks the new data type and for each nested position, either preserves
   * the old ID (if the field path existed before) or assigns a fresh one.
   */
  private def walkAndMerge(
      dataType: DataType,
      parentPath: Seq[String],
      mergedFieldIds: mutable.Map[Seq[String], Long],
      oldFieldIds: mutable.Map[Seq[String], Long]): Unit = {
    dataType match {
      case structType: StructType =>
        structType.fields.foreach { field =>
          val fieldPath = parentPath :+ field.name.toLowerCase(Locale.ROOT)
          mergedFieldIds(fieldPath) =
            oldFieldIds.getOrElse(fieldPath, InMemoryBaseTable.nextColumnId())
          walkAndMerge(field.dataType, fieldPath, mergedFieldIds, oldFieldIds)
        }
      case ArrayType(elementType, _) =>
        val elementPath = parentPath :+ "__element__"
        mergedFieldIds(elementPath) =
          oldFieldIds.getOrElse(elementPath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(elementType, elementPath, mergedFieldIds, oldFieldIds)
      case MapType(keyType, valueType, _) =>
        val keyPath = parentPath :+ "__key__"
        mergedFieldIds(keyPath) =
          oldFieldIds.getOrElse(keyPath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(keyType, keyPath, mergedFieldIds, oldFieldIds)
        val valuePath = parentPath :+ "__value__"
        mergedFieldIds(valuePath) =
          oldFieldIds.getOrElse(valuePath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(valueType, valuePath, mergedFieldIds, oldFieldIds)
      case _ =>
    }
  }

  /**
   * Encodes a root ID and its nested field IDs into a single deterministic string.
   * Format: `rootId[fieldPath1:id1,fieldPath2:id2,...]` with paths sorted
   * lexicographically.
   *
   * Example: column `person STRUCT<name: STRING, age: INT>` with root ID "5"
   * and nested field IDs {Seq("name")->10, Seq("age")->11} encodes as:
   * `"5[age:11,name:10]"`
   *
   * If the column has no nested fields (e.g. `INT`), returns just the root ID.
   */
  private def encodeComposedId(
      rootId: String,
      nestedFieldIds: mutable.Map[Seq[String], Long]): String = {
    if (nestedFieldIds.isEmpty) {
      rootId
    } else {
      val sortedEntries = nestedFieldIds.toSeq.sortBy(_._1.mkString("."))
      val encoded = sortedEntries.map { case (fieldPath, fieldId) =>
        s"${fieldPath.mkString(".")}:$fieldId"
      }.mkString(",")
      s"$rootId[$encoded]"
    }
  }
}

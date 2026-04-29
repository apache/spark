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
 * Any nested change (drop+re-add a struct field, etc.) produces a
 * different encoded top-level string, so [[V2TableUtil.validateColumnIds]]
 * detects it without Spark needing to traverse below the top level.
 *
 * Nested positions are keyed by ordinal path (`Seq[Int]`), not by field
 * name. This matches Delta/Iceberg semantics where rename preserves the
 * column ID: a renamed field stays at the same ordinal position, so the
 * composed string is unchanged and schema validation catches the rename
 * via the differing [[StructType]].
 *
 * Example: for a column `person STRUCT<name: STRING, age: INT>` with
 * root ID 5 and nested field IDs position 0 (name) = 10,
 * position 1 (age) = 11, the composed [[Column.id]] string is
 * `"5[0:10,1:11]"`. If `age` is dropped and re-added, the new age gets
 * ID 12, producing `"5[0:10,1:12]"`. Spark sees different strings and
 * fires `COLUMN_ID_MISMATCH`.
 */
class ComposedColumnIdTableCatalog extends InMemoryTableCatalog {

  // Per-table nested ID maps.
  // Structure: tableIdentifier -> (columnName -> nestedFieldIdMap)
  // where nestedFieldIdMap maps an ordinal path to its assigned ID.
  //
  // For column `person STRUCT<name: STRING, addr: STRUCT<city: STRING>>`:
  //   "person" -> {
  //     Seq(0) -> 10,       // name
  //     Seq(1) -> 11,       // addr
  //     Seq(1, 0) -> 12     // addr.city
  //   }
  private val nestedIdMaps =
    new ConcurrentHashMap[Identifier, mutable.Map[String, mutable.Map[Seq[Int], Long]]]()

  // Bare (uncomposed) root IDs, tracked separately to avoid double-encoding.
  // Structure: tableIdentifier -> (columnName -> bareRootIdString)
  private val rootIds =
    new ConcurrentHashMap[Identifier, mutable.Map[String, String]]()

  override def createTable(
      ident: Identifier, info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val allColumnNestedIds = mutable.Map[String, mutable.Map[Seq[Int], Long]]()
    val allRootIds = mutable.Map[String, String]()

    val composedColumns: Array[Column] = table.columns().map { column =>
      val nestedFieldIds = mutable.Map[Seq[Int], Long]()
      assignNestedIds(column.dataType(), parentPath = Seq.empty, nestedFieldIds)
      val columnName = column.name().toLowerCase(Locale.ROOT)
      allColumnNestedIds(columnName) = nestedFieldIds
      allRootIds(columnName) = column.id()
      val composedId = encodeComposedId(column.id(), nestedFieldIds)
      column.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, allColumnNestedIds)
    rootIds.put(ident, allRootIds)

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
      .getOrElse(mutable.Map[String, mutable.Map[Seq[Int], Long]]())
    val oldRootIds = Option(rootIds.get(ident))
      .getOrElse(mutable.Map[String, String]())

    val alteredTable = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]

    val allColumnNestedIds = mutable.Map[String, mutable.Map[Seq[Int], Long]]()
    val allRootIds = mutable.Map[String, String]()
    val composedColumns: Array[Column] = alteredTable.columns().map { newColumn =>
      val columnName = newColumn.name().toLowerCase(Locale.ROOT)
      val oldNestedFieldIds =
        oldColumnNestedIds.getOrElse(columnName, mutable.Map[Seq[Int], Long]())

      // Find the old column to compare data types for merging nested IDs
      val oldColumnOpt = oldTable.columns()
        .find(oldCol => oldCol.name().toLowerCase(Locale.ROOT) == columnName)

      val newNestedFieldIds = oldColumnOpt match {
        case Some(oldColumn) =>
          // Column existed before: preserve IDs for positions that still exist,
          // assign fresh IDs for new positions (e.g. a re-added nested field)
          mergeNestedIds(oldNestedFieldIds, oldColumn.dataType(), newColumn.dataType())
        case None =>
          // Brand new column: assign fresh IDs to all nested positions
          val freshIds = mutable.Map[Seq[Int], Long]()
          assignNestedIds(newColumn.dataType(), parentPath = Seq.empty, freshIds)
          freshIds
      }

      allColumnNestedIds(columnName) = newNestedFieldIds

      // super.alterTable preserves IDs by name, so newColumn.id() is
      // the previously composed string (e.g. "5[0:10,1:11]"). Passing
      // that to encodeComposedId would produce "5[0:10,1:11][0:10,1:12]"
      // instead of "5[0:10,1:12]". Use the original root ID (e.g. "5")
      // from rootIds instead; fall back to newColumn.id() only for
      // genuinely new columns whose ID is a fresh numeric string.
      val rootId = oldRootIds.getOrElse(columnName, newColumn.id())
      allRootIds(columnName) = rootId
      val composedId = encodeComposedId(rootId, newNestedFieldIds)
      newColumn.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, allColumnNestedIds)
    rootIds.put(ident, allRootIds)

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
   * Each position is identified by an ordinal path from the column root:
   *
   * `STRUCT<name: STRING, addr: STRUCT<city: STRING>>` produces:
   *   - Seq(0) -> id1       (name, position 0)
   *   - Seq(1) -> id2       (addr, position 1)
   *   - Seq(1, 0) -> id3    (addr.city, position 0 within addr)
   *
   * `ARRAY<STRUCT<x: INT>>` produces:
   *   - Seq(0) -> id1       (element, position 0)
   *   - Seq(0, 0) -> id2    (element.x, position 0 within element)
   *
   * `MAP<STRING, STRUCT<v: INT>>` produces:
   *   - Seq(0) -> id1       (key, position 0)
   *   - Seq(1) -> id2       (value, position 1)
   *   - Seq(1, 0) -> id3    (value.v, position 0 within value)
   */
  private def assignNestedIds(
      dataType: DataType,
      parentPath: Seq[Int],
      nestedFieldIds: mutable.Map[Seq[Int], Long]): Unit = {
    dataType match {
      case structType: StructType =>
        structType.fields.zipWithIndex.foreach { case (field, idx) =>
          val fieldPath = parentPath :+ idx
          nestedFieldIds(fieldPath) = InMemoryBaseTable.nextColumnId()
          assignNestedIds(field.dataType, fieldPath, nestedFieldIds)
        }
      case ArrayType(elementType, _) =>
        val elementPath = parentPath :+ 0
        nestedFieldIds(elementPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(elementType, elementPath, nestedFieldIds)
      case MapType(keyType, valueType, _) =>
        val keyPath = parentPath :+ 0
        nestedFieldIds(keyPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(keyType, keyPath, nestedFieldIds)
        val valuePath = parentPath :+ 1
        nestedFieldIds(valuePath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(valueType, valuePath, nestedFieldIds)
      case _ => // primitive types have no nested structure
    }
  }

  /**
   * Merges nested IDs from old to new: preserves IDs for ordinal positions
   * that exist in both old and new types, assigns fresh IDs for new positions.
   *
   * For example, if the old type is `STRUCT<name: STRING, age: INT>` with
   * IDs {Seq(0)->10, Seq(1)->11}, and the new type is
   * `STRUCT<name: STRING, age: INT>` after drop+re-add of `age`, then `age`
   * gets a fresh ID 12 because its position was removed and re-added, while
   * `name` keeps ID 10.
   */
  private def mergeNestedIds(
      oldFieldIds: mutable.Map[Seq[Int], Long],
      oldType: DataType,
      newType: DataType): mutable.Map[Seq[Int], Long] = {
    val mergedFieldIds = mutable.Map[Seq[Int], Long]()
    walkAndMerge(newType, parentPath = Seq.empty, mergedFieldIds, oldFieldIds)
    mergedFieldIds
  }

  /**
   * Walks the new data type and for each nested position, either preserves
   * the old ID (if the ordinal path existed before) or assigns a fresh one.
   */
  private def walkAndMerge(
      dataType: DataType,
      parentPath: Seq[Int],
      mergedFieldIds: mutable.Map[Seq[Int], Long],
      oldFieldIds: mutable.Map[Seq[Int], Long]): Unit = {
    dataType match {
      case structType: StructType =>
        structType.fields.zipWithIndex.foreach { case (field, idx) =>
          val fieldPath = parentPath :+ idx
          mergedFieldIds(fieldPath) =
            oldFieldIds.getOrElse(fieldPath, InMemoryBaseTable.nextColumnId())
          walkAndMerge(field.dataType, fieldPath, mergedFieldIds, oldFieldIds)
        }
      case ArrayType(elementType, _) =>
        val elementPath = parentPath :+ 0
        mergedFieldIds(elementPath) =
          oldFieldIds.getOrElse(elementPath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(elementType, elementPath, mergedFieldIds, oldFieldIds)
      case MapType(keyType, valueType, _) =>
        val keyPath = parentPath :+ 0
        mergedFieldIds(keyPath) =
          oldFieldIds.getOrElse(keyPath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(keyType, keyPath, mergedFieldIds, oldFieldIds)
        val valuePath = parentPath :+ 1
        mergedFieldIds(valuePath) =
          oldFieldIds.getOrElse(valuePath, InMemoryBaseTable.nextColumnId())
        walkAndMerge(valueType, valuePath, mergedFieldIds, oldFieldIds)
      case _ =>
    }
  }

  /**
   * Encodes a root ID and its nested field IDs into a single deterministic string.
   * Format: `rootId[path1:id1,path2:id2,...]` with paths sorted
   * lexicographically by their dot-joined ordinal representation.
   *
   * Example: column `person STRUCT<name: STRING, age: INT>` with root ID "5"
   * and nested field IDs {Seq(0)->10, Seq(1)->11} encodes as:
   * `"5[0:10,1:11]"`
   *
   * If the column has no nested fields (e.g. `INT`), returns just the root ID.
   */
  private def encodeComposedId(
      rootId: String,
      nestedFieldIds: mutable.Map[Seq[Int], Long]): String = {
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

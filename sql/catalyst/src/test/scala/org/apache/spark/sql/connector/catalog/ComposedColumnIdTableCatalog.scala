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
 * This models the recommended adoption pattern for connectors with nested
 * IDs (e.g., Delta with column mapping, Iceberg with per-field IDs).
 * Any nested change (drop+re-add a struct field, rename, etc.) produces a
 * different encoded top-level string, so [[V2TableUtil.validateColumnIds]]
 * detects it without Spark needing to traverse below the top level.
 *
 * The encoded format is deterministic: the root ID followed by sorted
 * nested path:id pairs separated by commas.
 */
class ComposedColumnIdTableCatalog extends InMemoryTableCatalog {

  // Per-table nested ID maps: table identifier -> (column name -> nested ID map)
  // The nested ID map is: path (e.g. Seq("person", "age")) -> assigned ID
  private val nestedIdMaps =
    new ConcurrentHashMap[Identifier, mutable.Map[String, mutable.Map[Seq[String], Long]]]()

  override def createTable(
      ident: Identifier, info: TableInfo): Table = {
    val table = super.createTable(ident, info).asInstanceOf[InMemoryTable]
    val colNestedIds = mutable.Map[String, mutable.Map[Seq[String], Long]]()

    // Assign nested IDs for each column's data type
    val composedColumns: Array[Column] = table.columns().map { col =>
      val nestedIds = mutable.Map[Seq[String], Long]()
      assignNestedIds(col.dataType(), Seq.empty, nestedIds)
      val key = col.name().toLowerCase(Locale.ROOT)
      colNestedIds(key) = nestedIds
      val composedId = encodeComposedId(col.id(), nestedIds)
      col.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, colNestedIds)

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
    val oldColNestedIds = Option(nestedIdMaps.get(ident))
      .getOrElse(mutable.Map[String, mutable.Map[Seq[String], Long]]())

    val alteredTable = super.alterTable(ident, changes: _*).asInstanceOf[InMemoryTable]

    val colNestedIds = mutable.Map[String, mutable.Map[Seq[String], Long]]()
    val composedColumns: Array[Column] = alteredTable.columns().map { newCol =>
      val key = newCol.name().toLowerCase(Locale.ROOT)
      val oldNestedIds = oldColNestedIds.getOrElse(key, mutable.Map[Seq[String], Long]())

      // Find the old column to get its data type for merging
      val oldColOpt = oldTable.columns()
        .find(c => c.name().toLowerCase(Locale.ROOT) == key)

      val newNestedIds = oldColOpt match {
        case Some(oldCol) =>
          mergeNestedIds(oldNestedIds, oldCol.dataType(), newCol.dataType())
        case None =>
          val ids = mutable.Map[Seq[String], Long]()
          assignNestedIds(newCol.dataType(), Seq.empty, ids)
          ids
      }

      colNestedIds(key) = newNestedIds

      // Extract the root ID (the base ID assigned by assignMissingIds, before composition)
      // The alteredTable already has the root ID from super.alterTable
      val rootId = alteredTable.columns()
        .find(c => c.name().toLowerCase(Locale.ROOT) == key)
        .map(_.id()).getOrElse(newCol.id())
      val composedId = encodeComposedId(rootId, newNestedIds)
      newCol.asInstanceOf[ColumnImpl].copy(id = composedId): Column
    }

    nestedIdMaps.put(ident, colNestedIds)

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
   */
  private def assignNestedIds(
      dataType: DataType,
      pathPrefix: Seq[String],
      ids: mutable.Map[Seq[String], Long]): Unit = {
    dataType match {
      case st: StructType =>
        st.fields.foreach { field =>
          val path = pathPrefix :+ field.name.toLowerCase(Locale.ROOT)
          ids(path) = InMemoryBaseTable.nextColumnId()
          assignNestedIds(field.dataType, path, ids)
        }
      case ArrayType(elementType, _) =>
        val path = pathPrefix :+ "__element__"
        ids(path) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(elementType, path, ids)
      case MapType(keyType, valueType, _) =>
        val keyPath = pathPrefix :+ "__key__"
        ids(keyPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(keyType, keyPath, ids)
        val valPath = pathPrefix :+ "__value__"
        ids(valPath) = InMemoryBaseTable.nextColumnId()
        assignNestedIds(valueType, valPath, ids)
      case _ => // primitive types have no nested structure
    }
  }

  /**
   * Merges nested IDs from old to new: preserves IDs for paths that exist
   * in both old and new types, assigns fresh IDs for new paths.
   */
  private def mergeNestedIds(
      oldIds: mutable.Map[Seq[String], Long],
      oldType: DataType,
      newType: DataType): mutable.Map[Seq[String], Long] = {
    val newIds = mutable.Map[Seq[String], Long]()
    collectNestedPaths(newType, Seq.empty, newIds, oldIds)
    newIds
  }

  /**
   * Walks the new data type and for each nested position, either preserves
   * the old ID (if the path existed before) or assigns a fresh one.
   */
  private def collectNestedPaths(
      dataType: DataType,
      pathPrefix: Seq[String],
      newIds: mutable.Map[Seq[String], Long],
      oldIds: mutable.Map[Seq[String], Long]): Unit = {
    dataType match {
      case st: StructType =>
        st.fields.foreach { field =>
          val path = pathPrefix :+ field.name.toLowerCase(Locale.ROOT)
          newIds(path) = oldIds.getOrElse(path, InMemoryBaseTable.nextColumnId())
          collectNestedPaths(field.dataType, path, newIds, oldIds)
        }
      case ArrayType(elementType, _) =>
        val path = pathPrefix :+ "__element__"
        newIds(path) = oldIds.getOrElse(path, InMemoryBaseTable.nextColumnId())
        collectNestedPaths(elementType, path, newIds, oldIds)
      case MapType(keyType, valueType, _) =>
        val keyPath = pathPrefix :+ "__key__"
        newIds(keyPath) = oldIds.getOrElse(keyPath, InMemoryBaseTable.nextColumnId())
        collectNestedPaths(keyType, keyPath, newIds, oldIds)
        val valPath = pathPrefix :+ "__value__"
        newIds(valPath) = oldIds.getOrElse(valPath, InMemoryBaseTable.nextColumnId())
        collectNestedPaths(valueType, valPath, newIds, oldIds)
      case _ =>
    }
  }

  /**
   * Encodes a root ID and its nested IDs into a single deterministic string.
   * Format: rootId[path1:id1,path2:id2,...] with paths sorted lexicographically.
   */
  private def encodeComposedId(
      rootId: String,
      nestedIds: mutable.Map[Seq[String], Long]): String = {
    if (nestedIds.isEmpty) {
      rootId
    } else {
      val sorted = nestedIds.toSeq.sortBy(_._1.mkString("."))
      val encoded = sorted.map { case (path, id) =>
        s"${path.mkString(".")}:$id"
      }.mkString(",")
      s"$rootId[$encoded]"
    }
  }
}

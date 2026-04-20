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

import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
 * Mutable metadata column state for testing
 * METADATA_COLUMNS_MISMATCH validation.
 */
object MutableMetadataColumnTable {
  @volatile var currentMetadataColumns: Array[MetadataColumn] =
    defaultMetadataColumns

  def defaultMetadataColumns: Array[MetadataColumn] =
    Array(VersionColumn)

  def changedMetadataColumns: Array[MetadataColumn] =
    Array(ChangedVersionColumn)

  def reset(): Unit = {
    currentMetadataColumns = defaultMetadataColumns
  }

  object VersionColumn extends MetadataColumn {
    override def name: String = "_version"
    override def dataType: DataType = IntegerType
    override def isNullable: Boolean = false
    override def comment: String = "Row version"
  }

  object ChangedVersionColumn extends MetadataColumn {
    override def name: String = "_version"
    override def dataType: DataType = StringType
    override def isNullable: Boolean = false
    override def comment: String = "Row version changed"
  }
}

/**
 * Catalog whose tables report mutable metadata columns.
 * Does NOT use copyOnLoad -- the same table instance is
 * shared, but metadataColumns are read from the mutable
 * static field on each access. This lets tests change
 * metadata columns between loadTable calls.
 *
 * Configure WITHOUT copyOnLoad:
 *   spark.sql.catalog.metacat=...MutableMetadataColumnCatalog
 *   (no copyOnLoad setting)
 */
class MutableMetadataColumnCatalog extends InMemoryTableCatalog {
  // Override loadTable to return the SAME table instance
  // (no copy) but with dynamic metadata columns.
  // The table's metadataColumns getter reads from the
  // static mutable field each time.
  override def loadTable(ident: Identifier): Table = {
    val base = super.loadTable(ident)
    new TableWithMutableMetadata(base)
  }
}

/**
 * Wrapper that delegates everything to the base table but
 * overrides metadataColumns to read from the mutable static.
 * Intentionally does not implement SupportsWrite since this
 * is only used for read path metadata column validation tests.
 */
private class TableWithMutableMetadata(base: Table)
  extends Table
  with SupportsRead
  with SupportsMetadataColumns {

  override def name(): String = base.name()
  override def columns(): Array[Column] = base.columns()
  override def partitioning(): Array[
    org.apache.spark.sql.connector.expressions.Transform] =
    base.partitioning()
  override def properties(): java.util.Map[String, String] =
    base.properties()
  override def capabilities(): java.util.Set[TableCapability] =
    base.capabilities()

  override def metadataColumns: Array[MetadataColumn] =
    MutableMetadataColumnTable.currentMetadataColumns

  override def id: String = base match {
    case t: InMemoryTable => t.id
    case _ => null
  }

  override def version: String = base match {
    case t: InMemoryBaseTable => t.version()
    case _ => null
  }

  override def newScanBuilder(
      options: org.apache.spark.sql.util.CaseInsensitiveStringMap)
    : org.apache.spark.sql.connector.read.ScanBuilder =
    base.asInstanceOf[SupportsRead].newScanBuilder(options)
}

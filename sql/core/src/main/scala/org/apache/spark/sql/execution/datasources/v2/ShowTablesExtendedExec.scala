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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, ToPrettyString}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, StringUtils}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsPartitionManagement, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper

/**
 * Physical plan node for showing tables without partition, Show the information of tables.
 */
case class ShowTablesExtendedExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    namespace: Seq[String],
    pattern: String) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    // fetch tables
    // TODO We need a new listTable overload that takes a pattern string.
    val tables = catalog.listTables(namespace.toArray)
    tables.map { tableIdent =>
      if (StringUtils.filterPattern(Seq(tableIdent.name()), pattern).nonEmpty) {
        val table = catalog.loadTable(tableIdent)
        val information = getTableDetails(catalog.name, tableIdent, table)
        rows += toCatalystRow(tableIdent.namespace().quoted, tableIdent.name(), false,
          s"$information\n")
        }
      }

    // fetch temp views, includes: global temp view, local temp view
    val sessionCatalog = session.sessionState.catalog
    val db = namespace match {
      case Seq(db) => Some(db)
      case _ => None
    }
    val tempViews = sessionCatalog.listTempViews(db.getOrElse(""), pattern)
    tempViews.map { tempView =>
      val database = tempView.identifier.database.getOrElse("")
      val tableName = tempView.identifier.table
      val information = tempView.simpleString
      rows += toCatalystRow(database, tableName, true, s"$information\n")
    }

    rows.toSeq
  }

  private def getTableDetails(
      catalogName: String,
      identifier: Identifier,
      table: Table): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    results.put("Catalog", catalogName)
    results.put("Namespace", identifier.namespace().quoted)
    results.put("Table", identifier.name())
    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    results.put("Type", tableType.name)

    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          results.put(propKey.capitalize, table.properties.get(propKey))
        }
      })

    val properties =
      conf.redactOptions(table.properties.asScala.toMap).toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1).map {
        case (key, value) => key + "=" + value
      }.mkString("[", ",", "]")
    if (!table.properties().isEmpty) {
      results.put("Table Properties", properties.mkString("[", ", ", "]"))
    }

    // Partition Provider & Partition Columns
    if (table.supportsPartitions && table.asPartitionable.partitionSchema().nonEmpty) {
      results.put("Partition Provider", "Catalog")
      results.put("Partition Columns", table.asPartitionable.partitionSchema().map(
        field => quoteIdentifier(field.name)).mkString("[", ", ", "]"))
    }

    if (table.schema().nonEmpty) {
      results.put("Schema", table.schema().treeString)
    }

    results.map { case (key, value) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }
}

/**
 * Physical plan node for showing tables with partition, Show the information of partitions.
 */
case class ShowTablePartitionExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    tableIndent: Identifier,
    table: SupportsPartitionManagement,
    partSpec: ResolvedPartitionSpec) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    val information = getTablePartitionDetails(tableIndent,
      table, partSpec)
    rows += toCatalystRow(tableIndent.namespace.quoted,
      tableIndent.name(), false, s"$information\n")

    rows.toSeq
  }

  private def getTablePartitionDetails(
      tableIdent: Identifier,
      partitionTable: SupportsPartitionManagement,
      partSpec: ResolvedPartitionSpec): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    // "Partition Values"
    val partitionSchema = partitionTable.partitionSchema()
    val (names, ident) = (partSpec.names, partSpec.ident)
    val partitionIdentifiers = partitionTable.listPartitionIdentifiers(names.toArray, ident)
    if (partitionIdentifiers.isEmpty) {
      throw QueryCompilationErrors.notExistPartitionError(tableIdent, ident, partitionSchema)
    }
    assert(partitionIdentifiers.length == 1)
    val row = partitionIdentifiers.head
    val len = partitionSchema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    for (i <- 0 until len) {
      val dataType = partitionSchema(i).dataType
      val partValueUTF8String = ToPrettyString(Literal(row.get(i, dataType), dataType),
        Some(timeZoneId)).eval(null)
      val partValueStr = if (partValueUTF8String == null) "null" else partValueUTF8String.toString
      partitions(i) = escapePathName(partitionSchema(i).name) + "=" + escapePathName(partValueStr)
    }
    val partitionValues = partitions.mkString("[", ", ", "]")
    results.put("Partition Values", s"$partitionValues")

    // "Partition Parameters"
    val metadata = partitionTable.loadPartitionMetadata(ident)
    if (!metadata.isEmpty) {
      val metadataValues = metadata.asScala.map { case (key, value) =>
        if (value.isEmpty) key else s"$key: $value"
      }.mkString("{", ", ", "}")
      results.put("Partition Parameters", metadataValues)
    }

    // TODO "Created Time", "Last Access", "Partition Statistics"

    results.map { case (key, value) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }
}

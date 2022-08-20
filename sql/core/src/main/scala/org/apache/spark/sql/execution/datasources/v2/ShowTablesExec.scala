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

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvePartitionSpec.convertToPartIdent
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal}
import org.apache.spark.sql.catalyst.util.{StringUtils, quoteIdentifier}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsPartitionManagement, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.PartitioningUtils.{normalizePartitionSpec, requireExactMatchedPartitionSpec}

/**
 * Physical plan node for showing tables.
 */
case class ShowTablesExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    namespace: Seq[String],
    pattern: Option[String],
    isExtended: Boolean = false,
    partitionSpec: Option[TablePartitionSpec] = None) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    val identifiers = catalog.listTables(namespace.toArray)
    identifiers.map { identifier =>
      if (pattern.map(StringUtils.filterPattern(
        Seq(identifier.name()), _).nonEmpty).getOrElse(true)) {
        if (!isExtended) {
          rows += toCatalystRow(identifier.namespace().quoted, identifier.name(),
            isTempView(identifier))
        } else {
          val table = catalog.loadTable(identifier)
          if (!partitionSpec.isEmpty && table.isPartitionable) {
            // Show the information of partitions.
            rows += toCatalystRow(
              identifier.namespace().quoted,
              identifier.name(),
              isTempView(identifier),
              s"${extendedPartition(identifier, table.asPartitionable, partitionSpec)}")
          } else {
            // Show the information of tables.
            rows += toCatalystRow(
              identifier.namespace().quoted,
              identifier.name(),
              isTempView(identifier),
              s"${extendedTable(identifier, table)}")
          }
        }
      }
    }

    rows.toSeq
  }

  private def isTempView(ident: Identifier): Boolean = {
    catalog match {
      case s: V2SessionCatalog => s.isTempView(ident)
      case _ => false
    }
  }

  private def extendedTable(identifier: Identifier, table: Table): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    if (!identifier.namespace().isEmpty) {
      results.put("Namespace", identifier.namespace().quoted)
    }
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
    if (table.properties().isEmpty) {
      results.put("Table Properties", properties.mkString("[", ", ", "]"))
    }

    // Partition Provider & Partition Columns
    // TODO check
    if (table.isPartitionable && !table.asPartitionable.partitionSchema().isEmpty) {
      results.put("Partition Provider", "Catalog")
      results.put("Partition Columns", table.asPartitionable.partitionSchema().map(
        field => quoteIdentifier(field.name)).mkString(", "))
    }

    if (table.schema().nonEmpty) results.put("Schema", table.schema().treeString)

    results.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "")
  }

  private def extendedPartition(
      identifier: Identifier,
      partitionTable: SupportsPartitionManagement,
      partitionSpec: Option[TablePartitionSpec]): String = {
    val results = new mutable.LinkedHashMap[String, String]()

    // "Partition Values"
    val partitionSchema = partitionTable.partitionSchema()
    val normalizedSpec = normalizePartitionSpec(
      partitionSpec.get,
      partitionSchema,
      partitionTable.name(),
      conf.resolver)
    requireExactMatchedPartitionSpec(identifier.toString,
      normalizedSpec, partitionSchema.fieldNames)

    val partitionNames = normalizedSpec.keySet
    val (names, ident) = (partitionSchema.map(_.name),
      convertToPartIdent(normalizedSpec, partitionSchema))
    val partitionIdentifiers = partitionTable.listPartitionIdentifiers(names.toArray, ident)
    partitionIdentifiers.length match {
      case 0 =>
        throw QueryExecutionErrors.notExistPartitionError(
          identifier.toString, ident, partitionSchema)
      case len if (len > 1) =>
        throw QueryExecutionErrors.showTableExtendedMultiPartitionUnsupportedError(
          identifier.toString)
      case _ => // do nothing
    }
    val partitionIdentifier = partitionIdentifiers.head
    val len = partitionSchema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    var i = 0
    while (i < len) {
      val dataType = partitionSchema(i).dataType
      val partValueUTF8String =
        Cast(Literal(partitionIdentifier.get(i, dataType), dataType),
          StringType, Some(timeZoneId)).eval()
      val partValueStr = if (partValueUTF8String == null) "null" else partValueUTF8String.toString
      partitions(i) = escapePathName(partitionSchema(i).name) + "=" + escapePathName(partValueStr)
      i += 1
    }
    val partitionValues = partitions.mkString("[", ", ", "]")
    results.put("Partition Values", s"${partitionValues}")

    // "Partition Parameters"
    val metadata = partitionTable.loadPartitionMetadata(ident)
    if (!metadata.isEmpty) {
      val metadataValues = metadata.map { case ((key, value)) =>
        if (value.isEmpty) key else s"$key: $value"
      }.mkString("{", ", ", "}")
      results.put("Partition Parameters", metadataValues)
    }

    // TODO "Created Time", "Last Access", "Partition Statistics"

    results.map { case ((key, value)) =>
      if (value.isEmpty) key else s"$key: $value"
    }.mkString("", "\n", "\n")
  }
}

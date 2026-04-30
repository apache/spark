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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, SupportsMetadataColumns, SupportsRead, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, IdentityTransform}
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Schema + partitioning + clustering row formatting shared by `DescribeTableExec.run()` (which
 * uses it for the schema-row prefix) and `DescribeTablePartitionExec.run()` (which uses it as
 * the entire pre-partition section). Pulling these helpers into a trait lets the partition
 * exec call them directly off `this` -- the prior shape constructed a `DescribeTableExec`
 * inner instance just to invoke `addBaseDescription`, which forced threading unused
 * `catalogName` / `identifier` constructor args through the partition exec.
 */
private[v2] trait DescribeTableBaseRows extends LeafV2CommandExec {
  def table: Table

  /** Schema + partitioning + clustering rows, shared with DescribeTablePartitionExec. */
  protected def addBaseDescription(rows: ArrayBuffer[InternalRow]): Unit = {
    addSchema(rows)
    addPartitioning(rows)
    addClustering(rows)
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.columns().map{ column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.comment)
    }
  }

  private def addClusteringToRows(
      clusterBySpec: ClusterBySpec,
      rows: ArrayBuffer[InternalRow]): Unit = {
    rows += toCatalystRow("# Clustering Information", "", "")
    rows += toCatalystRow(s"# ${output.head.name}", output(1).name, output(2).name)
    rows ++= clusterBySpec.columnNames.map { fieldNames =>
      val schema = CatalogV2Util.v2ColumnsToStructType(table.columns())
      val nestedField = schema.findNestedField(fieldNames.fieldNames.toIndexedSeq)
      assert(nestedField.isDefined,
        "The clustering column " +
          s"${fieldNames.fieldNames.map(quoteIfNeeded).mkString(".")} " +
          s"was not found in the table schema ${schema.catalogString}.")
      nestedField.get
    }.map { case (path, field) =>
      toCatalystRow(
        (path :+ field.name).map(quoteIfNeeded).mkString("."),
        field.dataType.simpleString,
        field.getComment().orNull)
    }
  }

  private def addClustering(rows: ArrayBuffer[InternalRow]): Unit = {
    ClusterBySpec.extractClusterBySpec(table.partitioning.toIndexedSeq).foreach { clusterBySpec =>
      addClusteringToRows(clusterBySpec, rows)
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    // Clustering columns are handled in addClustering().
    val partitioning = table.partitioning
      .filter(t => !t.isInstanceOf[ClusterByTransform])
    if (partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        rows += toCatalystRow("# Partition Information", "", "")
        rows += toCatalystRow(s"# ${output(0).name}", output(1).name, output(2).name)
        val schema = CatalogV2Util.v2ColumnsToStructType(table.columns())
        rows ++= table.partitioning
          .map(_.asInstanceOf[IdentityTransform].ref.fieldNames())
          .map { fieldNames =>
            val nestedField = schema.findNestedField(fieldNames.toImmutableArraySeq)
            if (nestedField.isEmpty) {
              throw QueryExecutionErrors.partitionColumnNotFoundInTheTableSchemaError(
                fieldNames.toSeq,
                schema)
            }
            nestedField.get
          }.map { case (path, field) =>
            toCatalystRow(
              (path :+ field.name).map(quoteIfNeeded(_)).mkString("."),
              field.dataType.simpleString,
              field.getComment().orNull)
          }
      } else {
        rows += toCatalystRow("", "", "")
        rows += toCatalystRow("# Partitioning", "", "")
        rows ++= table.partitioning.zipWithIndex.map {
          case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
        }
      }
    }
  }
}

case class DescribeTableExec(
    output: Seq[Attribute],
    catalogName: String,
    identifier: Identifier,
    table: Table,
    isExtended: Boolean) extends DescribeTableBaseRows {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addBaseDescription(rows)

    if (isExtended) {
      addMetadataColumns(rows)
      addTableDetails(rows)
      addTableStats(rows)
      addTableConstraints(rows)
    }
    rows.toSeq
  }

  private def addTableDetails(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Table Information", "", "")
    rows += toCatalystRow("Catalog", catalogName, "")
    // The `Namespace` row is always emitted as the canonical v2 representation. Multi-segment
    // namespaces use `Identifier.namespace().quoted` so segments containing dots round-trip;
    // a zero-segment (root) namespace renders as an empty string -- intentional, so consumers
    // can distinguish "table at the catalog root" from a missing row.
    rows += toCatalystRow("Namespace", identifier.namespace().quoted, "")
    // For v1 compatibility, also emit a `Database` row when the namespace is exactly one
    // segment, mirroring v1 `CatalogTable.toJsonLinkedHashMap` which renders the `database`
    // part of `TableIdentifier` as `Database`. Multi-segment namespaces can't be rendered
    // as a single-string `database` losslessly, and a zero-segment (root) namespace has no
    // single-name representation, so the `Database` row is omitted in those cases and
    // consumers must read `Namespace` instead. The value is the raw segment (no
    // `quoteIfNeeded`), matching v1's `identifier.database.get`; consumers needing a
    // quoting-safe form should read `Namespace`, where `quoted` is applied per segment.
    if (identifier.namespace().length == 1) {
      rows += toCatalystRow("Database", identifier.namespace().head, "")
    }
    rows += toCatalystRow("Table", identifier.name(), "")

    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL.name
    } else {
      CatalogTableType.MANAGED.name
    }
    rows += toCatalystRow("Type", tableType, "")
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
        }
      })
    val properties =
      conf.redactOptions(table.properties.asScala.toMap).toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1).map {
        case (key, value) => key + "=" + value
      }.mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")

    // If any columns have default values, append them to the result.
    ResolveDefaultColumns.getDescribeMetadata(table.columns()).foreach { row =>
      rows += toCatalystRow(row._1, row._2, row._3)
    }
  }

  private def addTableConstraints(rows: ArrayBuffer[InternalRow]): Unit = {
    if (table.constraints.nonEmpty) {
      rows += emptyRow()
      rows += toCatalystRow("# Constraints", "", "")
      rows ++= table.constraints().map{ constraint =>
        toCatalystRow(constraint.name(), constraint.toDescription, "")
      }
    }
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= hasMeta.metadataColumns.map { column =>
        toCatalystRow(
          column.name,
          column.dataType.simpleString,
          Option(column.comment()).getOrElse(""))
      }
    case _ =>
  }

  private def addTableStats(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case read: SupportsRead =>
      read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
        case s: SupportsReportStatistics =>
          val stats = s.estimateStatistics()
          val statsComponents = Seq(
            Option.when(stats.sizeInBytes().isPresent)(s"${stats.sizeInBytes().getAsLong} bytes"),
            Option.when(stats.numRows().isPresent)(s"${stats.numRows().getAsLong} rows")
          ).flatten
          if (statsComponents.nonEmpty) {
            rows += toCatalystRow("Statistics", statsComponents.mkString(", "), null)
          }
        case _ =>
      }
    case _ =>
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}

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
 * Catalog / Namespace / Database / <entity> row formatting shared by
 * `DescribeTableExec.addTableDetails` and `DescribeV2ViewExec.run`. Hosting it in one place
 * keeps the row layout (including the v1-compat `Database` row) as a single source of truth
 * so the table and view paths can't drift.
 */
private[v2] trait DescribeIdentifierRows extends LeafV2CommandExec {
  /**
   * Append the structured identifier rows (`Catalog`, `Namespace`, `Database`,
   * `<entityLabel>`) to `rows`. `entityLabel` is `"Table"` for a v2 table and `"View"` for a
   * v2 view -- the only divergence between the two paths.
   *
   * Row shapes:
   *  - `Catalog` carries the catalog plugin name (always present for v2).
   *  - `Namespace` is the canonical multi-segment representation, joined with `.` and with
   *    `quoteIfNeeded` applied per segment (so segments containing dots round-trip). Always
   *    emitted; for an empty namespace (root-level entity) the value is the empty string,
   *    so the row's presence stays uniform across v2 outputs.
   *  - `Database` is always emitted for v1 compatibility. Its value is the trailing
   *    namespace segment (so multi-segment namespaces still surface their leaf segment),
   *    or the empty string when the namespace is the catalog root. Consumers that need
   *    the full namespace should read `Namespace`; `Database` alone is not round-trip-safe
   *    for multi-segment cases.
   *  - `<entityLabel>` is the unqualified entity name from `Identifier.name()`.
   */
  protected def addIdentifierRows(
      rows: ArrayBuffer[InternalRow],
      catalogName: String,
      identifier: Identifier,
      entityLabel: String): Unit = {
    rows += toCatalystRow("Catalog", catalogName, "")
    rows += toCatalystRow("Namespace", identifier.namespace().quoted, "")
    rows += toCatalystRow("Database", identifier.namespace().lastOption.getOrElse(""), "")
    rows += toCatalystRow(entityLabel, identifier.name(), "")
  }
}

/**
 * Schema + partitioning + clustering row formatting shared by `DescribeTableExec.run()` (which
 * uses it for the schema-row prefix) and `DescribeTablePartitionExec.run()` (which uses it as
 * the entire pre-partition section). Mixing the helpers into a trait lets each exec invoke
 * them directly off `this`, so the partition exec doesn't need to thread the table-only
 * `catalogName` / `identifier` arguments that `DescribeTableExec` consumes for the EXTENDED
 * `# Detailed Table Information` block.
 *
 * Kept orthogonal to [[DescribeIdentifierRows]] so `DescribeTablePartitionExec` (which only
 * needs the schema/partitioning rows) doesn't inherit identifier-row helpers it never calls.
 * `DescribeTableExec` mixes both traits in.
 */
private[v2] trait DescribeTableBaseRows extends LeafV2CommandExec {
  def table: Table

  /** A blank `("", "", "")` row used as a section separator in DESCRIBE output. */
  protected def emptyRow(): InternalRow = toCatalystRow("", "", "")

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
        rows += emptyRow()
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
    isExtended: Boolean) extends DescribeTableBaseRows with DescribeIdentifierRows {
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
    addIdentifierRows(rows, catalogName, identifier, entityLabel = "Table")

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
}

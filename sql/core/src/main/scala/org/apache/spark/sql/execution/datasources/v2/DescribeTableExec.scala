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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table, TableCatalog, V2TableUtil}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, IdentityTransform}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.util.ArrayImplicits._

case class DescribeTableExec(
    output: Seq[Attribute],
    table: Table,
    isExtended: Boolean) extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows)
    addPartitioning(rows)
    addClustering(rows)

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
    rows += toCatalystRow("Name", table.name(), "")

    val tableType = V2TableUtil.getTableType(table)
    rows += toCatalystRow("Type", tableType, "")
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
        }
      })
    val properties = V2TableUtil.extractProperties(table, conf.redactOptions(_))
      .map { case (key, value) => key + "=" + value }
      .mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")

    // If any columns have default values, append them to the result.
    ResolveDefaultColumns.getDescribeMetadata(table.columns()).foreach { row =>
      rows += toCatalystRow(row._1, row._2, row._3)
    }
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.columns().map{ column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.comment)
    }
  }

  private def addTableConstraints(rows: ArrayBuffer[InternalRow]): Unit = {
    val constraints = V2TableUtil.extractConstraints(table)
    if (constraints.nonEmpty) {
      rows += emptyRow()
      rows += toCatalystRow("# Constraints", "", "")
      rows ++= constraints.map { case (name, desc) =>
        toCatalystRow(name, desc, "")
      }
    }
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow]): Unit = {
    val metaCols = V2TableUtil.extractMetadataColumns(table)
    if (metaCols.nonEmpty) {
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= metaCols.map { case (name, dataType, _, comment) =>
        toCatalystRow(name, dataType.simpleString, comment.getOrElse(""))
      }
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

  private def addTableStats(rows: ArrayBuffer[InternalRow]): Unit = {
    val (sizeOpt, rowsOpt) = V2TableUtil.extractStatistics(table)
    val statsComponents = Seq(
      sizeOpt.map(s => s"$s bytes"),
      rowsOpt.map(r => s"$r rows")
    ).flatten
    if (statsComponents.nonEmpty) {
      rows += toCatalystRow("Statistics", statsComponents.mkString(", "), null)
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

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}

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
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, ToPrettyString}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsPartitionManagement}
import org.apache.spark.sql.errors.QueryCompilationErrors

case class DescribeTablePartitionExec(
    output: Seq[Attribute],
    table: SupportsPartitionManagement,
    tableIdent: Identifier,
    partSpec: ResolvedPartitionSpec,
    isExtended: Boolean) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    // Always validate partition existence, even for non-extended describe.
    validatePartitionExists()

    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows)
    addPartitioning(rows)

    if (isExtended) {
      addPartitionDetails(rows)
    }
    rows.toSeq
  }

  private def validatePartitionExists(): Unit = {
    val partitionSchema = table.partitionSchema()
    val (names, ident) = (partSpec.names, partSpec.ident)
    val partitionIdentifiers = table.listPartitionIdentifiers(names.toArray, ident)
    if (partitionIdentifiers.isEmpty) {
      throw QueryCompilationErrors.notExistPartitionError(tableIdent, ident, partitionSchema)
    }
  }

  private def addPartitionDetails(rows: ArrayBuffer[InternalRow]): Unit = {
    val partitionSchema = table.partitionSchema()
    val (names, ident) = (partSpec.names, partSpec.ident)

    // Re-list to obtain the canonical partition row for rendering.
    val partitionIdentifiers = table.listPartitionIdentifiers(names.toArray, ident)
    assert(partitionIdentifiers.length == 1)
    val row = partitionIdentifiers.head

    // Render partition values using ToPrettyString + escapePathName,
    // consistent with ShowTablePartitionExec.
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

    rows += emptyRow()
    rows += toCatalystRow("# Detailed Partition Information", "", "")
    rows += toCatalystRow("Partition Values", partitionValues, "")

    val metadata = table.loadPartitionMetadata(ident)
    metadata.asScala.toSeq.sortBy(_._1).foreach { case (k, v) =>
      rows += toCatalystRow(k, v, "")
    }
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.columns().map { column =>
      toCatalystRow(column.name, column.dataType.simpleString, column.comment)
    }
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Util
    import org.apache.spark.sql.connector.expressions.IdentityTransform
    import org.apache.spark.sql.catalyst.util.quoteIfNeeded

    val partitioning = table.partitioning
    if (partitioning.nonEmpty) {
      val partitionColumnsOnly = partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        rows += toCatalystRow("# Partition Information", "", "")
        rows += toCatalystRow(s"# ${output(0).name}", output(1).name, output(2).name)
        val schema = CatalogV2Util.v2ColumnsToStructType(table.columns())
        rows ++= partitioning
          .map(_.asInstanceOf[IdentityTransform].ref.fieldNames())
          .map { fieldNames =>
            val nestedField = schema.findNestedField(fieldNames.toIndexedSeq)
            nestedField.get
          }.map { case (path, field) =>
            toCatalystRow(
              (path :+ field.name).map(quoteIfNeeded(_)).mkString("."),
              field.dataType.simpleString,
              field.getComment().orNull)
          }
      }
    }
  }

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")
}

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
    val partitionRow = validateAndGetPartition()

    // Delegate schema + partitioning + clustering to DescribeTableExec.
    val rows = new ArrayBuffer[InternalRow]()
    DescribeTableExec(output, table, isExtended = false).addBaseDescription(rows)

    if (isExtended) {
      addPartitionDetails(rows, partitionRow)
    }
    rows.toSeq
  }

  private def validateAndGetPartition(): InternalRow = {
    val partitionSchema = table.partitionSchema()
    val (names, ident) = (partSpec.names, partSpec.ident)
    val partitionIdentifiers = table.listPartitionIdentifiers(names.toArray, ident)
    if (partitionIdentifiers.isEmpty) {
      throw QueryCompilationErrors.notExistPartitionError(tableIdent, ident, partitionSchema)
    }
    assert(partitionIdentifiers.length == 1)
    partitionIdentifiers.head
  }

  private def addPartitionDetails(
      rows: ArrayBuffer[InternalRow],
      partitionRow: InternalRow): Unit = {
    val partitionSchema = table.partitionSchema()

    // Render partition values using ToPrettyString + escapePathName,
    // consistent with ShowTablePartitionExec.
    val len = partitionSchema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    for (i <- 0 until len) {
      val dataType = partitionSchema(i).dataType
      val partValueUTF8String = ToPrettyString(Literal(partitionRow.get(i, dataType), dataType),
        Some(timeZoneId)).eval(null)
      val partValueStr = if (partValueUTF8String == null) "null" else partValueUTF8String.toString
      partitions(i) = escapePathName(partitionSchema(i).name) + "=" + escapePathName(partValueStr)
    }
    val partitionValues = partitions.mkString("[", ", ", "]")

    rows += toCatalystRow("", "", "")
    rows += toCatalystRow("# Detailed Partition Information", "", "")
    rows += toCatalystRow("Partition Values", partitionValues, "")

    val metadata = table.loadPartitionMetadata(partSpec.ident)
    metadata.asScala.toSeq.sortBy(_._1).foreach { case (k, v) =>
      rows += toCatalystRow(k, v, "")
    }
  }
}

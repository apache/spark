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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal}
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, TableCatalog}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for showing partitions.
 */
case class ShowPartitionsExec(
    output: Seq[Attribute],
    catalog: TableCatalog,
    table: SupportsPartitionManagement,
    partitionSpec: Option[ResolvedPartitionSpec]) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val (names, ident) = partitionSpec
      .map(spec => (spec.names, spec.ident))
      // listPartitionByNames() should return all partitions if the partition spec
      // does not specify any partition names.
      .getOrElse((Seq.empty[String], InternalRow.empty))
    val partitionIdentifiers = table.listPartitionIdentifiers(names.toArray, ident)
    // Converting partition identifiers as `InternalRow` of partition values,
    // for instance InternalRow(value0, value1, ..., valueN), to `InternalRow`s
    // with a string in the format: "col0=value0/col1=value1/.../colN=valueN".
    val schema = table.partitionSchema()
    val len = schema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    val output = partitionIdentifiers.map { row =>
      var i = 0
      while (i < len) {
        val dataType = schema(i).dataType
        val partValueUTF8String =
          Cast(Literal(row.get(i, dataType), dataType), StringType, Some(timeZoneId)).eval()
        val partValueStr = if (partValueUTF8String == null) "null" else partValueUTF8String.toString
        partitions(i) = escapePathName(schema(i).name) + "=" + escapePathName(partValueStr)
        i += 1
      }
      partitions.mkString("/")
    }
    output.sorted.map(p => InternalRow(UTF8String.fromString(p))).toImmutableArraySeq
  }
}

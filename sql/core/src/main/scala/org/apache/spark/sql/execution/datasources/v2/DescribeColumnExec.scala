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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class DescribeColumnExec(
    override val output: Seq[Attribute],
    column: Attribute,
    isExtended: Boolean,
    table: Table) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    val comment = if (column.metadata.contains("comment")) {
      column.metadata.getString("comment")
    } else {
      "NULL"
    }

    rows += toCatalystRow("col_name", column.name)
    rows += toCatalystRow("data_type",
      CharVarcharUtils.getRawType(column.metadata).getOrElse(column.dataType).catalogString)
    rows += toCatalystRow("comment", comment)

    if (isExtended) {
      val colStats = table match {
        case read: SupportsRead =>
          read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
            case s: SupportsReportStatistics =>
              val stats = s.estimateStatistics()
              Option(stats.columnStats().get(FieldReference.column(column.name)))
            case _ => None
          }
        case _ => None
      }

      if (colStats.nonEmpty) {
        if (colStats.get.min().isPresent) {
          rows += toCatalystRow("min", colStats.get.min().toString)
        } else {
          rows += toCatalystRow("min", "NULL")
        }

        if (colStats.get.max().isPresent) {
          rows += toCatalystRow("max", colStats.get.max().toString)
        } else {
          rows += toCatalystRow("max", "NULL")
        }

        if (colStats.get.nullCount().isPresent) {
          rows += toCatalystRow("num_nulls", colStats.get.nullCount().getAsLong.toString)
        } else {
          rows += toCatalystRow("num_nulls", "NULL")
        }

        if (colStats.get.distinctCount().isPresent) {
          rows += toCatalystRow("distinct_count", colStats.get.distinctCount().getAsLong.toString)
        } else {
          rows += toCatalystRow("distinct_count", "NULL")
        }

        if (colStats.get.avgLen().isPresent) {
          rows += toCatalystRow("avg_col_len", colStats.get.avgLen().getAsLong.toString)
        } else {
          rows += toCatalystRow("avg_col_len", "NULL")
        }

        if (colStats.get.maxLen().isPresent) {
          rows += toCatalystRow("max_col_len", colStats.get.maxLen().getAsLong.toString)
        } else {
          rows += toCatalystRow("max_col_len", "NULL")
        }
      }
    }

    rows.toSeq
  }
}

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

import org.apache.spark.sql.StatisticsCache
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Physical plan node for analyze table from a data source v2.
 */
case class AnalyzeTableExec(
    resolvedTable: ResolvedTable,
    statisticsCache: StatisticsCache) extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val colStats = resolvedTable.table match {
      case read: SupportsRead =>
        read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
          case s: SupportsReportStatistics =>
            s.estimateStatistics()
          case _ => throw QueryCompilationErrors.unsupportedTableOperationError(
            resolvedTable.catalog, resolvedTable.identifier, "ANALYZE TABLE")
        }
      case _ => throw QueryCompilationErrors.unsupportedTableOperationError(resolvedTable.catalog,
        resolvedTable.identifier, "ANALYZE TABLE")
    }
    statisticsCache.put(ResolvedIdentifier(resolvedTable.catalog,
      resolvedTable.identifier), colStats)
    Seq.empty
  }
  override def output: Seq[Attribute] = Seq.empty
}

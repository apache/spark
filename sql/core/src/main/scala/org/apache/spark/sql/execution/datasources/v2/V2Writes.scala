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

import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, Project, ReplaceData}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.connector.write.{LogicalWriteInfoImpl, SupportsDynamicOverwrite, SupportsOverwrite, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.sources.{MicroBatchWrite, WriteToMicroBatchDataSource}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * A rule that constructs logical writes.
 */
object V2Writes extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, None) =>
      val writeBuilder = newWriteBuilder(r.table, options, query.schema)
      val write = writeBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query)
      a.copy(write = Some(write), query = newQuery)

    case o @ OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, options, _, None) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).flatMap { pred =>
        val filter = DataSourceStrategy.translateFilter(pred, supportNestedPredicatePushdown = true)
        if (filter.isEmpty) {
          throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(pred)
        }
        filter
      }.toArray

      val table = r.table
      val writeBuilder = newWriteBuilder(table, options, query.schema)
      val write = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(filters) =>
          builder.truncate().build()
        case builder: SupportsOverwrite =>
          builder.overwrite(filters).build()
        case _ =>
          throw QueryExecutionErrors.overwriteTableByUnsupportedExpressionError(table)
      }

      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query)
      o.copy(write = Some(write), query = newQuery)

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, None) =>
      val table = r.table
      val writeBuilder = newWriteBuilder(table, options, query.schema)
      val write = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw QueryExecutionErrors.dynamicPartitionOverwriteUnsupportedByTableError(table)
      }
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query)
      o.copy(write = Some(write), query = newQuery)

    case WriteToMicroBatchDataSource(
        relation, table, query, queryId, writeOptions, outputMode, Some(batchId)) =>

      val writeBuilder = newWriteBuilder(table, writeOptions, query.schema, queryId)
      val write = buildWriteForMicroBatch(table, writeBuilder, outputMode)
      val microBatchWrite = new MicroBatchWrite(batchId, write.toStreaming)
      val customMetrics = write.supportedCustomMetrics.toSeq
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query)
      WriteToDataSourceV2(relation, microBatchWrite, newQuery, customMetrics)

    case rd @ ReplaceData(r: DataSourceV2Relation, _, query, _, None) =>
      val rowSchema = StructType.fromAttributes(rd.dataInput)
      val writeBuilder = newWriteBuilder(r.table, Map.empty, rowSchema)
      val write = writeBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query)
      // project away any metadata columns that could be used for distribution and ordering
      rd.copy(write = Some(write), query = Project(rd.dataInput, newQuery))

  }

  private def buildWriteForMicroBatch(
      table: SupportsWrite,
      writeBuilder: WriteBuilder,
      outputMode: OutputMode): Write = {

    outputMode match {
      case Append =>
        writeBuilder.build()
      case Complete =>
        // TODO: we should do this check earlier when we have capability API.
        require(writeBuilder.isInstanceOf[SupportsTruncate],
          table.name + " does not support Complete mode.")
        writeBuilder.asInstanceOf[SupportsTruncate].truncate().build()
      case Update =>
        require(writeBuilder.isInstanceOf[SupportsStreamingUpdateAsAppend],
          table.name + " does not support Update mode.")
        writeBuilder.asInstanceOf[SupportsStreamingUpdateAsAppend].build()
    }
  }

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }

  private def newWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      rowSchema: StructType,
      queryId: String = UUID.randomUUID().toString): WriteBuilder = {

    val info = LogicalWriteInfoImpl(queryId, rowSchema, writeOptions.asOptions)
    table.asWritable.newWriteBuilder(info)
  }
}

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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceData, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.write.{DeltaWriteBuilder, LogicalWriteInfoImpl, SupportsDynamicOverwrite, SupportsOverwriteV2, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.streaming.sources.{MicroBatchWrite, WriteToMicroBatchDataSource}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * A rule that constructs logical writes.
 */
object V2Writes extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, None, _) =>
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(r.table, writeOptions, query.schema)
      val write = writeBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      a.copy(write = Some(write), query = newQuery)

    case o @ OverwriteByExpression(
        r: DataSourceV2Relation, deleteExpr, query, options, _, None, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val predicates = splitConjunctivePredicates(deleteExpr).flatMap { pred =>
        val predicate = DataSourceV2Strategy.translateFilterV2(pred)
        if (predicate.isEmpty) {
          throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(pred)
        }
        predicate
      }.toArray

      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(table, writeOptions, query.schema)
      val write = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(predicates) =>
          builder.truncate().build()
        case builder: SupportsOverwriteV2 if builder.canOverwrite(predicates) =>
          builder.overwrite(predicates).build()
        case _ =>
          throw QueryExecutionErrors.overwriteTableByUnsupportedExpressionError(table)
      }

      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, None) =>
      val table = r.table
      val writeOptions = mergeOptions(options, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(table, writeOptions, query.schema)
      val write = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw QueryExecutionErrors.dynamicPartitionOverwriteUnsupportedByTableError(table)
      }
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      o.copy(write = Some(write), query = newQuery)

    case WriteToMicroBatchDataSource(
        relationOpt, table, query, queryId, options, outputMode, Some(batchId)) =>
      val writeOptions = mergeOptions(
        options,
        relationOpt.map(r => r.options.asCaseSensitiveMap.asScala.toMap).getOrElse(Map.empty))
      val writeBuilder = newWriteBuilder(table, writeOptions, query.schema, queryId = queryId)
      val write = buildWriteForMicroBatch(table, writeBuilder, outputMode)
      val microBatchWrite = new MicroBatchWrite(batchId, write.toStreaming)
      val customMetrics = write.supportedCustomMetrics.toImmutableArraySeq
      val funCatalogOpt = relationOpt.flatMap(_.funCatalog)
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, funCatalogOpt)
      WriteToDataSourceV2(relationOpt, microBatchWrite, newQuery, customMetrics)

    case rd @ ReplaceData(r: DataSourceV2Relation, _, query, _, projections, _, None) =>
      val rowSchema = projections.rowProjection.schema
      val metadataSchema = projections.metadataProjection.map(_.schema)
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val writeBuilder = newWriteBuilder(r.table, writeOptions, rowSchema, metadataSchema)
      val write = writeBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(write, query, r.funCatalog)
      rd.copy(write = Some(write), query = newQuery)

    case wd @ WriteDelta(r: DataSourceV2Relation, _, query, _, projections, None) =>
      val writeOptions = mergeOptions(Map.empty, r.options.asCaseSensitiveMap.asScala.toMap)
      val deltaWriteBuilder = newDeltaWriteBuilder(r.table, writeOptions, projections)
      val deltaWrite = deltaWriteBuilder.build()
      val newQuery = DistributionAndOrderingUtils.prepareQuery(deltaWrite, query, r.funCatalog)
      wd.copy(write = Some(deltaWrite), query = newQuery)
  }

  private def mergeOptions(
      commandOptions: Map[String, String],
      dsOptions: Map[String, String]): Map[String, String] = {
    // for DataFrame API cases, same options are carried by both Command and DataSourceV2Relation
    // for DataFrameV2 API cases, options are only carried by Command
    // for SQL cases, options are only carried by DataSourceV2Relation
    assert(commandOptions == dsOptions || commandOptions.isEmpty || dsOptions.isEmpty)
    commandOptions ++ dsOptions
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

  private def isTruncate(predicates: Array[Predicate]): Boolean = {
    predicates.length == 1 && predicates(0).name().equals("ALWAYS_TRUE")
  }

  private def newWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      rowSchema: StructType,
      metadataSchema: Option[StructType] = None,
      queryId: String = UUID.randomUUID().toString): WriteBuilder = {

    val info = LogicalWriteInfoImpl(
      queryId,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema = None,
      metadataSchema)
    table.asWritable.newWriteBuilder(info)
  }

  private def newDeltaWriteBuilder(
      table: Table,
      writeOptions: Map[String, String],
      projections: WriteDeltaProjections,
      queryId: String = UUID.randomUUID().toString): DeltaWriteBuilder = {

    val rowSchema = projections.rowProjection.map(_.schema).getOrElse(StructType(Nil))
    val rowIdSchema = Some(projections.rowIdProjection.schema)
    val metadataSchema = projections.metadataProjection.map(_.schema)

    val info = LogicalWriteInfoImpl(
      queryId,
      rowSchema,
      writeOptions.asOptions,
      rowIdSchema,
      metadataSchema)

    val writeBuilder = table.asWritable.newWriteBuilder(info)
    assert(writeBuilder.isInstanceOf[DeltaWriteBuilder], s"$writeBuilder must be DeltaWriteBuilder")
    writeBuilder.asInstanceOf[DeltaWriteBuilder]
  }
}

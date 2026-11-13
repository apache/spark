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

import org.apache.spark.sql.catalyst.analysis.ExternalLookupJoinEligibility
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AlterViewAs, AppendData, Command, CreateView, InsertOnlyMerge, Join, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.TreePattern.{COMMAND, DATA_SOURCE_V2_RELATION,
  PLAN_EXPRESSION}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{AlterViewAsCommand, CreateViewCommand}
import org.apache.spark.sql.execution.streaming.runtime.StreamingRelation
import org.apache.spark.sql.types.BooleanType

/**
 * Checks the capabilities of Data Source V2 tables, and fail problematic queries earlier.
 */
object TableCapabilityCheck extends (LogicalPlan => Unit) {
  import DataSourceV2Implicits._

  private def supportsBatchWrite(table: Table): Boolean = {
    table.supportsAny(BATCH_WRITE, V1_BATCH_WRITE)
  }

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      // A table that supports external lookup may be read by a lookup join formed later, e.g.
      // when a DataFrame over it is joined. In a plan that is not a command,
      // PrepareExternalLookupJoin rejects it when it remains a plain scan after lookup joins are
      // prepared. A command is checked here, below.
      case r: DataSourceV2Relation
          if !r.table.supports(BATCH_READ) && !r.table.supports(EXTERNAL_LOOKUP) =>
        throw QueryCompilationErrors.unsupportedBatchReadError(r.table)

      case r: StreamingRelationV2 if !r.table.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
        throw QueryCompilationErrors.unsupportedStreamingScanError(r.table)

      // TODO: check STREAMING_WRITE capability. It's not doable now because we don't have a
      //       a logical plan for streaming write.
      case AppendData(r: DataSourceV2Relation, _, _, _, _, _, _) if !supportsBatchWrite(r.table) =>
        throw QueryCompilationErrors.unsupportedAppendInBatchModeError(r.name)

      case InsertOnlyMerge(r: DataSourceV2Relation, _, _, _) if !supportsBatchWrite(r.table) =>
        throw QueryCompilationErrors.unsupportedAppendInBatchModeError(r.name)

      case OverwritePartitionsDynamic(r: DataSourceV2Relation, _, _, _, _, _)
        if !r.table.supports(BATCH_WRITE) || !r.table.supports(OVERWRITE_DYNAMIC) =>
        throw QueryCompilationErrors.unsupportedDynamicOverwriteInBatchModeError(r.table)

      case OverwriteByExpression(r: DataSourceV2Relation, expr, _, _, _, _, _, _) =>
        expr match {
          case Literal(true, BooleanType) =>
            if (!supportsBatchWrite(r.table) ||
                !r.table.supportsAny(TRUNCATE, OVERWRITE_BY_FILTER)) {
              throw QueryCompilationErrors.unsupportedTruncateInBatchModeError(r.table)
            }
          case _ =>
            if (!supportsBatchWrite(r.table) || !r.table.supports(OVERWRITE_BY_FILTER)) {
              throw QueryCompilationErrors.unsupportedOverwriteByFilterInBatchModeError(
               r.name)
            }
        }

      case _ => // OK
    }

    // A command may drop, truncate or replace its target before it optimizes its query, where
    // PrepareExternalLookupJoin would reject a lookup-only table that no lookup join reads. So a
    // command whose query reads one outside a lookup join fails here, during analysis. A view
    // definition stores its query without reading it, for a lookup join that reads the view.
    // Other plans, such as a query, hold no command, so they need no walk.
    if (plan.containsPattern(COMMAND)) {
      plan.foreach {
        case _: CreateViewCommand | _: AlterViewAsCommand | _: CreateView | _: AlterViewAs =>
        case command: Command => rejectLookupOnlyReads(command)
        case _ =>
      }
    }

    // The streaming sources in a query should all support micro-batch scan, or all support
    // continuous scan.
    val streamingSources = plan.collect {
      case r: StreamingRelationV2 => r.table
    }
    val v1StreamingRelations = plan.collect {
      case r: StreamingRelation => r
    }

    if (streamingSources.length + v1StreamingRelations.length > 1) {
      val allSupportsMicroBatch = streamingSources.forall(_.supports(MICRO_BATCH_READ))
      // v1 streaming data source only supports micro-batch.
      val allSupportsContinuous = streamingSources.forall(_.supports(CONTINUOUS_READ)) &&
        v1StreamingRelations.isEmpty
      if (!allSupportsMicroBatch && !allSupportsContinuous) {
        val microBatchSources =
          streamingSources.filter(_.supports(MICRO_BATCH_READ)).map(_.name()) ++
            v1StreamingRelations.map(_.sourceName)
        val continuousSources = streamingSources.filter(_.supports(CONTINUOUS_READ)).map(_.name())
        throw QueryCompilationErrors.streamingSourcesDoNotSupportCommonExecutionModeError(
          microBatchSources, continuousSources)
      }
    }
  }

  /**
   * Fails on a lookup-only table that `plan` reads outside a lookup join, including in the plans
   * that a node holds outside its children, as a V1 write command holds its query, and in
   * subqueries. A join over a lookup-only table is classified as for a lookup join, so a join that
   * cannot run fails with its named reason, and the lookup side of one that can is not a scan. The
   * walk enters only the plans that can read a DSv2 table, or hold a subquery that can.
   */
  private def rejectLookupOnlyReads(plan: LogicalPlan): Unit = {
    if (plan.containsAnyPattern(DATA_SOURCE_V2_RELATION, PLAN_EXPRESSION)) {
      plan match {
        case join: Join =>
          ExternalLookupJoinEligibility(join) match {
            case ExternalLookupJoinEligibility.Ineligible(error) => throw error
            // The query is analyzed in full, so the condition cannot be added later.
            case ExternalLookupJoinEligibility.MissingCondition =>
              throw QueryCompilationErrors.unsupportedExternalLookupJoinError(
                "MISSING_JOIN_CONDITION")
            case ExternalLookupJoinEligibility.NotLookupJoin =>
              join.children.foreach(rejectLookupOnlyReads)
            case ExternalLookupJoinEligibility.Eligible => rejectLookupOnlyReads(join.left)
          }
        case node =>
          ExternalLookupRelations.lookupOnlyTable(node).foreach { table =>
            throw QueryCompilationErrors.unsupportedBatchReadError(table)
          }
          node.children.foreach(rejectLookupOnlyReads)
      }
    }
    // A node's inner children are its subqueries unless it holds other plans, such as a V1 write
    // command's query, so each plan is walked once: the inner children, then the subqueries that
    // are not among them. Walking both would double the work at each level of nested subqueries.
    val innerPlans = plan.innerChildren.collect { case inner: LogicalPlan => inner }
    innerPlans.foreach(rejectLookupOnlyReads)
    if (plan.containsPattern(PLAN_EXPRESSION)) {
      plan.subqueries.filterNot(subquery => innerPlans.exists(_ eq subquery))
        .foreach(rejectLookupOnlyReads)
    }
  }
}

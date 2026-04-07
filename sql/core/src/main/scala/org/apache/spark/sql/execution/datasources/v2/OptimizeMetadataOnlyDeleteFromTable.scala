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

import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, DeleteFromTableWithFilters, LogicalPlan, ReplaceData, RowLevelWrite, WriteDelta}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{SupportsDeleteV2, TruncatableTable}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.util.ArrayImplicits._

/**
 * A rule that replaces a rewritten DELETE operation with a delete using filters if the data source
 * can handle this DELETE command without executing the plan that operates on individual or groups
 * of rows.
 *
 * Note this rule must be run after expression optimization but before scan planning.
 */
object OptimizeMetadataOnlyDeleteFromTable extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case RewrittenRowLevelCommand(rowLevelPlan, DELETE, cond, relation: DataSourceV2Relation) =>
      relation.table match {
        case table: SupportsDeleteV2 if !SubqueryExpression.hasSubquery(cond) =>
          val predicates = splitConjunctivePredicates(cond)
          val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
          val filtersOpt = tryTranslateToV2(normalizedPredicates)
          if (filtersOpt.exists(table.canDeleteWhere)) {
            logDebug(s"Switching to delete with filters: " +
              s"${filtersOpt.get.mkString("[", ", ", "]")}")
            DeleteFromTableWithFilters(relation, filtersOpt.get.toImmutableArraySeq)
          } else {
            tryDeleteWithPartitionPredicates(table, relation, normalizedPredicates)
              .getOrElse {
                logDebug(s"Falling back to row-level delete on ${relation.table.name()}")
                rowLevelPlan
              }
          }

        case _: TruncatableTable if cond == TrueLiteral =>
          DeleteFromTable(relation, cond)

        case _ =>
          rowLevelPlan
      }
  }

  /**
   * Attempts to convert partition-column filters to [[PartitionPredicate]]s and
   * combine them with translated V2 data filters for a metadata-only delete. (See SPARK-55596)
   *
   * Returns [[Some]] with the plan if the table accepts the combined predicates,
   * or [[None]] if partition predicates cannot be created or the table rejects them.
   */
  private def tryDeleteWithPartitionPredicates(
      table: SupportsDeleteV2,
      relation: DataSourceV2Relation,
      normalizedPredicates: Seq[Expression]): Option[LogicalPlan] = {
    for {
      partitionFields <- PushDownUtils.getPartitionPredicateSchema(relation)
      flattenedFilters = PushDownUtils.flattenNestedPartitionFilters(
        normalizedPredicates, partitionFields).keys.toSeq
      (candidatePredicates, remainingFilters) =
        PushDownUtils.createPartitionPredicates(flattenedFilters, partitionFields)
      // None if no partition predicates created
      partPredicates <- Option.when(candidatePredicates.nonEmpty)(candidatePredicates)
      // None if any remaining filter cannot be translated to V2
      dataV2Filters <- tryTranslateToV2(remainingFilters)
      combined = partPredicates.toArray ++ dataV2Filters
      if table.canDeleteWhere(combined)
    } yield {
      logDebug(s"Switching to delete with PartitionPredicate filters: " +
        s"${combined.mkString("[", ", ", "]")}")
      DeleteFromTableWithFilters(relation, combined.toImmutableArraySeq)
    }
  }

  /** Translates all expressions to V2 filters, or returns [[None]] if any fail. */
  private def tryTranslateToV2(predicates: Seq[Expression]): Option[Array[Predicate]] = {
    val filters = predicates.flatMap { p =>
      val filter = DataSourceV2Strategy.translateFilterV2(p)
      if (filter.isEmpty) {
        logDebug(s"Cannot translate expression to data source filter: $p")
      }
      filter
    }.toArray
    Option.when(filters.length == predicates.size)(filters)
  }

  private object RewrittenRowLevelCommand {
    type ReturnType = (RowLevelWrite, RowLevelOperation.Command, Expression, LogicalPlan)

    def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
      case rd @ ReplaceData(_, cond, _, originalTable, _, _, _) =>
        val command = rd.operation.command
        Some(rd, command, cond, originalTable)

      case wd @ WriteDelta(_, cond, _, originalTable, _, _) =>
        val command = wd.operation.command
        Some(wd, command, cond, originalTable)

      case _ =>
        None
    }
  }
}

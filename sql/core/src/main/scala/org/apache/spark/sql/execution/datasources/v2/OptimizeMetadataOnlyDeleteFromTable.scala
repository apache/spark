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
          val filters = toDataSourceV2Filters(normalizedPredicates)
          val allPredicatesTranslated = normalizedPredicates.size == filters.length
          if (allPredicatesTranslated && table.canDeleteWhere(filters)) {
            logDebug(s"Switching to delete with filters: ${filters.mkString("[", ", ", "]")}")
            DeleteFromTableWithFilters(relation, filters.toImmutableArraySeq)
          } else {
            rowLevelPlan
          }

        case _: TruncatableTable if cond == TrueLiteral =>
          DeleteFromTable(relation, cond)

        case _ =>
          rowLevelPlan
      }
  }

  private def toDataSourceV2Filters(predicates: Seq[Expression]): Array[Predicate] = {
    predicates.flatMap { p =>
      val filter = DataSourceV2Strategy.translateFilterV2(p)
      if (filter.isEmpty) {
        logDebug(s"Cannot translate expression to data source filter: $p")
      }
      filter
    }.toArray
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

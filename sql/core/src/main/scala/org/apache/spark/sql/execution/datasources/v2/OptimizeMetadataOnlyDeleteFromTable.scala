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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsDelete
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources

/**
 * Checks whether a metadata delete is possible and nullifies the rewrite plan if the source can
 * handle this delete without executing the rewrite plan.
 *
 * Note this rule must be run after expression optimization.
 */
object OptimizeMetadataOnlyDeleteFromTable extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeleteFromTable(relation: DataSourceV2Relation, cond, Some(_)) =>
      val deleteCond = cond.getOrElse(Literal.TrueLiteral)
      relation.table match {
        case table: SupportsDelete if !SubqueryExpression.hasSubquery(deleteCond) =>
          val predicates = splitConjunctivePredicates(deleteCond)
          val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
          val dataSourceFilters = toDataSourceFilters(normalizedPredicates)
          val allPredicatesTranslated = normalizedPredicates.size == dataSourceFilters.length
          if (allPredicatesTranslated && table.canDeleteWhere(dataSourceFilters)) {
            d.copy(rewritePlan = None)
          } else {
            d
          }
        case _ =>
          d
      }
  }

  protected def toDataSourceFilters(predicates: Seq[Expression]): Array[sources.Filter] = {
    predicates.flatMap { p =>
      val filter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (filter.isEmpty) {
        logWarning(s"Cannot translate expression to source filter: $p")
      }
      filter
    }.toArray
  }
}

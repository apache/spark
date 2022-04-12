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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.GroupBasedRowLevelOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReplaceData}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Filter}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter

/**
 * A rule that builds scans for group-based row-level operations.
 *
 * Note this rule must be run before [[V2ScanRelationPushDown]] as scans for group-based
 * row-level operations must be planned in a special way.
 */
object GroupBasedRowLevelOperationScanPlanning extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // push down the filter from the command condition instead of the filter in the rewrite plan,
    // which is negated for data sources that only support replacing groups of data (e.g. files)
    case GroupBasedRowLevelOperation(rd: ReplaceData, cond, relation: DataSourceV2Relation) =>
      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val (pushedFilters, remainingFilters) = pushFilters(cond, relation.output, scanBuilder)
      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.left.get.mkString(", ")
      } else {
        pushedFilters.right.get.mkString(", ")
      }

      val (scan, output) = PushDownUtils.pruneColumns(scanBuilder, relation, relation.output, Nil)

      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed filters: $pushedFiltersStr
           |Filters that were not pushed: ${remainingFilters.mkString(", ")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      // replace DataSourceV2Relation with DataSourceV2ScanRelation for the row operation table
      rd transform {
        case r: DataSourceV2Relation if r eq relation =>
          DataSourceV2ScanRelation(r, scan, PushDownUtils.toOutputAttrs(scan.readSchema(), r))
      }
  }

  private def pushFilters(
      cond: Expression,
      tableAttrs: Seq[AttributeReference],
      scanBuilder: ScanBuilder): (Either[Seq[Filter], Seq[V2Filter]], Seq[Expression]) = {

    val tableAttrSet = AttributeSet(tableAttrs)
    val filters = splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, tableAttrs)
    val (_, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    PushDownUtils.pushFilters(scanBuilder, normalizedFiltersWithoutSubquery)
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

object RowLevelCommandScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case RewrittenRowLevelCommand(command, relation: DataSourceV2Relation, rewritePlan) =>
      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val filters = command.condition.toSeq
      val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, relation.output)
      val (_, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      val (pushedFilters, remainingFilters) = PushDownUtils.pushFilters(
        scanBuilder, normalizedFiltersWithoutSubquery)
      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.left.get.mkString(", ")
      } else {
        pushedFilters.right.get.mkString(", ")
      }

      val (scan, output) = PushDownUtils.pruneColumns(
        scanBuilder, relation, relation.output, Seq.empty)

      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed filters: $pushedFiltersStr
           |Filters that were not pushed: ${remainingFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      // replace DataSourceV2Relation with DataSourceV2ScanRelation for the row operation table
      // there may be multiple scan relations for UPDATEs that rely on the UNION approach
      val newRewritePlan = rewritePlan transform {
        case r: DataSourceV2Relation if r.table eq table =>
          DataSourceV2ScanRelation(r, scan, PushDownUtils.toOutputAttrs(scan.readSchema(), r))
      }

      command.withNewRewritePlan(newRewritePlan)
  }
}

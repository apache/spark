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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.FunctionCatalog
import org.apache.spark.sql.connector.read.{SupportsReportOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, UnknownPartitioning}
import org.apache.spark.util.collection.Utils.sequenceToOption

/**
 * Extracts [[DataSourceV2ScanRelation]] from the input logical plan, converts any V2 partitioning
 * and ordering reported by data sources to their catalyst counterparts. Then, annotates the plan
 * with the partitioning and ordering result.
 */
object V2ScanPartitioningAndOrdering extends Rule[LogicalPlan] with SQLConfHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val scanRules = Seq[LogicalPlan => LogicalPlan] (partitioning, ordering)

    scanRules.foldLeft(plan) { (newPlan, scanRule) =>
      scanRule(newPlan)
    }
  }

  private def partitioning(plan: LogicalPlan) = plan.transformDown {
    case d @ DataSourceV2ScanRelation(relation, scan: SupportsReportPartitioning, _, None, _) =>
      val funCatalogOpt = relation.catalog.flatMap {
        case c: FunctionCatalog => Some(c)
        case _ => None
      }

      val catalystPartitioning = scan.outputPartitioning() match {
        case kgp: KeyGroupedPartitioning => sequenceToOption(kgp.keys().map(
          V2ExpressionUtils.toCatalystOpt(_, relation, funCatalogOpt)))
        case _: UnknownPartitioning => None
        case p => throw new IllegalArgumentException("Unsupported data source V2 partitioning " +
            "type: " + p.getClass.getSimpleName)
      }

      d.copy(keyGroupedPartitioning = catalystPartitioning)
  }

  private def ordering(plan: LogicalPlan) = plan.transformDown {
    case d @ DataSourceV2ScanRelation(relation, scan: SupportsReportOrdering, _, _, _) =>
      val ordering = V2ExpressionUtils.toCatalystOrdering(scan.outputOrdering(), relation)
      d.copy(ordering = Some(ordering))
  }
}

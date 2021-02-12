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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 *  Removes the filter nodes with dynamic pruning that were not pushed down to the scan.
 *  These nodes will not be pushed through projects and aggregates with non-deterministic
 *  expressions.
 */
object CleanupDynamicPruningFilters extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transform {
      // pass through anything that is pushed down into PhysicalOperation
      case p @ PhysicalOperation(_, _, LogicalRelation(_: HadoopFsRelation, _, _, _)) => p
      // remove any Filters with DynamicPruning that didn't get pushed down to PhysicalOperation.
      case f @ Filter(condition, _) =>
        val newCondition = condition.transform {
          case _: DynamicPruning => TrueLiteral
        }
        f.copy(condition = newCondition)
    }
  }
}

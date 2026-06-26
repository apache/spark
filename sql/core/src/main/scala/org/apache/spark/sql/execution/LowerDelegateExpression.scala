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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.DelegateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DELEGATE_EXPRESSION

/**
 * Strips every [[DelegateExpression]] down to its `definition`. Run on the optimized logical plan in
 * [[QueryExecution.createSparkPlan]] -- the single entry point to the planner, used by both the main
 * query and AQE re-planning -- so the planner and every physical consumer (join-key extraction,
 * V1 / cached-batch pushdown, columnar rules, codegen) sees the real executed expression rather than
 * the informational wrapper. Data source V2 pushdown runs earlier, in the logical optimizer, so it
 * unfolds the wrapper separately in `V2ExpressionBuilder`. The wrapper remains in the optimized
 * logical plan for EXPLAIN.
 */
object LowerDelegateExpression extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(_.containsPattern(DELEGATE_EXPRESSION)) {
      case d: DelegateExpression => d.definition
    }
}

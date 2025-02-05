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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{WINDOW, WINDOW_EXPRESSION}

/**
 * Remove window partition if partition expressions are foldable.
 */
object EliminateWindowPartitions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(WINDOW), ruleId) {
    case w @ Window(windowExprs, partitionSpec, _, _, _) if partitionSpec.exists(_.foldable) =>
      val newWindowExprs = windowExprs.map(_.transformWithPruning(
        _.containsPattern(WINDOW_EXPRESSION)) {
        case windowExpr @ WindowExpression(_, wsd @ WindowSpecDefinition(ps, _, _))
          if ps.exists(_.foldable) =>
          val newWsd = wsd.copy(partitionSpec = ps.filter(!_.foldable))
          windowExpr.copy(windowSpec = newWsd)
      }.asInstanceOf[NamedExpression])
      w.copy(windowExpressions = newWindowExprs, partitionSpec = partitionSpec.filter(!_.foldable))
  }
}

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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, DynamicPruningExpression, ListQuery, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{DYNAMIC_PRUNING_SUBQUERY, IN_SUBQUERY, SCALAR_SUBQUERY}
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.{InSubqueryExec, SparkPlan, SubqueryAdaptiveBroadcastExec, SubqueryExec}

case class PlanAdaptiveSubqueries(
    subqueryMap: Map[Long, SparkPlan]) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressionsWithPruning(
      _.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY, DYNAMIC_PRUNING_SUBQUERY)) {
      case expressions.ScalarSubquery(_, _, exprId, _, _, _, _, _) =>
        val subquery = SubqueryExec.createForScalarSubquery(
          s"subquery#${exprId.id}", subqueryMap(exprId.id))
        execution.ScalarSubquery(subquery, exprId)
      case expressions.InSubquery(values, ListQuery(_, _, exprId, _, _, _)) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          CreateNamedStruct(
            values.zipWithIndex.flatMap { case (v, index) =>
              Seq(Literal(s"col_$index"), v)
            }
          )
        }
        val subquery = SubqueryExec(s"subquery#${exprId.id}", subqueryMap(exprId.id))
        InSubqueryExec(expr, subquery, exprId, isDynamicPruning = false)
      case expressions.DynamicPruningSubquery(value, buildPlan,
          buildKeys, broadcastKeyIndices, onlyInBroadcast, exprId, _) =>
        val name = s"dynamicpruning#${exprId.id}"
        val subquery = SubqueryAdaptiveBroadcastExec(name, broadcastKeyIndices, onlyInBroadcast,
          buildPlan, buildKeys, subqueryMap(exprId.id))
        DynamicPruningExpression(InSubqueryExec(value, subquery, exprId))
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * Replaces logical [[AsOfJoin]] operator using a combination of Join and Aggregate operator.
 *
 * Input Pseudo-Query:
 * {{{
 *    SELECT * FROM left ASOF JOIN right ON (condition, as_of on(left.t, right.t), tolerance)
 * }}}
 *
 * Rewritten Query:
 * {{{
 *   SELECT left.*, __right__.*
 *   FROM (
 *        SELECT
 *             left.*,
 *             (
 *                  SELECT MIN_BY(STRUCT(right.*), left.t - right.t) AS __nearest_right__
 *                  FROM right
 *                  WHERE condition AND left.t >= right.t AND right.t >= left.t - tolerance
 *             ) as __right__
 *        FROM left
 *        )
 *   WHERE __right__ IS NOT NULL
 * }}}
 */
object RewriteAsOfJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
    case j @ AsOfJoin(left, right, asOfCondition, condition, joinType, orderExpression, _) =>
      val conditionWithOuterReference =
        condition.map(And(_, asOfCondition)).getOrElse(asOfCondition).transformUp {
          case a: AttributeReference if left.outputSet.contains(a) =>
            OuterReference(a)
      }
      val filtered = Filter(conditionWithOuterReference, right)

      val orderExpressionWithOuterReference = orderExpression.transformUp {
          case a: AttributeReference if left.outputSet.contains(a) =>
            OuterReference(a)
        }
      val rightStruct = CreateStruct(right.output)
      val nearestRight = MinBy(rightStruct, orderExpressionWithOuterReference)
        .toAggregateExpression()
      val aggExpr = Alias(nearestRight, "__nearest_right__")()
      val aggregate = Aggregate(Seq.empty, Seq(aggExpr), filtered)

      val projectWithScalarSubquery = Project(
        left.output :+ Alias(ScalarSubquery(aggregate, left.output), "__right__")(),
        left)

      val filterRight = joinType match {
        case LeftOuter => projectWithScalarSubquery
        case _ =>
          Filter(IsNotNull(projectWithScalarSubquery.output.last), projectWithScalarSubquery)
      }

      val project = Project(
        left.output ++ right.output.zipWithIndex.map {
          case (out, idx) =>
            Alias(GetStructField(filterRight.output.last, idx), out.name)()
        },
        filterRight)
      val attrMapping = j.output.zip(project.output)

      project -> attrMapping
  }
}

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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.AnyTimestampNanoType
import org.apache.spark.util.Utils

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
object RewriteAsOfJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithNewOutput {
      case j: AsOfJoin if !j.requiresSortMergeAsOfJoin =>
        val left = j.left
        val right = j.right
        val asOfCondition = j.asOfCondition
        val condition = j.condition
        val joinType = j.joinType
        val orderExpression = j.orderExpression.get
        val conditionWithOuterReference =
          condition.map(And(_, asOfCondition)).getOrElse(asOfCondition).transformUp {
            case a: AttributeReference if left.outputSet.contains(a) =>
              OuterReference(a)
        }
        val filtered = Filter(conditionWithOuterReference, right)

        val orderExpressionWithOuterReference = orderExpression.transformUp {
            // A nanosecond timestamp subtraction compares only epochMicros; rank by the exact
            // epoch-nanosecond difference instead.
            case s: SubtractTimestamps
                if s.children.forall(c => AnyTimestampNanoType.acceptsType(c.dataType)) =>
              Subtract(UnixNanos(s.left), UnixNanos(s.right))
        }.transformUp {
            case a: AttributeReference if left.outputSet.contains(a) =>
              OuterReference(a)
        }
        val rightStruct = CreateStruct(right.output)
        val nearestRight = MinBy(rightStruct, orderExpressionWithOuterReference)
          .toAggregateExpression()
        val aggExpr = Alias(nearestRight, "__nearest_right__")()
        val aggregate = Aggregate(Seq.empty, Seq(aggExpr), filtered)

        val projectWithCorrelatedSubquery = Project(
          left.output :+ Alias(ScalarSubquery(aggregate, left.output), "__right__")(),
          left)
        // When DecorrelateInnerQuery is disabled, two cases keep the legacy decorrelation and its
        // previous plan:
        // - A streaming left side reaches here only with
        //   spark.sql.legacy.asOfJoin.rewriteStreamingLeftToSubquery, which keeps the previous plan
        //   so that a query restarts from its checkpoint. The decorrelated plan would add stateful
        //   operators on the streaming side.
        // - With a constant left as-of key, when every correlated conjunct is an equality, a
        //   single-value IN or reads only left columns, the legacy decorrelation groups the right
        //   rows by the right columns these read. That is cheaper than the domain join of
        //   DecorrelateInnerQuery when one side of an equality is an expression such as
        //   `r.k + 1`, and right when that side maps right rows one-to-one. When it does not,
        //   such as `r.k % 10`, the rows stay as before.
        val keepsLegacyPlan = left.isStreaming ||
          (!orderExpressionWithOuterReference.exists(_.isInstanceOf[OuterReference]) &&
            splitConjunctivePredicates(conditionWithOuterReference).forall {
              case Equality(_, _) | In(_, Seq(_)) => true
              case e => !SubExprUtils.containsOuter(e) || !e.exists(_.isInstanceOf[Attribute])
            })
        val projectWithScalarSubquery = if (conf.decorrelateInnerQueryEnabled || keepsLegacyPlan) {
          projectWithCorrelatedSubquery
        } else {
          // Without DecorrelateInnerQuery, PullupCorrelatedPredicates only pulls correlated
          // predicates out of a Filter. It would leave the outer reference in the MIN_BY order
          // expression, and pull a non-equality predicate such as the as-of condition above the
          // Aggregate, which changes its result. So decorrelate this subquery as when it is
          // enabled: run the rules that do it then, with it enabled, on this Project alone. A
          // leaf stands in for `left`, so that other subqueries follow the configuration. The
          // later run of PullupCorrelatedPredicates keeps the result, as it has no outer
          // references left. Like the optimizer, skip an excluded
          // PullOutNestedDataOuterRefExpressions; PullupCorrelatedPredicates is non-excludable.
          val leftPlaceholder = LocalRelation(left.output)
          val host = projectWithCorrelatedSubquery.withNewChildren(Seq(leftPlaceholder))
          val pullOutExcluded = conf.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
            .contains(PullOutNestedDataOuterRefExpressions.ruleName)
          val decorrelationEnabled = conf.clone()
          decorrelationEnabled.setConf(SQLConf.DECORRELATE_INNER_QUERY_ENABLED, true)
          SQLConf.withExistingConf(decorrelationEnabled) {
            PullupCorrelatedPredicates(
              if (pullOutExcluded) host else PullOutNestedDataOuterRefExpressions(host))
          }.transformUp { case r: LocalRelation if r == leftPlaceholder => left }
        }

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
}

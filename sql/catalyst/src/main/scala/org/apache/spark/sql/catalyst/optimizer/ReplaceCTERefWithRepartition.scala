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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Replaces CTE references that have not been previously inlined with [[Repartition]] operations
 * which will then be planned as shuffles and reused across different reference points.
 *
 * Note that this rule should be called at the very end of the optimization phase to best guarantee
 * that CTE repartition shuffles are reused.
 */
object ReplaceCTERefWithRepartition extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case _: Subquery => plan
    case _ =>
      replaceWithRepartition(plan, mutable.HashMap.empty[Long, LogicalPlan])
  }

  private def replaceWithRepartition(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {
    case WithCTE(child, cteDefs) =>
      cteDefs.foreach { cteDef =>
        val inlined = replaceWithRepartition(cteDef.child, cteMap)
        val withRepartition =
          if (inlined.isInstanceOf[RepartitionOperation] || cteDef.underSubquery) {
            // If the CTE definition plan itself is a repartition operation or if it hosts a merged
            // scalar subquery, we do not need to add an extra repartition shuffle.
            inlined
          } else {
            Repartition(conf.numShufflePartitions, shuffle = true, inlined)
          }
        cteMap.put(cteDef.id, withRepartition)
      }
      replaceWithRepartition(child, cteMap)

    case ref: CTERelationRef =>
      val cteDefPlan = cteMap(ref.cteId)
      if (ref.outputSet == cteDefPlan.outputSet) {
        cteDefPlan
      } else {
        val ctePlan = DeduplicateRelations(
          Join(cteDefPlan, cteDefPlan, Inner, None, JoinHint(None, None))).children(1)
        val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
          Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
        }
        Project(projectList, ctePlan)
      }

    case _ if plan.containsPattern(CTE) =>
      plan
        .withNewChildren(plan.children.map(c => replaceWithRepartition(c, cteMap)))
        .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression =>
            e.withNewPlan(replaceWithRepartition(e.plan, cteMap))
        }

    case _ => plan
  }
}

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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.DeduplicateRelations
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf

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

  private def canSkipExtraRepartition(p: LogicalPlan): Boolean = p match {
    case _: RepartitionOperation => true
    case _: RebalancePartitions => true
    case _ => false
  }

  private def replaceWithRepartition(
      plan: LogicalPlan,
      cteMap: mutable.HashMap[Long, LogicalPlan]): LogicalPlan = plan match {
    case WithCTE(child, cteDefs) =>
      cteDefs.foreach { cteDef =>
        val inlined = replaceWithRepartition(cteDef.child, cteMap)
        // The sealing-oriented lowering (plan-reuse id, pinned partitioning) only runs when
        // guaranteed reuse is enabled; otherwise this rule must not change the plan shape (e.g.
        // add an extra shuffle), so turning the flag off fully rolls the feature back. Sealing also
        // requires `forceSkipInline`: a pin on a non-forceSkipInline definition is ignored because
        // such a definition may be inlined by `InlineCTE` before this rule runs.
        val sealForReuse = cteDef.forceSkipInline &&
          SQLConf.get.getConf(SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE)
        val withRepartition = (sealForReuse, cteDef.forcePartitioning) match {
          case (true, Some(h: HashPartitioning)) =>
            // A pinned partitioning materializes the CTE as a plan-reuse RepartitionByExpression
            // on the pinned expressions, so guaranteed CTE reuse can pick it up. It takes
            // precedence over `canSkipExtraRepartition` -- the pin states the exact partitioning
            // (keys and number of partitions) we must produce.
            RepartitionByExpression(
              h.expressions, inlined, optNumPartitions = Some(h.numPartitions))
              .addRepartitionId(reassign = true)
          case (true, Some(other)) =>
            // forcePartitioning is an internal dev API; only HashPartitioning is supported
            // today (see CTERelationDef.forcePartitioning).
            throw new UnsupportedOperationException(
              s"CTERelationDef.forcePartitioning supports only HashPartitioning, but got " +
                s"${other.getClass.getSimpleName}")
          case (true, None) =>
            // A forced CTE must always carry the plan-reuse identity so every reference seals into
            // a CTEReuseRelation and materializes once, whatever the body's root shape. Stamp an
            // existing reusable repartition in place (no extra shuffle); wrap any other root -- a
            // plain repartition, a rebalance, or a merged-subquery body -- in a plan-reuse
            // RepartitionByExpression.
            inlined match {
              case r: PlanReusableRepartition => r.addRepartitionId()
              case _ =>
                RepartitionByExpression(Seq.empty, inlined, None)
                  .addRepartitionId(reassign = true)
            }
          case _ =>
            // Not sealing (a non-forceSkipInline CTE, or the reuse flag is off): keep the original
            // OSS behavior -- a plain repartition that is not sealed for reuse (no repartitionId),
            // and no extra shuffle when the body already repartitions or hosts a merged subquery.
            // Any forcePartitioning pin is ignored in this branch.
            if (canSkipExtraRepartition(inlined) || cteDef.underSubquery) {
              inlined
            } else {
              RepartitionByExpression(Seq.empty, inlined, None)
            }
        }
        cteMap.put(cteDef.id, withRepartition)
      }
      replaceWithRepartition(child, cteMap)

    case ref: CTERelationRef =>
      val cteDefPlan = cteMap.getOrElse(ref.cteId,
        throw SparkException.internalError(
          s"No CTERelationDef found for CTERelationRef(cteId=${ref.cteId})."))

      if (ref.outputSet == cteDefPlan.outputSet) {
        cteDefPlan
      } else {
        val ctePlan = DeduplicateRelations.deduplicateRight(cteDefPlan, cteDefPlan)
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

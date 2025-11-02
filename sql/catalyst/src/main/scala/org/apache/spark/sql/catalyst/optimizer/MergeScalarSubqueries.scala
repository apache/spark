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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * This rule tries to merge multiple non-correlated [[ScalarSubquery]]s to compute multiple scalar
 * values once.
 *
 * The process is the following:
 * - While traversing through the plan each [[ScalarSubquery]] plan is tried to merge into already
 *   seen subquery plans using `PlanMerger`s.
 *   During this first traversal each [[ScalarSubquery]] expression is replaced to a temporal
 *   [[ScalarSubqueryReference]] pointing to its possible merged version stored in `PlanMerger`s.
 *   `PlanMerger`s keep track of whether a plan is a result of merging 2 or more plans, or is an
 *   original unmerged plan. [[ScalarSubqueryReference]]s contain all the required information to
 *   either restore the original [[ScalarSubquery]] or create a reference to a merged CTE.
 * - Once the first traversal is complete and all possible merging have been done a second traversal
 *   removes the [[ScalarSubqueryReference]]s to either restore the original [[ScalarSubquery]] or
 *   to replace the original to a modified one that references a CTE with a merged plan.
 *   A modified [[ScalarSubquery]] is constructed like:
 *   `GetStructField(ScalarSubquery(CTERelationRef(...)), outputIndex)` where `outputIndex` is the
 *   index of the output attribute (of the CTE) that corresponds to the output of the original
 *   subquery.
 * - If there are merged subqueries in `PlanMerger`s then a `WithCTE` node is built from these
 *   queries. The `CTERelationDef` nodes contain the merged subquery in the following form:
 *   `Project(Seq(CreateNamedStruct(name1, attribute1, ...) AS mergedValue), mergedSubqueryPlan)`.
 *   The definitions are flagged that they host a subquery, that can return maximum one row.
 *
 * Eg. the following query:
 *
 * SELECT
 *   (SELECT avg(a) FROM t),
 *   (SELECT sum(b) FROM t)
 *
 * is optimized from:
 *
 * == Optimized Logical Plan ==
 * Project [scalar-subquery#242 [] AS scalarsubquery()#253,
 *          scalar-subquery#243 [] AS scalarsubquery()#254L]
 * :  :- Aggregate [avg(a#244) AS avg(a)#247]
 * :  :  +- Project [a#244]
 * :  :     +- Relation default.t[a#244,b#245] parquet
 * :  +- Aggregate [sum(a#251) AS sum(a)#250L]
 * :     +- Project [a#251]
 * :        +- Relation default.t[a#251,b#252] parquet
 * +- OneRowRelation
 *
 * to:
 *
 * == Optimized Logical Plan ==
 * Project [scalar-subquery#242 [].avg(a) AS scalarsubquery()#253,
 *          scalar-subquery#243 [].sum(a) AS scalarsubquery()#254L]
 * :  :- Project [named_struct(avg(a), avg(a)#247, sum(a), sum(a)#250L) AS mergedValue#260]
 * :  :  +- Aggregate [avg(a#244) AS avg(a)#247, sum(a#244) AS sum(a)#250L]
 * :  :     +- Project [a#244]
 * :  :        +- Relation default.t[a#244,b#245] parquet
 * :  +- Project [named_struct(avg(a), avg(a)#247, sum(a), sum(a)#250L) AS mergedValue#260]
 * :     +- Aggregate [avg(a#244) AS avg(a)#247, sum(a#244) AS sum(a)#250L]
 * :        +- Project [a#244]
 * :           +- Relation default.t[a#244,b#245] parquet
 * +- OneRowRelation
 *
 * == Physical Plan ==
 *  *(1) Project [Subquery scalar-subquery#242, [id=#125].avg(a) AS scalarsubquery()#253,
 *                ReusedSubquery
 *                  Subquery scalar-subquery#242, [id=#125].sum(a) AS scalarsubquery()#254L]
 * :  :- Subquery scalar-subquery#242, [id=#125]
 * :  :  +- *(2) Project [named_struct(avg(a), avg(a)#247, sum(a), sum(a)#250L) AS mergedValue#260]
 * :  :     +- *(2) HashAggregate(keys=[], functions=[avg(a#244), sum(a#244)],
 *                                output=[avg(a)#247, sum(a)#250L])
 * :  :        +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#120]
 * :  :           +- *(1) HashAggregate(keys=[], functions=[partial_avg(a#244), partial_sum(a#244)],
 *                                      output=[sum#262, count#263L, sum#264L])
 * :  :              +- *(1) ColumnarToRow
 * :  :                 +- FileScan parquet default.t[a#244] ...
 * :  +- ReusedSubquery Subquery scalar-subquery#242, [id=#125]
 * +- *(1) Scan OneRowRelation[]
 */
object MergeScalarSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // Subquery reuse needs to be enabled for this optimization.
      case _ if !conf.getConf(SQLConf.SUBQUERY_REUSE_ENABLED) => plan

      // This rule does a whole plan traversal, no need to run on subqueries.
      case _: Subquery => plan

      // Plans with CTEs are not supported for now.
      case _: WithCTE => plan

      case _ => extractCommonScalarSubqueries(plan)
    }
  }

  private def extractCommonScalarSubqueries(plan: LogicalPlan) = {
    // Collect `ScalarSubquery` plans by level into `PlanMerger`s and insert references in place of
    // `ScalarSubquery`s.
    val planMergers = ArrayBuffer.empty[PlanMerger]
    val planWithReferences = insertReferences(plan, planMergers)._1

    // Traverse level by level and convert merged plans to `CTERelationDef`s and keep non-merged
    // ones. While traversing replace references in plans back to `CTERelationRef`s or to original
    // `ScalarSubquery`s. This is safe as a subquery plan at a level can reference only lower level
    // other subqueries.
    val subqueryPlansByLevel = ArrayBuffer.empty[IndexedSeq[LogicalPlan]]
    planMergers.foreach { planMerger =>
      val mergedPlans = planMerger.mergedPlans()
      subqueryPlansByLevel += mergedPlans.map { mergedPlan =>
        val planWithoutReferences = if (subqueryPlansByLevel.isEmpty) {
          // Level 0 plans can't contain references
          mergedPlan.plan
        } else {
          removeReferences(mergedPlan.plan, subqueryPlansByLevel)
        }
        if (mergedPlan.merged) {
          CTERelationDef(
            Project(
              Seq(Alias(
                CreateNamedStruct(
                  planWithoutReferences.output.flatMap(a => Seq(Literal(a.name), a))),
                "mergedValue")()),
              planWithoutReferences),
            underSubquery = true)
        } else {
          planWithoutReferences
        }
      }
    }

    // Replace references back to `CTERelationRef`s or to original `ScalarSubquery`s in the main
    // plan.
    val newPlan = removeReferences(planWithReferences, subqueryPlansByLevel)

    // Add `CTERelationDef`s to the plan.
    val subqueryCTEs = subqueryPlansByLevel.flatMap(_.collect { case cte: CTERelationDef => cte })
    if (subqueryCTEs.nonEmpty) {
      WithCTE(newPlan, subqueryCTEs.toSeq)
    } else {
      newPlan
    }
  }

  // First traversal inserts `ScalarSubqueryReference`s to the plan and tries to merge subquery
  // plans by each level.
  private def insertReferences(
      plan: LogicalPlan,
      planMergers: ArrayBuffer[PlanMerger]): (LogicalPlan, Int) = {
    // The level of a subquery plan is maximum level of its inner subqueries + 1 or 0 if it has no
    // inner subqueries.
    var maxLevel = 0
    val planWithReferences =
      plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
        case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
          val (planWithReferences, level) = insertReferences(s.plan, planMergers)

          while (level >= planMergers.size) planMergers += PlanMerger()
          // The subquery could contain a hint that is not propagated once we merge it, but as a
          // non-correlated scalar subquery won't be turned into a Join the loss of hints is fine.
          val planMergeResult = planMergers(level).merge(planWithReferences)

          maxLevel = maxLevel.max(level + 1)

          val mergedOutput = planMergeResult.outputMap(planWithReferences.output.head)
          val headerIndex =
            planMergeResult.mergedPlan.output.indexWhere(_.exprId == mergedOutput.exprId)
          ScalarSubqueryReference(
            level,
            planMergeResult.mergedPlanIndex,
            headerIndex,
            s.dataType,
            s.exprId)
        case o => o
      }
    (planWithReferences, maxLevel)
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(CTERelationRef to the merged plan)` if the plan is merged from
  // multiple subqueries or `ScalarSubquery(original plan)` if it isn't.
  private def removeReferences(
      plan: LogicalPlan,
      subqueryPlansByLevel: ArrayBuffer[IndexedSeq[LogicalPlan]]) = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
      case ssr: ScalarSubqueryReference =>
        subqueryPlansByLevel(ssr.level)(ssr.mergedPlanIndex) match {
          case cte: CTERelationDef =>
            GetStructField(
              ScalarSubquery(
                CTERelationRef(cte.id, _resolved = true, cte.output, cte.isStreaming),
                exprId = ssr.exprId),
              ssr.outputIndex)
          case o => ScalarSubquery(o, exprId = ssr.exprId)
        }
    }
  }
}

/**
 * Temporal reference to a subquery which is added to a `PlanMerger`.
 *
 * @param level The level of the replaced subquery. It defines the `PlanMerger` instance into which
 *              the subquery is merged.
 * @param mergedPlanIndex The index of the merged plan in the `PlanMerger`.
 * @param outputIndex The index of the output attribute of the merged plan.
 * @param dataType The dataType of original scalar subquery.
 * @param exprId The expression id of the original scalar subquery.
 */
case class ScalarSubqueryReference(
    level: Int,
    mergedPlanIndex: Int,
    outputIndex: Int,
    dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)
}

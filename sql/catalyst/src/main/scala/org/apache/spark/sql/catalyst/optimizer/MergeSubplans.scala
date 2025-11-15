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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, LeafNode, LogicalPlan, OneRowRelation, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, NO_GROUPING_AGGREGATE_REFERENCE, SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
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
object MergeSubplans extends Rule[LogicalPlan] {
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
    // Collect subplans by level into `PlanMerger`s and insert references in place of them.
    val planMergers = ArrayBuffer.empty[PlanMerger]
    val planWithReferences = insertReferences(plan, true, planMergers)._1

    // Traverse level by level and convert merged plans to `CTERelationDef`s and keep non-merged
    // ones. While traversing replace references in plans back to `CTERelationRef`s or to original
    // plans. This is safe as a subplan at a level can reference only lower level ot other subplans.
    val subplansByLevel = ArrayBuffer.empty[IndexedSeq[LogicalPlan]]
    planMergers.foreach { planMerger =>
      val mergedPlans = planMerger.mergedPlans()
      subplansByLevel += mergedPlans.map { mergedPlan =>
        val planWithoutReferences = if (subplansByLevel.isEmpty) {
          // Level 0 plans can't contain references
          mergedPlan.plan
        } else {
          removeReferences(mergedPlan.plan, subplansByLevel)
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

    // Replace references back to `CTERelationRef`s or to original subplans.
    val newPlan = removeReferences(planWithReferences, subplansByLevel)

    // Add `CTERelationDef`s to the plan.
    val subplanCTEs = subplansByLevel.flatMap(_.collect { case cte: CTERelationDef => cte })
    if (subplanCTEs.nonEmpty) {
      WithCTE(newPlan, subplanCTEs.toSeq)
    } else {
      newPlan
    }
  }

  // First traversal inserts `ScalarSubqueryReference`s and `NoGroupingAggregateReference`s to the
  // plan and tries to merge subplans by each level. Levels are separated eiter by scalar subqueries
  // or by non-grouping aggregate nodes. Nodes with the same level make sense to try merging.
  private def insertReferences(
      plan: LogicalPlan,
      root: Boolean,
      planMergers: ArrayBuffer[PlanMerger]): (LogicalPlan, Int) = {
    if (!plan.containsAnyPattern(AGGREGATE, SCALAR_SUBQUERY)) {
      return (plan, 0)
    }

    // Calculate the level propagated from subquery plans, which is the maximum level of the
    // subqueries of the node + 1 or 0 if the node has no subqueries.
    var levelFromSubqueries = 0
    val nodeSubqueriesWithReferences =
      plan.transformExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
        case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
          val (planWithReferences, level) = insertReferences(s.plan, true, planMergers)

          // The subquery could contain a hint that is not propagated once we merge it, but as a
          // non-correlated scalar subquery won't be turned into a Join the loss of hints is fine.
          val mergeResult = getPlanMerger(planMergers, level).merge(planWithReferences, true)

          levelFromSubqueries = levelFromSubqueries.max(level + 1)

          val mergedOutput = mergeResult.outputMap(planWithReferences.output.head)
          val outputIndex =
            mergeResult.mergedPlan.plan.output.indexWhere(_.exprId == mergedOutput.exprId)
          ScalarSubqueryReference(
            level,
            mergeResult.mergedPlanIndex,
            outputIndex,
            s.dataType,
            s.exprId)
        case o => o
      }

    // Calculate the level of the node, which is the maximum of the above calculated level
    // propagated from subqueries and the level propagated from child nodes.
    val (planWithReferences, level) = nodeSubqueriesWithReferences match {
      case a: Aggregate if !root && a.groupingExpressions.isEmpty =>
        val (childWithReferences, levelFromChild) = insertReferences(a.child, false, planMergers)
        val aggregateWithReferences = a.withNewChildren(Seq(childWithReferences))

        // Level is the maximum of the level from subqueries and the level from child.
        val level = levelFromChild.max(levelFromSubqueries)

        val mergeResult = getPlanMerger(planMergers, level).merge(aggregateWithReferences, false)

        val mergedOutput = aggregateWithReferences.output.map(mergeResult.outputMap)
        val outputIndices =
          mergedOutput.map(a => mergeResult.mergedPlan.plan.output.indexWhere(_.exprId == a.exprId))
        val aggregateReference = NonGroupingAggregateReference(
          level,
          mergeResult.mergedPlanIndex,
          outputIndices,
          a.output
        )

        // This is a non-grouping aggregate node so propagate the level of the node + 1 to its
        // parent
        (aggregateReference, level + 1)
      case o =>
        val (newChildren, levels) = o.children.map(insertReferences(_, false, planMergers)).unzip
        // Level is the maximum of the level from subqueries and the level from the children.
        (o.withNewChildren(newChildren), (levelFromSubqueries +: levels).max)
    }

    (planWithReferences, level)
  }

  private def getPlanMerger(planMergers: ArrayBuffer[PlanMerger], level: Int) = {
    while (level >= planMergers.size) planMergers += new PlanMerger()
    planMergers(level)
  }

  // Second traversal replaces:
  // - a `ScalarSubqueryReference` either to
  //   `GetStructField(ScalarSubquery(CTERelationRef to the merged plan), merged output index)` if
  //   the plan is merged from multiple subqueries or to `ScalarSubquery(original plan)` if it
  //   isn't.
  // - a `NoGroupingAggregateReference` either to
  //   ```
  //     Project(
  //       Seq(
  //         GetStructField(
  //           ScalarSubquery(CTERelationRef to the merged plan),
  //           merged output index 1),
  //         GetStructField(
  //           ScalarSubquery(CTERelationRef to the merged plan),
  //           merged output index 2),
  //         ...),
  //       OneRowRelation)
  //   ```
  //   if the plan is merged from multiple subqueries or to `original plan` if it isn't.
  private def removeReferences(
      plan: LogicalPlan,
      subplansByLevel: ArrayBuffer[IndexedSeq[LogicalPlan]]) = {
    plan.transformUpWithPruning(
        _.containsAnyPattern(NO_GROUPING_AGGREGATE_REFERENCE, SCALAR_SUBQUERY_REFERENCE)) {
      case ngar: NonGroupingAggregateReference =>
        subplansByLevel(ngar.level)(ngar.mergedPlanIndex) match {
          case cte: CTERelationDef =>
            val projectList = ngar.outputIndices.zip(ngar.output).map { case (i, a) =>
              Alias(
                GetStructField(
                  ScalarSubquery(
                    CTERelationRef(cte.id, _resolved = true, cte.output, cte.isStreaming)),
                  i),
                a.name)(a.exprId)
            }
            Project(projectList, OneRowRelation())
          case o => o
        }
      case o => o.transformExpressionsUpWithPruning(_.containsPattern(SCALAR_SUBQUERY_REFERENCE)) {
        case ssr: ScalarSubqueryReference =>
          subplansByLevel(ssr.level)(ssr.mergedPlanIndex) match {
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
    override val dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)
}

/**
 * Temporal reference to a non-grouping aggregate which is added to a `PlanMerger`.
 *
 * @param level The level of the replaced aggregate. It defines the `PlanMerger` instance into which
 *              the aggregate is merged.
 * @param mergedPlanIndex The index of the merged plan in the `PlanMerger`.
 * @param outputIndices The indices of the output attributes of the merged plan.
 * @param output The output of original aggregate.
 */
case class NonGroupingAggregateReference(
    level: Int,
    mergedPlanIndex: Int,
    outputIndices: Seq[Int],
    override val output: Seq[Attribute]) extends LeafNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(NO_GROUPING_AGGREGATE_REFERENCE)
}

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
 * This rule tries to merge multiple subplans that have one row result. This can be either the plan
 * tree of a [[ScalarSubquery]] expression or the plan tree starting at a non-grouping [[Aggregate]]
 * node.
 *
 * The process is the following:
 * - While traversing through the plan each one row returning subplan is tried to merge into already
 *   seen one row returning subplans using `PlanMerger`s.
 *   During this first traversal each [[ScalarSubquery]] expression is replaced to a temporal
 *   [[ScalarSubqueryReference]] and each non-grouping [[Aggregate]] node is replaced to a temporal
 *   [[NonGroupingAggregateReference]] pointing to its possible merged version in `PlanMerger`s.
 *   `PlanMerger`s keep track of whether a plan is a result of merging 2 or more subplans, or is an
 *   original unmerged plan.
 *   [[ScalarSubqueryReference]]s and [[NonGroupingAggregateReference]]s contain all the required
 *   information to either restore the original subplan or create a reference to a merged CTE.
 * - Once the first traversal is complete and all possible merging have been done, a second
 *   traversal removes the references to either restore the original subplans or to replace the
 *   original to a modified ones that reference a CTE with a merged plan.
 *   A modified [[ScalarSubquery]] is constructed like:
 *   `GetStructField(ScalarSubquery(CTERelationRef to the merged plan), merged output index)`
 *   while a modified [[Aggregate]] is constructed like:
 *   ```
 *   Project(
 *     Seq(
 *       GetStructField(
 *         ScalarSubquery(CTERelationRef to the merged plan),
 *         merged output index 1),
 *       GetStructField(
 *         ScalarSubquery(CTERelationRef to the merged plan),
 *         merged output index 2),
 *       ...),
 *     OneRowRelation)
 *   ```
 *   where `merged output index`s are the index of the output attributes (of the CTE) that
 *   correspond to the output of the original node.
 * - If there are merged subqueries in `PlanMerger`s then a `WithCTE` node is built from these
 *   queries. The `CTERelationDef` nodes contain the merged subplans in the following form:
 *   `Project(Seq(CreateNamedStruct(name 1, attribute 1, ...) AS mergedValue), mergedSubplan)`.
 *
 * Here are a few examples:
 *
 * 1. a query with 2 subqueries:
 * ```
 * Project [scalar-subquery [] AS scalarsubquery(), scalar-subquery [] AS scalarsubquery()]
 * :  :- Aggregate [min(a) AS min(a)]
 * :  :  +- Relation [a, b, c]
 * :  +- Aggregate [sum(b) AS sum(b)]
 * :     +- Relation [a, b, c]
 * +- OneRowRelation
 * ```
 * is optimized to:
 * ```
 * WithCTE
 * :- CTERelationDef 0
 * :  +- Project [named_struct(min(a), min(a), sum(b), sum(b)) AS mergedValue]
 * :     +- Aggregate [min(a) AS min(a), sum(b) AS sum(b)]
 * :        +- Relation [a, b, c]
 * +- Project [scalar-subquery [].min(a) AS scalarsubquery(),
 *             scalar-subquery [].sum(b) AS scalarsubquery()]
 *    :  :- CTERelationRef 0
 *    :  +- CTERelationRef 0
 *    +- OneRowRelation
 * ```
 *
 * 2. a query with 2 non-grouping aggregates:
 * ```
 * Join Inner
 * :- Aggregate [min(a) AS min(a)]
 * :  +- Relation [a, b, c]
 * +- Aggregate [sum(b) AS sum(b), avg(cast(c as double)) AS avg(c)]
 *    +- Relation [a, b, c]
 * ```
 * is optimized to:
 * ```
 * WithCTE
 * :- CTERelationDef 0
 * :  +- Project [named_struct(min(a), min(a), sum(b), sum(b), avg(c), avg(c)) AS mergedValue]
 * :     +- Aggregate [min(a) AS min(a), sum(b) AS sum(b), avg(cast(c as double)) AS avg(c)]
 * :        +- Relation [a, b, c]
 * +- Join Inner
 *    :- Project [scalar-subquery [].min(a) AS min(a)]
 *    :  :  +- CTERelationRef 0
 *    :  +- OneRowRelation
 *    +- Project [scalar-subquery [].sum(b) AS sum(b), scalar-subquery [].avg(c) AS avg(c)]
 *       :  :- CTERelationRef 0
 *       :  +- CTERelationRef 0
 *       +- OneRowRelation
 * ```
 *
 * 3. a query with a subquery and a non-grouping aggregate:
 * ```
 * Join Inner
 * :- Project [scalar-subquery [] AS scalarsubquery()]
 * :  :  +- Aggregate [min(a) AS min(a)]
 * :  :     +- Relation [a, b, c]
 * :  +- OneRowRelation
 * +- Aggregate [sum(b) AS sum(b), avg(cast(c as double)) AS avg(c)]
 *    +- Relation [a, b, c]
 * ```
 * is optimized to:
 * ```
 * WithCTE
 * :- CTERelationDef 0
 * :  +- Project [named_struct(min(a), min(a), sum(b), sum(b), avg(c), avg(c)) AS mergedValue]
 * :     +- Aggregate [min(a) AS min(a), sum(b) AS sum(b), avg(cast(c as double)) AS avg(c)]
 * :        +- Relation [a, b, c]
 * +- Join Inner
 *    :- Project [scalar-subquery [].min(a) AS scalarsubquery()]
 *    :  :  +- CTERelationRef 0
 *    :  +- OneRowRelation
 *    +- Project [scalar-subquery [].sum(b) AS sum(b), scalar-subquery [].avg(c) AS avg(c)]
 *       :  :- CTERelationRef 0
 *       :  +- CTERelationRef 0
 *       +- OneRowRelation
 * ```
 *
 * Please note that in the above examples the aggregations are part of a "join group", which could
 * be rewritten as one aggregate without the need to introduce a CTE and keeping the join. But there
 * are more complex cases when this CTE based approach is the only viable option. Such cases include
 * when the aggregates reside at different parts of plan, maybe even in different subquery
 * expressions.
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
    // plans. This is safe as a subplan at a level can reference only lower level subplans.
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
  // plan and tries to merge subplans by each level. Levels are separated either by scalar
  // subqueries or by non-grouping aggregate nodes. Nodes with the same level make sense to try
  // merging.
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
        val (newChildren, levelsFromChildren) =
          o.children.map(insertReferences(_, false, planMergers)).unzip
        // Level is the maximum of the level from subqueries and the level from the children.
        (o.withNewChildren(newChildren), (levelFromSubqueries +: levelsFromChildren).max)
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

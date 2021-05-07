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
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY

/**
 * This rule tries to merge multiple non-correlated [[ScalarSubquery]]s into a
 * [[MultiScalarSubquery]] to compute multiple scalar values once.
 *
 * The process is the following:
 * - While traversing through the plan each [[ScalarSubquery]] plan is tried to merge into the cache
 *   of already seen subquery plans. If merge is possible then cache is updated with the merged
 *   subquery plan, if not then the new subquery plan is added to the cache.
 * - The original [[ScalarSubquery]] expression is replaced to a reference pointing to its cached
 *   version in this form: `GetStructField(MultiScalarSubquery(SubqueryReference(...)))`.
 * - A second traversal checks if a [[SubqueryReference]] is pointing to a subquery plan that
 *   returns multiple values and either replaces only [[SubqueryReference]] to the cached plan or
 *   restores the whole expression to its original [[ScalarSubquery]] form.
 * - [[ReuseSubquery]] rule makes sure that merged subqueries are computed once.
 *
 * Eg. the following query:
 *
 * SELECT
 *   (SELECT avg(a) FROM t GROUP BY b),
 *   (SELECT sum(b) FROM t GROUP BY b)
 *
 * is optimized from:
 *
 * Project [scalar-subquery#231 [] AS scalarsubquery()#241,
 *   scalar-subquery#232 [] AS scalarsubquery()#242L]
 * :  :- Aggregate [b#234], [avg(a#233) AS avg(a)#236]
 * :  :  +- Relation default.t[a#233,b#234] parquet
 * :  +- Aggregate [b#240], [sum(b#240) AS sum(b)#238L]
 * :     +- Project [b#240]
 * :        +- Relation default.t[a#239,b#240] parquet
 * +- OneRowRelation
 *
 * to:
 *
 * Project [multi-scalar-subquery#231.avg(a) AS scalarsubquery()#241,
 *   multi-scalar-subquery#232.sum(b) AS scalarsubquery()#242L]
 * :  :- Aggregate [b#234], [avg(a#233) AS avg(a)#236, sum(b#234) AS sum(b)#238L]
 * :  :  +- Project [a#233, b#234]
 * :  :     +- Relation default.t[a#233,b#234] parquet
 * :  +- Aggregate [b#234], [avg(a#233) AS avg(a)#236, sum(b#234) AS sum(b)#238L]
 * :     +- Project [a#233, b#234]
 * :        +- Relation default.t[a#233,b#234] parquet
 * +- OneRowRelation
 */
object MergeScalarSubqueries extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.scalarSubqueryMergeEabled && conf.subqueryReuseEnabled) {
      val mergedSubqueries = ArrayBuffer.empty[Project]
      removeReferences(mergeAndInsertReferences(plan, mergedSubqueries))
    } else {
      plan
    }
  }

  private def mergeAndInsertReferences(
      plan: LogicalPlan,
      mergedSubqueries: ArrayBuffer[Project]): LogicalPlan = {
    plan.transformWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY), ruleId) {
      case o => o.transformExpressionsUpWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY), ruleId) {
        case s: ScalarSubquery if s.children.isEmpty =>
          val (mergedPlan, ordinal) = mergeAndGetReference(s.plan, mergedSubqueries)
          GetStructField(s.copy(plan = mergedPlan), ordinal)
      }
    }
  }

  case class SubqueryReference(
      index: Int,
      mergedSubqueries: ArrayBuffer[Project]) extends LeafNode {
    override def stringArgs: Iterator[Any] = Iterator(index)

    override def output: Seq[Attribute] = mergedSubqueries(index).output
  }

  private def mergeAndGetReference(
      plan: LogicalPlan,
      mergedSubqueries: ArrayBuffer[Project]): (SubqueryReference, Int) = {
    mergedSubqueries.zipWithIndex.collectFirst {
      Function.unlift { case (header, i) => tryMergePlans(plan, header.child).map((header, _, i)) }
    }.map { case (header, (mergedPlan, outputMap), i) =>
      if (mergedPlan.output.size > header.child.output.size) {
        mergedSubqueries(i) = createHeader(mergedPlan)
      }
      val ordinal = mergedPlan.output.indexOf(outputMap(plan.output.head))
      SubqueryReference(i, mergedSubqueries) -> ordinal
    }.getOrElse {
      mergedSubqueries += createHeader(plan)
      SubqueryReference(mergedSubqueries.length - 1, mergedSubqueries) -> 0
    }
  }

  private def createHeader(plan: LogicalPlan) = {
    Project(Seq(Alias(CreateStruct(plan.output), "mergedValue")()), plan)
  }

  private def tryMergePlans(
      newPlan: LogicalPlan,
      existingPlan: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute])] = {
    (newPlan, existingPlan) match {
      case (np, ep) if np.canonicalized == ep.canonicalized =>
        Some(ep -> AttributeMap(np.output.zip(ep.output)))
      case (np: Project, ep: Project) =>
        tryMergePlans(np.child, ep.child).map { case (mergedChild, outputMap) =>
          val newProjectList = replaceAttributes(np.projectList, outputMap)
          val newOutputMap = createOutputMap(np.projectList, newProjectList)
          Project(distinctExpressions(ep.projectList ++ newProjectList), mergedChild) ->
            newOutputMap
        }
      case (np, ep: Project) =>
        tryMergePlans(np, ep.child).map { case (mergedChild, outputMap) =>
          Project(distinctExpressions(ep.projectList ++ outputMap.values), mergedChild) -> outputMap
        }
      case (np: Project, ep) =>
        tryMergePlans(np.child, ep).map { case (mergedChild, outputMap) =>
          val newProjectList = replaceAttributes(np.projectList, outputMap)
          val newOutputMap = createOutputMap(np.projectList, newProjectList)
          Project(distinctExpressions(ep.output ++ newProjectList), mergedChild) -> newOutputMap
        }
      case (np: Aggregate, ep: Aggregate) if supportedAggregateMerge(np, ep) =>
        tryMergePlans(np.child, ep.child).flatMap { case (mergedChild, outputMap) =>
          val newGroupingExpression = replaceAttributes(np.groupingExpressions, outputMap)
          if (ExpressionSet(newGroupingExpression) == ExpressionSet(ep.groupingExpressions)) {
            val newAggregateExpressions = replaceAttributes(np.aggregateExpressions, outputMap)
            val newOutputMap = createOutputMap(np.aggregateExpressions, newAggregateExpressions)
            Some(Aggregate(ep.groupingExpressions,
              distinctExpressions(ep.aggregateExpressions ++ newAggregateExpressions),
              mergedChild) -> newOutputMap)
          } else {
            None
          }
        }
      case _ => None
    }
  }

  private def replaceAttributes[T <: Expression](
      expressions: Seq[T],
      outputMap: AttributeMap[Attribute]) = {
    expressions.map(_.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T])
  }

  private def createOutputMap(from: Seq[NamedExpression], to: Seq[NamedExpression]) = {
    AttributeMap(from.map(_.toAttribute).zip(to.map(_.toAttribute)))
  }

  private def distinctExpressions(expressions: Seq[NamedExpression]) = {
    ExpressionSet(expressions).toSeq.asInstanceOf[Seq[NamedExpression]]
  }

  // Merging different aggregate implementations could cause performance regression
  private def supportedAggregateMerge(newPlan: Aggregate, existingPlan: Aggregate) = {
    val newPlanAggregateExpressions = newPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val existingPlanAggregateExpressions = existingPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val newPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      newPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    val existingPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      existingPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    newPlanSupportsHashAggregate && existingPlanSupportsHashAggregate ||
      !newPlanSupportsHashAggregate && !existingPlanSupportsHashAggregate && {
        val newPlanSupportsObjectHashAggregate =
          Aggregate.supportsObjectHashAggregate(newPlanAggregateExpressions)
        val existingPlanSupportsObjectHashAggregate =
          Aggregate.supportsObjectHashAggregate(existingPlanAggregateExpressions)
        newPlanSupportsObjectHashAggregate && existingPlanSupportsObjectHashAggregate ||
          !newPlanSupportsObjectHashAggregate && !existingPlanSupportsObjectHashAggregate
      }
  }

  private def removeReferences(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY), ruleId) {
      case gsf @ GetStructField(ss @ ScalarSubquery(sr: SubqueryReference, _, _), _, _) =>
        val header = sr.mergedSubqueries(sr.index)
        if (header.child.output.size > 1) {
          gsf.copy(child = ss.copy(plan = header))
        } else {
          ss.copy(plan = header.child)
        }
    }
  }
}

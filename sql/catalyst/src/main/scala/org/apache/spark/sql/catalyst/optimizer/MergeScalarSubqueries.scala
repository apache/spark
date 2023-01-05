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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, Filter, Join, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * This rule tries to merge multiple non-correlated [[ScalarSubquery]]s to compute multiple scalar
 * values once.
 *
 * The process is the following:
 * - While traversing through the plan each [[ScalarSubquery]] plan is tried to merge into the cache
 *   of already seen subquery plans. If merge is possible then cache is updated with the merged
 *   subquery plan, if not then the new subquery plan is added to the cache.
 *   During this first traversal each [[ScalarSubquery]] expression is replaced to a temporal
 *   [[ScalarSubqueryReference]] reference pointing to its cached version.
 *   The cache uses a flag to keep track of if a cache entry is a result of merging 2 or more
 *   plans, or it is a plan that was seen only once.
 *   Merged plans in the cache get a "Header", that contains the list of attributes form the scalar
 *   return value of a merged subquery.
 * - A second traversal checks if there are merged subqueries in the cache and builds a `WithCTE`
 *   node from these queries. The `CTERelationDef` nodes contain the merged subquery in the
 *   following form:
 *   `Project(Seq(CreateNamedStruct(name1, attribute1, ...) AS mergedValue), mergedSubqueryPlan)`
 *   and the definitions are flagged that they host a subquery, that can return maximum one row.
 *   During the second traversal [[ScalarSubqueryReference]] expressions that pont to a merged
 *   subquery is either transformed to a `GetStructField(ScalarSubquery(CTERelationRef(...)))`
 *   expression or restored to the original [[ScalarSubquery]].
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

  /**
   * An item in the cache of merged scalar subqueries.
   *
   * @param attributes Attributes that form the struct scalar return value of a merged subquery.
   * @param plan The plan of a merged scalar subquery.
   * @param merged A flag to identify if this item is the result of merging subqueries.
   *               Please note that `attributes.size == 1` doesn't always mean that the plan is not
   *               merged as there can be subqueries that are different ([[checkIdenticalPlans]] is
   *               false) due to an extra [[Project]] node in one of them. In that case
   *               `attributes.size` remains 1 after merging, but the merged flag becomes true.
   * @param references A set of subquery indexes in the cache to track all (including transitive)
   *                   nested subqueries.
   */
  case class Header(
      attributes: Seq[Attribute],
      plan: LogicalPlan,
      merged: Boolean,
      references: Set[Int])

  private def extractCommonScalarSubqueries(plan: LogicalPlan) = {
    val cache = ArrayBuffer.empty[Header]
    val planWithReferences = insertReferences(plan, cache)
    cache.zipWithIndex.foreach { case (header, i) =>
      cache(i) = cache(i).copy(plan =
        if (header.merged) {
          CTERelationDef(
            createProject(header.attributes, removeReferences(header.plan, cache)),
            underSubquery = true)
        } else {
          removeReferences(header.plan, cache)
        })
    }
    val newPlan = removeReferences(planWithReferences, cache)
    val subqueryCTEs = cache.filter(_.merged).map(_.plan.asInstanceOf[CTERelationDef])
    if (subqueryCTEs.nonEmpty) {
      WithCTE(newPlan, subqueryCTEs.toSeq)
    } else {
      newPlan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(plan: LogicalPlan, cache: ArrayBuffer[Header]): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case n => n.transformExpressionsUpWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
        // The subquery could contain a hint that is not propagated once we cache it, but as a
        // non-correlated scalar subquery won't be turned into a Join the loss of hints is fine.
        case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
          val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
          ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
      }
    }
  }

  // Caching returns the index of the subquery in the cache and the index of scalar member in the
  // "Header".
  private def cacheSubquery(plan: LogicalPlan, cache: ArrayBuffer[Header]): (Int, Int) = {
    val output = plan.output.head
    val references = mutable.HashSet.empty[Int]
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
      case ssr: ScalarSubqueryReference =>
        references += ssr.subqueryIndex
        references ++= cache(ssr.subqueryIndex).references
        ssr
    }

    cache.zipWithIndex.collectFirst(Function.unlift {
      case (header, subqueryIndex) if !references.contains(subqueryIndex) =>
        checkIdenticalPlans(plan, header.plan).map { outputMap =>
          val mappedOutput = mapAttributes(output, outputMap)
          val headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
          subqueryIndex -> headerIndex
        }.orElse{
          tryMergePlans(plan, header.plan).map {
            case (mergedPlan, outputMap) =>
              val mappedOutput = mapAttributes(output, outputMap)
              var headerIndex = header.attributes.indexWhere(_.exprId == mappedOutput.exprId)
              val newHeaderAttributes = if (headerIndex == -1) {
                headerIndex = header.attributes.size
                header.attributes :+ mappedOutput
              } else {
                header.attributes
              }
              cache(subqueryIndex) =
                Header(newHeaderAttributes, mergedPlan, true, header.references ++ references)
              subqueryIndex -> headerIndex
          }
        }
      case _ => None
    }).getOrElse {
      cache += Header(Seq(output), plan, false, references.toSet)
      cache.length - 1 -> 0
    }
  }

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  private def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  // Recursively traverse down and try merging 2 plans. If merge is possible then return the merged
  // plan with the attribute mapping from the new to the merged version.
  // Please note that merging arbitrary plans can be complicated, the current version supports only
  // some of the most important nodes.
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute])] = {
    checkIdenticalPlans(newPlan, cachedPlan).map(cachedPlan -> _).orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child).map { case (mergedChild, outputMap) =>
            val (mergedProjectList, newOutputMap) =
              mergeNamedExpressions(np.projectList, outputMap, cp.projectList)
            val mergedPlan = Project(mergedProjectList, mergedChild)
            mergedPlan -> newOutputMap
          }
        case (np, cp: Project) =>
          tryMergePlans(np, cp.child).map { case (mergedChild, outputMap) =>
            val (mergedProjectList, newOutputMap) =
              mergeNamedExpressions(np.output, outputMap, cp.projectList)
            val mergedPlan = Project(mergedProjectList, mergedChild)
            mergedPlan -> newOutputMap
          }
        case (np: Project, cp) =>
          tryMergePlans(np.child, cp).map { case (mergedChild, outputMap) =>
            val (mergedProjectList, newOutputMap) =
              mergeNamedExpressions(np.projectList, outputMap, cp.output)
            val mergedPlan = Project(mergedProjectList, mergedChild)
            mergedPlan -> newOutputMap
          }
        case (np: Aggregate, cp: Aggregate) if supportedAggregateMerge(np, cp) =>
          tryMergePlans(np.child, cp.child).flatMap { case (mergedChild, outputMap) =>
            val mappedNewGroupingExpression =
              np.groupingExpressions.map(mapAttributes(_, outputMap))
            // Order of grouping expression does matter as merging different grouping orders can
            // introduce "extra" shuffles/sorts that might not present in all of the original
            // subqueries.
            if (mappedNewGroupingExpression.map(_.canonicalized) ==
              cp.groupingExpressions.map(_.canonicalized)) {
              val (mergedAggregateExpressions, newOutputMap) =
                mergeNamedExpressions(np.aggregateExpressions, outputMap, cp.aggregateExpressions)
              val mergedPlan =
                Aggregate(cp.groupingExpressions, mergedAggregateExpressions, mergedChild)
              Some(mergedPlan -> newOutputMap)
            } else {
              None
            }
          }

        case (np: Filter, cp: Filter) =>
          tryMergePlans(np.child, cp.child).flatMap { case (mergedChild, outputMap) =>
            val mappedNewCondition = mapAttributes(np.condition, outputMap)
            // Comparing the canonicalized form is required to ignore different forms of the same
            // expression.
            if (mappedNewCondition.canonicalized == cp.condition.canonicalized) {
              val mergedPlan = cp.withNewChildren(Seq(mergedChild))
              Some(mergedPlan -> outputMap)
            } else {
              None
            }
          }

        case (np: Join, cp: Join) if np.joinType == cp.joinType && np.hint == cp.hint =>
          tryMergePlans(np.left, cp.left).flatMap { case (mergedLeft, leftOutputMap) =>
            tryMergePlans(np.right, cp.right).flatMap { case (mergedRight, rightOutputMap) =>
              val outputMap = leftOutputMap ++ rightOutputMap
              val mappedNewCondition = np.condition.map(mapAttributes(_, outputMap))
              // Comparing the canonicalized form is required to ignore different forms of the same
              // expression and `AttributeReference.quailifier`s in `cp.condition`.
              if (mappedNewCondition.map(_.canonicalized) == cp.condition.map(_.canonicalized)) {
                val mergedPlan = cp.withNewChildren(Seq(mergedLeft, mergedRight))
                Some(mergedPlan -> outputMap)
              } else {
                None
              }
            }
          }

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  private def createProject(attributes: Seq[Attribute], plan: LogicalPlan): Project = {
    Project(
      Seq(Alias(
        CreateNamedStruct(attributes.flatMap(a => Seq(Literal(a.name), a))),
        "mergedValue")()),
      plan)
  }

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  // Applies `outputMap` attribute mapping on attributes of `newExpressions` and merges them into
  // `cachedExpressions`. Returns the merged expressions and the attribute mapping from the new to
  // the merged version that can be propagated up during merging nodes.
  private def mergeNamedExpressions(
      newExpressions: Seq[NamedExpression],
      outputMap: AttributeMap[Attribute],
      cachedExpressions: Seq[NamedExpression]) = {
    val mergedExpressions = ArrayBuffer[NamedExpression](cachedExpressions: _*)
    val newOutputMap = AttributeMap(newExpressions.map { ne =>
      val mapped = mapAttributes(ne, outputMap)
      val withoutAlias = mapped match {
        case Alias(child, _) => child
        case e => e
      }
      ne.toAttribute -> mergedExpressions.find {
        case Alias(child, _) => child semanticEquals withoutAlias
        case e => e semanticEquals withoutAlias
      }.getOrElse {
        mergedExpressions += mapped
        mapped
      }.toAttribute
    })
    (mergedExpressions.toSeq, newOutputMap)
  }

  // Only allow aggregates of the same implementation because merging different implementations
  // could cause performance regression.
  private def supportedAggregateMerge(newPlan: Aggregate, cachedPlan: Aggregate) = {
    val aggregateExpressionsSeq = Seq(newPlan, cachedPlan).map { plan =>
      plan.aggregateExpressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
    }
    val Seq(newPlanSupportsHashAggregate, cachedPlanSupportsHashAggregate) =
      aggregateExpressionsSeq.map(aggregateExpressions => Aggregate.supportsHashAggregate(
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)))
    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
      newPlanSupportsHashAggregate == cachedPlanSupportsHashAggregate && {
        val Seq(newPlanSupportsObjectHashAggregate, cachedPlanSupportsObjectHashAggregate) =
          aggregateExpressionsSeq.map(aggregateExpressions =>
            Aggregate.supportsObjectHashAggregate(aggregateExpressions))
        newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
          newPlanSupportsObjectHashAggregate == cachedPlanSupportsObjectHashAggregate
      }
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(CTERelationRef to the merged plan)` if the plan is merged from
  // multiple subqueries or `ScalarSubquery(original plan)` if it isn't.
  private def removeReferences(
      plan: LogicalPlan,
      cache: ArrayBuffer[Header]) = {
    plan.transformUpWithSubqueries {
      case n =>
        n.transformExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
          case ssr: ScalarSubqueryReference =>
            val header = cache(ssr.subqueryIndex)
            if (header.merged) {
              val subqueryCTE = header.plan.asInstanceOf[CTERelationDef]
              GetStructField(
                ScalarSubquery(
                  CTERelationRef(subqueryCTE.id, _resolved = true, subqueryCTE.output),
                  exprId = ssr.exprId),
                ssr.headerIndex)
            } else {
              ScalarSubquery(header.plan, exprId = ssr.exprId)
            }
        }
    }
  }
}

/**
 * Temporal reference to a cached subquery.
 *
 * @param subqueryIndex A subquery index in the cache.
 * @param headerIndex An index in the output of merged subquery.
 * @param dataType The dataType of origin scalar subquery.
 */
case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALAR_SUBQUERY_REFERENCE)

  override def stringArgs: Iterator[Any] = Iterator(subqueryIndex, headerIndex, dataType, exprId.id)
}

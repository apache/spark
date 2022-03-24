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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CTERelationDef, CTERelationRef, Filter, Join, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, SCALAR_SUBQUERY_REFERENCE, TreePattern}
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
 * WithCTE
 * :- CTERelationDef 0
 * :  +- Project [named_struct(avg(a), avg(a)#247, sum(a), sum(a)#250L) AS mergedValue#260]
 * :     +- Aggregate [avg(a#244) AS avg(a)#247, sum(a#244) AS sum(a)#250L]
 * :        +- Project [a#244]
 * :           +- Relation default.t[a#244,b#245] parquet
 * +- Project [scalar-subquery#242 [].avg(a) AS scalarsubquery()#253,
 *             scalar-subquery#243 [].sum(a) AS scalarsubquery()#254L]
 *    :  :- CTERelationRef 0, true, [mergedValue#260], true
 *    :  +- CTERelationRef 0, true, [mergedValue#260], true
 *    +- OneRowRelation
 */
object MergeScalarSubqueries extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case s: Subquery => s.copy(child = extractCommonScalarSubqueries(s.child))
      case _ => extractCommonScalarSubqueries(plan)
    }
  }

  /**
   * An item in the cache of merged scalar subqueries.
   *
   * @param elements  List of attributes that form the scalar return value of a merged subquery
   * @param plan      The plan of a merged scalar subquery
   * @param merged    A flag to identify if this item is the result of merging subqueries.
   *                  Please note that `elements.size == 1` doesn't always mean that the plan is not
   *                  merged as there can be subqueries that are different ([[checkIdenticalPlans]]
   *                  is false) due to an extra [[Project]] node in one of them. In that case
   *                  `elements.size` remains 1 after merging, but the merged flag becomes true.
   */
  case class Header(elements: Seq[(String, Attribute)], plan: LogicalPlan, merged: Boolean)

  private def extractCommonScalarSubqueries(plan: LogicalPlan) = {
    val cache = ListBuffer.empty[Header]
    val (newPlan, subqueryCTEs) = removeReferences(insertReferences(plan, cache), cache)
    if (subqueryCTEs.nonEmpty) {
      WithCTE(newPlan, subqueryCTEs)
    } else {
      newPlan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(plan: LogicalPlan, cache: ListBuffer[Header]): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
      case s: ScalarSubquery if !s.isCorrelated && s.deterministic =>
        val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
        ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
    }
  }

  // Caching returns the index of the subquery in the cache and the index of scalar member in the
  // "Header".
  private def cacheSubquery(plan: LogicalPlan, cache: ListBuffer[Header]): (Int, Int) = {
    val output = plan.output.head
    cache.zipWithIndex.collectFirst(Function.unlift { case (header, subqueryIndex) =>
      checkIdenticalPlans(plan, header.plan).map { outputMap =>
        val mappedOutput = mapAttributes(output, outputMap)
        val headerIndex = header.elements.indexWhere {
          case (_, attribute) => attribute.exprId == mappedOutput.exprId
        }
        subqueryIndex -> headerIndex
      }.orElse(tryMergePlans(plan, header.plan).map {
        case (mergedPlan, outputMap) =>
          val mappedOutput = mapAttributes(output, outputMap)
          var headerIndex = header.elements.indexWhere {
            case (_, attribute) => attribute.exprId == mappedOutput.exprId
          }
          val newHeaderElements = if (headerIndex == -1) {
            headerIndex = header.elements.size
            header.elements :+ (output.name -> mappedOutput)
          } else {
            header.elements
          }
          cache(subqueryIndex) = Header(newHeaderElements, mergedPlan, true)
          subqueryIndex -> headerIndex
      })
    }).getOrElse {
      cache += Header(Seq(output.name -> output), plan, false)
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
            // Order of grouping expression doesn't matter so we can compare sets
            if (ExpressionSet(mappedNewGroupingExpression) ==
              ExpressionSet(cp.groupingExpressions)) {
              val (mergedAggregateExpressions, newOutputMap) =
                mergeNamedExpressions(np.aggregateExpressions, outputMap, cp.aggregateExpressions)
              val mergedPlan =
                Aggregate(cp.groupingExpressions, mergedAggregateExpressions, mergedChild)
              Some(mergedPlan -> newOutputMap)
            } else {
              None
            }
          }

        // Merging general nodes is complicated. This implementation:
        // - Supports only a whitelist of nodes (see `supportedMerge()`).
        // - All children need to be direct fields of the tree node case class. Tree nodes where a
        //   child is wrapped into a `Seq` or `Option` (e.g. `Union`) is not supported.
        // - Expressions can be wrapped.
        // - Children are tried to be merged in the same order.
        case (np, cp) if supportedMerge(np) && np.getClass == cp.getClass &&
            np.children.size == cp.children.size &&
            np.expressions.size == cp.expressions.size &&
            // Fields that don't contain any children or expressions should match
            np.productIterator.filterNot(np.children.contains)
              .filter(QueryPlan.extractExpressions(_).isEmpty).toSeq ==
              cp.productIterator.filterNot(cp.children.contains)
                .filter(QueryPlan.extractExpressions(_).isEmpty).toSeq =>
          val merged = np.children.zip(cp.children).map {
            case (npChild, cpChild) => tryMergePlans(npChild, cpChild)
          }
          if (merged.forall(_.isDefined)) {
            val (mergedChildren, outputMaps) = merged.map(_.get).unzip
            val outputMap = AttributeMap(outputMaps.map(_.iterator).reduce(_ ++ _).toSeq)
            // We know that fields that don't contain any children or expressions do match and
            // children can be merged so we need to test expressions only
            if (np.expressions.map(mapAttributes(_, outputMap).canonicalized) ==
              cp.expressions.map(_.canonicalized)) {
              val mergedPlan = cp.withNewChildren(mergedChildren)
              Some(mergedPlan -> outputMap)
            } else {
              None
            }
          } else {
            None
          }

        // Otherwise merging is not possible.
        case _ => None
      })
  }

  private def createProject(elements: Seq[(String, Attribute)], plan: LogicalPlan): Project = {
    Project(
      Seq(Alias(
        CreateNamedStruct(elements.flatMap {
          case (name, attribute) => Seq(Literal(name), attribute)
        }),
        "mergedValue")()),
      plan)
  }

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }

  // Applies `outputMap` attribute mapping on elements of `newExpressions` and merges them into
  // `cachedExpressions`. Returns the merged expressions and the attribute mapping from the new to
  // the merged version that can be propagated up during merging nodes.
  private def mergeNamedExpressions(
      newExpressions: Seq[NamedExpression],
      outputMap: AttributeMap[Attribute],
      cachedExpressions: Seq[NamedExpression]) = {
    val mergedExpressions = ListBuffer[NamedExpression](cachedExpressions: _*)
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
    val newPlanAggregateExpressions = newPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val cachedPlanAggregateExpressions = cachedPlan.aggregateExpressions.flatMap(_.collect {
      case a: AggregateExpression => a
    })
    val newPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      newPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    val cachedPlanSupportsHashAggregate = Aggregate.supportsHashAggregate(
      cachedPlanAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    newPlanSupportsHashAggregate && cachedPlanSupportsHashAggregate ||
      !newPlanSupportsHashAggregate && !cachedPlanSupportsHashAggregate && {
        val newPlanSupportsObjectHashAggregate =
          Aggregate.supportsObjectHashAggregate(newPlanAggregateExpressions)
        val cachedPlanSupportsObjectHashAggregate =
          Aggregate.supportsObjectHashAggregate(cachedPlanAggregateExpressions)
        newPlanSupportsObjectHashAggregate && cachedPlanSupportsObjectHashAggregate ||
          !newPlanSupportsObjectHashAggregate && !cachedPlanSupportsObjectHashAggregate
      }
  }

  // Whitelist of mergeable general nodes.
  private def supportedMerge(plan: LogicalPlan) = {
    plan match {
      case _: Filter => true
      case _: Join => true
      case _ => false
    }
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(CTERelationRef to the merged plan)` if the plan is merged from
  // multiple subqueries or `ScalarSubquery(original plan)` if it isn't.
  private def removeReferences(
      plan: LogicalPlan,
      cache: ListBuffer[Header]): (LogicalPlan, Seq[CTERelationDef]) = {
    val subqueryCTEs = mutable.Map.empty[Int, CTERelationDef]
    val newPlan =
      plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
        case ssr: ScalarSubqueryReference =>
          val header = cache(ssr.subqueryIndex)
          if (header.merged) {
            val subqueryCTE = subqueryCTEs.getOrElseUpdate(ssr.subqueryIndex,
              CTERelationDef(createProject(header.elements, header.plan)))
            GetStructField(
              ScalarSubquery(
                CTERelationRef(subqueryCTE.id, _resolved = true, subqueryCTE.output,
                  subquery = true),
                exprId = ssr.exprId),
              ssr.headerIndex)
          } else {
            ScalarSubquery(plan = header.plan, exprId = ssr.exprId)
          }
    }
    (newPlan, subqueryCTEs.values.toSeq)
  }
}

/**
 * Temporal reference to a subquery.
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

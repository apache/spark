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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, CommonScalarSubqueries, Filter, Join, LogicalPlan, Project, Subquery}
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
 *   During this first traversal each [[ScalarSubquery]] expression is replaced to a
 *   [[ScalarSubqueryReference]] pointing to its cached version.
 *   The cache uses a flag to keep track of if a cache entry is a results of merging 2 or more
 *   plans, or it is a plan that was seen only once.
 *   Merged plans in the cache get a "header", that is is basically
 *   `CreateNamedStruct(name1, attribute1, name2, attribute2, ...)` expression in new root
 *   [[Project]] node. This expression ensures that the merged plan is a valid scalar subquery that
 *   returns only one value.
 * - A second traversal checks if a [[ScalarSubqueryReference]] is pointing to a merged subquery
 *   plan or not and either keeps the reference or restores the original [[ScalarSubquery]].
 *   If there are [[ScalarSubqueryReference]] nodes remained a [[CommonScalarSubqueries]] root node
 *   is added to the plan with the referenced scalar subqueries.
 * - [[PlanSubqueries]] or [[PlanAdaptiveSubqueries]] rule does the physical planning of scalar
 *   subqueries including the ones under [[CommonScalarSubqueriesExec]] node and replaces
 *   each [[ScalarSubqueryReference]] to their referenced physical plan in
 *   `GetStructField(ScalarSubquery(merged plan with CreateNamedStruct() header))` form.
 *   It is important that references pointing to the same merged subquery are replaced to the same
 *   planned instance to make sure that each merged subquery runs only once (even without a wrapping
 *   [[ReuseSubquery]] node).
 *   Finally, the [[CommonScalarSubqueriesExec]] node is removed from the physical plan.
 * - The [[ReuseExchangeAndSubquery]] rule wraps the second, third, ... instances of the same
 *   subquery into a [[ReuseSubquery]] node, but this just a cosmetic change in the plan.
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
 *          scalar-subquery#232 [] AS scalarsubquery()#242L]
 * :  :- Aggregate [b#234], [avg(a#233) AS avg(a)#236]
 * :  :  +- Relation default.t[a#233,b#234] parquet
 * :  +- Aggregate [b#240], [sum(b#240) AS sum(b)#238L]
 * :     +- Project [b#240]
 * :        +- Relation default.t[a#239,b#240] parquet
 * +- OneRowRelation
 *
 * to:
 *
 * CommonScalarSubqueries [scalar-subquery#250 []]
 * :  +- Project [named_struct(avg(a), avg(a)#236, sum(b), sum(b)#238L) AS mergedValue#249]
 * :     +- Aggregate [b#234], [avg(a#233) AS avg(a)#236, sum(b#234) AS sum(b)#238L]
 * :        +- Project [a#233, b#234]
 * :           +- Relation default.t[a#233,b#234] parquet
 * +- Project [scalarsubqueryreference(0, 0, DoubleType, 231) AS scalarsubquery()#241,
 *             scalarsubqueryreference(0, 1, LongType, 232) AS scalarsubquery()#242L]
 *    +- OneRowRelation
 */
object MergeScalarSubqueries extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Subquery(_: CommonScalarSubqueries, _) => plan
      case s: Subquery => s.copy(child = extractCommonScalarSubqueries(s.child))
      case _: CommonScalarSubqueries => plan
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
    val newPlan = removeReferences(insertReferences(plan, cache), cache)
    if (cache.nonEmpty) {
      val scalarSubqueries = cache.map {
        case Header(elements, child, _) => ScalarSubquery(createProject(elements, child))
      }.toSeq
      CommonScalarSubqueries(scalarSubqueries, newPlan)
    } else {
      newPlan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(plan: LogicalPlan, cache: ListBuffer[Header]): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
      case s: ScalarSubquery if s.children.isEmpty =>
        val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
        ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
    }
  }

  // Caching returns the index of the subquery in the cache and the index of scalar member in the
  // "header", that `CreateNamedStruct(name1, attribute1, name2, attribute2, ...)` expression in a
  // [[Project]] node.
  private def cacheSubquery(plan: LogicalPlan, cache: ListBuffer[Header]): (Int, Int) = {
    val firstOutput = plan.output.head
    cache.zipWithIndex.collectFirst(Function.unlift { case (header, subqueryIndex) =>
      checkIdenticalPlans(plan, header.plan)
        .map((subqueryIndex, header, header.plan, _, header.merged))
        .orElse(tryMergePlans(plan, header.plan).map {
          case (mergedPlan, outputMap) => (subqueryIndex, header, mergedPlan, outputMap, true)
        })
    }).map { case (subqueryIndex, header, mergedPlan, outputMap, merged) =>
      val mappedFirstOutput = mapAttributes(firstOutput, outputMap)
      var headerIndex = header.elements.indexWhere {
        case (_, attribute) => attribute.exprId == mappedFirstOutput.exprId
      }
      if (headerIndex == -1) {
        val newHeaderElements = header.elements :+ (firstOutput.name -> mappedFirstOutput)
        cache(subqueryIndex) = Header(newHeaderElements, mergedPlan, merged)
        headerIndex = header.elements.size
      }
      subqueryIndex -> headerIndex
    }.getOrElse {
      cache += Header(Seq(firstOutput.name -> firstOutput), plan, false)
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

        // Merging general nodes is complicated and this implementation supports only those nodes in
        // which the order and the number of output attributes are not relevant (see
        // `supportedMerge()` whitelist).
        // Also, this implementation supports only those nodes in which children can be merged in
        // the same order.
        case (np, cp) if supportedMerge(np) && np.getClass == cp.getClass &&
          np.children.size == cp.children.size =>
          val merged = np.children.zip(cp.children).map {
            case (npChild, cpChild) => tryMergePlans(npChild, cpChild)
          }
          if (merged.forall(_.isDefined)) {
            val (mergedChildren, outputMaps) = merged.map(_.get).unzip
            val outputMap = AttributeMap(outputMaps.map(_.iterator).reduce(_ ++ _).toSeq)
            val mappedNewPlan = mapAttributes(np.withNewChildren(mergedChildren), outputMap)
            val mergedPlan = cp.withNewChildren(mergedChildren)
            if (mappedNewPlan.canonicalized == mergedPlan.canonicalized) {
              Some(mergedPlan -> outputMap)
            } else {
              None
            }
          } else {
            None
          }

        // As a follow-up, it would be possible to merge `CommonScalarSubqueries` nodes, which would
        // allow merging subqueries with mergee subqueries.
        // E.g. this query:
        //
        // SELECT
        //   (
        //     SELECT
        //       (SELECT avg(a) FROM t GROUP BY b) +
        //       (SELECT sum(b) FROM t GROUP BY b)
        //   ),
        //   (
        //     SELECT
        //       (SELECT max(a) FROM t GROUP BY b) +
        //       (SELECT min(b) FROM t GROUP BY b)
        //   )
        //
        // is currently optimized to:
        //
        // == Optimized Logical Plan ==
        // Project [scalar-subquery#233 [] AS scalarsubquery()#255,
        //          scalar-subquery#236 [] AS scalarsubquery()#256]
        // :  :- CommonScalarSubqueries [scalar-subquery#264 []]
        // :  :  :  +- Aggregate [b#238], [named_struct(avg(a), avg(a#237), sum(b), sum(b#238))
        //                                 AS mergedValue#263]
        // :  :  :     +- Relation default.t[a#237,b#238] parquet
        // :  :  +- Project [(scalarsubqueryreference(0, 0, DoubleType, 231) +
        //                   cast(scalarsubqueryreference(0, 1, LongType, 232) as double))
        //                   AS (scalarsubquery() + scalarsubquery())#245]
        // :  :     +- OneRowRelation
        // :  +- CommonScalarSubqueries [scalar-subquery#269 []]
        // :     :  +- Aggregate [b#254], [named_struct(min(a), min(a#253), max(b), max(b#254))
        //                                 AS mergedValue#268]
        // :     :     +- Relation default.t[a#253,b#254] parquet
        // :     +- Project [(scalarsubqueryreference(0, 0, IntegerType, 234) +
        //                   scalarsubqueryreference(0, 1, IntegerType, 235))
        //                   AS (scalarsubquery() + scalarsubquery())#252]
        // :        +- OneRowRelation
        // +- OneRowRelation
        //
        // but if we implemented merging `CommonScalarSubqueries` nodes then the plan could be
        // transformed further and all leaf subqueries could be merged.

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

  private def mapAttributes(plan: LogicalPlan, outputMap: AttributeMap[Attribute]) = {
    plan.transformExpressions {
      case a: Attribute => outputMap.getOrElse(a, a)
    }
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

  // Merging different aggregate implementations could cause performance regression
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

  // Whitelist of mergeable general nodes
  private def supportedMerge(plan: LogicalPlan) = {
    plan match {
      case _: Filter => true
      case _: Join => true
      case _ => false
    }
  }

  // Second traversal replaces `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(merged plan with CreateNamedStruct() header))` if the plan is
  // merged from multiple subqueries or `ScalarSubquery(original plan)` it it isn't.
  private def removeReferences(plan: LogicalPlan, cache: ListBuffer[Header]): LogicalPlan = {
    val nonMergedSubqueriesBefore = cache.scanLeft(0) {
      case (nonMergedSubqueriesBefore, header) =>
        nonMergedSubqueriesBefore + (if (header.merged) 0 else 1)
    }.toArray
    val newPlan =
      plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY_REFERENCE)) {
        case ssr: ScalarSubqueryReference =>
          val header = cache(ssr.subqueryIndex)
          if (header.merged) {
            if (nonMergedSubqueriesBefore(ssr.subqueryIndex) > 0) {
              ssr.copy(subqueryIndex =
                ssr.subqueryIndex - nonMergedSubqueriesBefore(ssr.subqueryIndex))
            } else {
              ssr
            }
          } else {
            ScalarSubquery(plan = header.plan, exprId = ssr.exprId)
          }
    }
    cache.zipWithIndex.collect {
      case (header, i) if !header.merged => i
    }.reverse.foreach(cache.remove)
    newPlan
  }
}

/**
 * Reference to a subquery in a `CommonScalarSubqueries` or `CommonScalarSubqueriesExec` node.
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

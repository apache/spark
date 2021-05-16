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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY
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
 *   The cache uses headers to keep track of if a cache entry is the results of merging 2 or more
 *   plans, or it is a plan that was seen only once.
 *   The header is basically a `CreateNamedStructure(name1, attribute1, name2, attribute2, ...)`
 *   expression to form a merged scalar subquery plan.
 * - A second traversal checks if a [[ScalarSubqueryReference]] is pointing to a merged subquery
 *   plan or not, and either replaces the reference to a
 *   `GetStructField(ScalarSubquery(merged plan with CreateNamedStruct() header))` expression to
 *   select the right scalar value from the merged plan, or restores the original
 *   [[ScalarSubquery]].
 * - [[ReuseSubquery]] rule makes sure that merged subqueries are computed only once.
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
 * Project [scalar-subquery#231 [].avg(a) AS scalarsubquery()#241,
 *          scalar-subquery#232 [].sum(b) AS scalarsubquery()#242L]
 * :  :- Project [named_struct(avg(a), avg(a)#236, sum(b), sum(b)#238L) AS mergedValue#249]
 * :  :  +- Aggregate [b#234], [avg(a#233) AS avg(a)#236, sum(b#234) AS sum(b)#238L]
 * :  :     +- Project [a#233, b#234]
 * :  :        +- Relation default.t[a#233,b#234] parquet
 * :  :- Project [named_struct(avg(a), avg(a)#236, sum(b), sum(b)#238L) AS mergedValue#249]
 * :  :  +- Aggregate [b#234], [avg(a#233) AS avg(a)#236, sum(b#234) AS sum(b)#238L]
 * :  :     +- Project [a#233, b#234]
 * :  :        +- Relation default.t[a#233,b#234] parquet
 * +- OneRowRelation
 */
object MergeScalarSubqueries extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.scalarSubqueryMergeEabled && conf.subqueryReuseEnabled) {
      // Plan of subqueries and a flag is the plan is merged
      val cache = ListBuffer.empty[(Project, Boolean)]
      removeReferences(insertReferences(plan, cache), cache)
    } else {
      plan
    }
  }

  // First traversal builds up the cache and inserts `ScalarSubqueryReference`s to the plan.
  private def insertReferences(
      plan: LogicalPlan,
      cache: ListBuffer[(Project, Boolean)]): LogicalPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY), ruleId) {
      case s: ScalarSubquery if s.children.isEmpty =>
        val (subqueryIndex, headerIndex) = cacheSubquery(s.plan, cache)
        ScalarSubqueryReference(subqueryIndex, headerIndex, s.dataType, s.exprId)
    }
  }

  // Caching returns the index of the subquery in the cache and the index of the scalar value the
  // `CreateNamedStruct` header.
  private def cacheSubquery(
      plan: LogicalPlan,
      cache: ListBuffer[(Project, Boolean)]): (Int, Int) = {
    val firstOutput = plan.output.head
    cache.zipWithIndex.collectFirst(Function.unlift { case ((header, merged), subqueryIndex) =>
      checkIdenticalPlans(plan, header.child)
        .map((subqueryIndex, header, header.child, _, merged))
        .orElse(tryMergePlans(plan, header.child).map {
          case (mergedPlan, outputMap) => (subqueryIndex, header, mergedPlan, outputMap, true)
        })
    }).map { case (subqueryIndex, header, mergedPlan, outputMap, merged) =>
      val mappedFirstOutput = mapAttributes(firstOutput, outputMap)
      val headerElements = getHeaderElements(header)
      var headerIndex = headerElements.indexWhere { case (name, attribute) =>
        name.value == firstOutput.name && attribute.exprId == mappedFirstOutput.exprId
      }
      if (headerIndex == -1) {
        val newHeaderElements = headerElements :+ (Literal(firstOutput.name) -> mappedFirstOutput)
        cache(subqueryIndex) = createHeader(newHeaderElements, mergedPlan) -> merged
        headerIndex = headerElements.size
      }
      subqueryIndex -> headerIndex
    }.getOrElse {
      val (mergedPlan, _) = traverseAndMergeNamedExpressions(plan)
      cache += createHeader(Seq(Literal(firstOutput.name) -> firstOutput), mergedPlan) -> false
      cache.length - 1 -> 0
    }
  }

  // If 2 plans are identical return the attribute mapping from the new to the cached version.
  // Please note that plans containing attributes can be non-equal due to cosmetic differences of
  // attributes (qualifier, metadata) so only return those attributes where exprId is different.
  private def checkIdenticalPlans(newPlan: LogicalPlan, cachedPlan: LogicalPlan) = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output).filterNot {
        case (a1, a2) => a1.exprId == a2.exprId
      }))
    } else {
      None
    }
  }

  // Try merging 2 plans and return the merged plan with the attribute mapping from the new to the
  // cached version.
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

        // Merging 2 general nodes is complicated. This implementation supports merging 2 nodes
        // where children can be merged in same order, but the `supportedMerge()` whitelist is also
        // applied to be on the safe side.
        case (np, cp) if supportedMerge(np) && np.getClass == cp.getClass && np.children.nonEmpty &&
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
        case _ => None
      })
  }

  private def createHeader(headerElements: Seq[(Literal, Attribute)], plan: LogicalPlan) = {
    Project(
      Seq(Alias(
        CreateNamedStruct(headerElements.flatMap {
          case (name, attribute) => Seq(name, attribute)
        }),
        "mergedValue")()),
      plan)
  }

  private def getHeaderElements(header: Project) = {
    val mergedValue =
      header.projectList.head.asInstanceOf[Alias].child.asInstanceOf[CreateNamedStruct]
    mergedValue.children.grouped(2).map {
      case Seq(name: Literal, attribute: Attribute) => name -> attribute
    }.toSeq
  }

  private def traverseAndMergeNamedExpressions(
      plan: LogicalPlan): (LogicalPlan, AttributeMap[Attribute]) = {
    plan match {
      case p: Project =>
        val (newChild, outputMap) = traverseAndMergeNamedExpressions(p.child)
        val (mergedProjectList, newOutputMap) =
          mergeNamedExpressions(p.projectList, outputMap, Seq.empty)
        val newProject = p.copy(projectList = mergedProjectList, child = newChild)
        newProject -> newOutputMap
      case a: Aggregate =>
        val (newChild, outputMap) = traverseAndMergeNamedExpressions(a.child)
        val newGroupingExpressions = a.groupingExpressions.map(mapAttributes(_, outputMap))
        val (newAggregateExpressions, newOutputMap) =
          mergeNamedExpressions(a.aggregateExpressions, outputMap, Seq.empty)
        val newAggregete = a.copy(
          groupingExpressions = newGroupingExpressions,
          aggregateExpressions = newAggregateExpressions,
          child = newChild)
        newAggregete-> newOutputMap
      case _ if supportedMerge(plan) && plan.children.nonEmpty =>
        val (newChildren, outputMaps) = plan.children.map(traverseAndMergeNamedExpressions).unzip
        val outputMap = AttributeMap(outputMaps.map(_.iterator).reduce(_ ++ _).toSeq)
        val newPlan = mapAttributes(plan.withNewChildren(newChildren), outputMap)
        newPlan -> outputMap
      case _ => plan -> AttributeMap.empty
    }
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

  // In a subquery it is not relevant if 2 or more `Alias` refer to the same expression so before
  // caching a new plan or while merging a new plan to a cached one we need to consolidate named
  // expressions.
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
    }.filterNot { case (a1, a2) => a1.exprId == a2.exprId })
    (mergedExpressions, newOutputMap)
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

  // Second traversal replaces temporary `ScalarSubqueryReference`s to either
  // `GetStructField(ScalarSubquery(merged plan with CreateNamedStruct() header))` if the plan is
  // merged or `ScalarSubquery(original plan)` it it isn't. Please note that this later behavior
  // makes the rule idempotent.
  private def removeReferences(
      plan: LogicalPlan,
      cache: ListBuffer[(Project, Boolean)]): LogicalPlan = {
    plan.transformAllExpressions {
      case ssr: ScalarSubqueryReference =>
        val (header, merged) = cache(ssr.subqueryIndex)
        if (merged) {
          GetStructField(
            ScalarSubquery(plan = header, exprId = ssr.exprId),
            ssr.headerIndex)
        } else {
          ScalarSubquery(plan = header.child, exprId = ssr.exprId)
        }
    }
  }
}

/**
 * Reference to a subquery in the cache. These nodes temporary replace [[ScalarSubquery]] nodes
 * while [[MergeScalarSubqueries]] is running.
 */
case class ScalarSubqueryReference(
    subqueryIndex: Int,
    headerIndex: Int,
    dataType: DataType,
    exprId: ExprId) extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true
}

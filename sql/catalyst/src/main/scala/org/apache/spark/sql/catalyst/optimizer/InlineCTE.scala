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
import org.apache.spark.sql.catalyst.expressions.{Alias, OuterReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Join, JoinHint, LogicalPlan, Project, Subquery, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Inlines CTE definitions into corresponding references if either of the conditions satisfies:
 * 1. The CTE definition does not contain any non-deterministic expressions or contains attribute
 *    references to an outer query. If this CTE definition references another CTE definition that
 *    has non-deterministic expressions, it is still OK to inline the current CTE definition.
 * 2. The CTE definition is only referenced once throughout the main query and all the subqueries.
 *
 * CTE definitions that appear in subqueries and are not inlined will be pulled up to the main
 * query level.
 *
 * @param alwaysInline if true, inline all CTEs in the query plan.
 * @param keepDanglingRelations if true, dangling CTE relations will be kept in the original
 *                              `WithCTE` node.
 */
case class InlineCTE(
    alwaysInline: Boolean = false,
    keepDanglingRelations: Boolean = false) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = mutable.SortedMap.empty[Long, CTEReferenceInfo]
      buildCTEMap(plan, cteMap)
      cleanCTEMap(cteMap)
      inlineCTE(plan, cteMap)
    } else {
      plan
    }
  }

  private def shouldInline(cteDef: CTERelationDef, refCount: Int): Boolean = alwaysInline || {
    // We do not need to check enclosed `CTERelationRef`s for `deterministic` or `OuterReference`,
    // because:
    // 1) It is fine to inline a CTE if it references another CTE that is non-deterministic;
    // 2) Any `CTERelationRef` that contains `OuterReference` would have been inlined first.
    refCount == 1 ||
      cteDef.deterministic ||
      cteDef.child.exists(_.expressions.exists(_.isInstanceOf[OuterReference]))
  }

  /**
   * Accumulates all the CTEs from a plan into a special map.
   *
   * @param plan The plan to collect the CTEs from
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids.
   * @param collectCTERefs A function to collect CTE references so that the caller side can do some
   *                       bookkeeping work.
   */
  private def buildCTEMap(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, CTEReferenceInfo],
      collectCTERefs: (CTERelationRef, Seq[Long]) => Unit = (_, _) => ()): Unit = {
    plan match {
      case WithCTE(child, cteDefs) =>
        cteDefs.foreach { cteDef =>
          cteMap(cteDef.id) = CTEReferenceInfo(
            cteDef = cteDef,
            refCount = 0,
            outgoingRefs = mutable.Map.empty.withDefaultValue(0),
            indirectOutgoingRefSources = mutable.Set.empty[Long],
            shouldInline = true
          )
        }
        cteDefs.foreach { cteDef =>
          buildCTEMap(cteDef, cteMap, (ref, lineage) => {
            if (cteDefs.exists(_.id == ref.cteId)) {
              // The CTE relation being referenced is defined by this WITH. We need to do some
              // bookkeeping here w.r.t. the lineage of this reference.
              //  - Update the outgoing ref count of the bottom-most CTE relation that directly
              //    reference the CTE relation. The bottom-most CTE relation can be the current
              //    CTE relation if lineage is empty. This is in case the bottom-most CTE relation
              //    is not referenced at all and can be optimized out, and we need to decrease the
              //    ref counts for CTE relations that are referenced by it.
              //  - Update indirect outgoing ref sources of all the CTE relations on the
              //    lineage (except for the bottom-most one). It's important to record this lineage,
              //    so that we can decrease ref counts properly if any CTE relation on the lineage
              //    is not referenced at all. Assume the lineage is t1 -> t2 -> t3 -> t4-ref, t3 is
              //    the bottom-most CTE relation. If t1 or t2 turns out to be not referenced at all,
              //    we can trace down the lineage and finally decrease the ref count of t4 by t3.
              if (lineage.isEmpty) {
                cteMap(cteDef.id).increaseOutgoingRefCount(ref.cteId, 1)
              } else {
                cteMap(lineage.head).increaseOutgoingRefCount(ref.cteId, 1)
                lineage.appended(cteDef.id).sliding(2).foreach {
                  case Seq(source, parent) => cteMap(parent).addIndirectOutgoingRefSource(source)
                }
              }
            } else {
              collectCTERefs(ref, lineage :+ cteDef.id)
            }
          })
        }
        buildCTEMap(child, cteMap, collectCTERefs)

      case ref: CTERelationRef =>
        cteMap(ref.cteId) = cteMap(ref.cteId).withRefCountIncreased(1)
        collectCTERefs(ref, Nil)
      case _ =>
        if (plan.containsPattern(CTE)) {
          plan.children.foreach { child =>
            buildCTEMap(child, cteMap, collectCTERefs)
          }

          plan.expressions.foreach { expr =>
            if (expr.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
              expr.foreach {
                case e: SubqueryExpression => buildCTEMap(e.plan, cteMap, collectCTERefs)
                case _ =>
              }
            }
          }
        }
    }
  }

  /**
   * Cleans the CTE map by removing those CTEs that are not referenced at all and corrects those
   * CTE's reference counts where the removed CTE referred to.
   *
   * @param cteMap A mutable map that accumulates the CTEs and their reference information by CTE
   *               ids. Needs to be sorted to speed up cleaning.
   */
  private def cleanCTEMap(cteMap: mutable.SortedMap[Long, CTEReferenceInfo]): Unit = {
    cteMap.keys.toSeq.reverse.foreach { currentCTEId =>
      val refInfo = cteMap(currentCTEId)
      if (refInfo.refCount == 0) {
        decreaseUseLessRefCount(cteMap, refInfo)
      }
    }
  }

  private def decreaseUseLessRefCount(
      cteMap: mutable.SortedMap[Long, CTEReferenceInfo],
      refInfo: CTEReferenceInfo): Unit = {
    refInfo.outgoingRefs.foreach { case (referencedCTEId, uselessRefCount) =>
      cteMap(referencedCTEId) = cteMap(referencedCTEId).withRefCountDecreased(uselessRefCount)
    }
    // It's important to clear the `outgoingRefs` here, as we may hit the same CTE relation more
    // than once, if more than one CTE relations in the reference lineage have ref count 0. We must
    // avoid over decreasing the ref counts.
    refInfo.outgoingRefs.clear()
    refInfo.indirectOutgoingRefSources.foreach { sourceId =>
      decreaseUseLessRefCount(cteMap, cteMap(sourceId))
    }
  }

  private def inlineCTE(
      plan: LogicalPlan,
      cteMap: mutable.Map[Long, CTEReferenceInfo]): LogicalPlan = {
    plan match {
      case WithCTE(child, cteDefs) =>
        val notInlined = cteDefs.flatMap {cteDef =>
          val refInfo = cteMap(cteDef.id)
          if (refInfo.refCount > 0) {
            val newDef = refInfo.cteDef.copy(child = inlineCTE(refInfo.cteDef.child, cteMap))
            val inlineDecision = shouldInline(newDef, refInfo.refCount)
            cteMap(cteDef.id) = cteMap(cteDef.id).copy(
              cteDef = newDef, shouldInline = inlineDecision
            )
            if (!inlineDecision) {
              Seq(newDef)
            } else {
              Nil
            }
          } else if (keepDanglingRelations) {
            Seq(refInfo.cteDef)
          } else {
            Nil
          }
        }
        val inlined = inlineCTE(child, cteMap)
        if (notInlined.isEmpty) {
          inlined
        } else {
          // Retain the not-inlined CTE relations in place.
          WithCTE(inlined, notInlined)
        }

      case ref: CTERelationRef =>
        val refInfo = cteMap(ref.cteId)
        if (refInfo.shouldInline) {
          if (ref.outputSet == refInfo.cteDef.outputSet) {
            refInfo.cteDef.child
          } else {
            val ctePlan = DeduplicateRelations(
              Join(
                refInfo.cteDef.child,
                refInfo.cteDef.child,
                Inner,
                None,
                JoinHint(None, None)
              )
            ).children(1)
            val projectList = ref.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
              if (srcAttr.semanticEquals(tgtAttr)) {
                tgtAttr
              } else {
                Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
              }
            }
            Project(projectList, ctePlan)
          }
        } else {
          ref
        }

      case _ if plan.containsPattern(CTE) =>
        plan
          .withNewChildren(plan.children.map(child => inlineCTE(child, cteMap)))
          .transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
            case e: SubqueryExpression =>
              e.withNewPlan(inlineCTE(e.plan, cteMap))
          }

      case _ => plan
    }
  }
}

/**
 * The bookkeeping information for tracking CTE relation references.
 *
 * @param cteDef The CTE relation definition
 * @param refCount The number of incoming references to this CTE relation. This includes references
 *                 from other CTE relations and regular places.
 * @param outgoingRefs A mutable map that tracks outgoing reference counts to other CTE relations.
 * @param indirectOutgoingRefSources The ids of nested CTE relations within this CTE relation that
 *                                   provide outgoing references indirectly. For example:
 *                                   WITH
 *                                     t1 AS (...),
 *                                     t2 AS (
 *                                       WITH
 *                                         t3 AS (SELECT * FROM t1)
 *                                     )
 *                                   The t2 references t1 indirectly via t3. So t1 is part of
 *                                   outgoingRefs of t3, and t3 is an indirect outgoing ref source
 *                                   of t2.
 * @param shouldInline If true, this CTE relation should be inlined in the places that reference it.
 */
case class CTEReferenceInfo(
    cteDef: CTERelationDef,
    refCount: Int,
    outgoingRefs: mutable.Map[Long, Int],
    indirectOutgoingRefSources: mutable.Set[Long],
    shouldInline: Boolean) {

  def withRefCountIncreased(count: Int): CTEReferenceInfo = {
    copy(refCount = refCount + count)
  }

  def withRefCountDecreased(count: Int): CTEReferenceInfo = {
    copy(refCount = refCount - count)
  }

  def increaseOutgoingRefCount(cteDefId: Long, count: Int): Unit = {
    outgoingRefs(cteDefId) += count
  }

  def addIndirectOutgoingRefSource(cteDefId: Long): Unit = {
    indirectOutgoingRefSources += cteDefId
  }
}

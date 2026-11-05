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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION, SCALA_UDF}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  // The CTE definition child a UnionLoop was built from, to rebuild the loop with wider
  // self-reference types.
  private val LOOP_ORIGIN = TreeNodeTag[LogicalPlan]("unionLoopOrigin")

  // The self-reference types to rebuild a recursive CTE definition with.
  private val SELF_REFERENCE_TYPES = TreeNodeTag[Seq[DataType]]("selfReferenceTypes")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsAllPatterns(CTE)) {
      // A helper map definitionID->Definition. Used for non-recursive CTE definitions only, either
      // inherently non-recursive or that became non-recursive due to recursive CTERelationRef->
      // UnionLoopRef substitution. Bridges the gap between two CTE resolutions (one for WithCTE,
      // another for reference) by materializing the resolved definitions in first pass and
      // consuming them in the second.
      val cteDefMap = mutable.HashMap.empty[Long, CTERelationDef]
      resolveWithCTE(plan, cteDefMap)
    } else {
      plan
    }
  }

  private def resolveWithCTE(
      plan: LogicalPlan,
      cteDefMap: mutable.HashMap[Long, CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsDownWithPruning(_.containsAllPatterns(CTE)) {
      case withCTE @ WithCTE(_, cteDefs) =>
        val newCTEDefs = cteDefs.map(widenSelfReference).map {
          // cteDef in the first case is either recursive and all the recursive CTERelationRefs
          // are already substituted to UnionLoopRef in the previous pass, or it is not recursive
          // at all. In both cases we need to put it in the map in case it is resolved.
          // Second case is performing the substitution of recursive CTERelationRefs.
          case cteDef if !cteDef.hasSelfReferenceAsCTERef =>
            if (cteDef.resolved) {
              cteDefMap.put(cteDef.id, cteDef)
            }
            cteDef
          case cteDef if cteDef.hasSelfReferenceInAnchor || cteDef.hasSelfReferenceInSubCTE =>
            throw new AnalysisException(
              errorClass = "INVALID_RECURSIVE_CTE",
              messageParameters = Map.empty)
          case cteDef =>
            // Multiple self-references are not allowed within one cteDef.
            cteDef.child match {
              // If it is a supported recursive CTE query pattern (4 so far), extract the anchor and
              // recursive plans from the Union and rewrite Union with UnionLoop. The recursive CTE
              // references inside UnionLoop's recursive plan will be rewritten as UnionLoopRef,
              // using the output of the resolved anchor plan. The side effect of recursive
              // CTERelationRef->UnionLoopRef substitution is that `cteDef` that was originally
              // considered `recursive` is no more in the context of `cteDef.recursive` method
              // definition.
              //
              // Simple case of duplicating (UNION ALL) clause.
              case alias @ SubqueryAlias(_, Union(Seq(anchor, recursion), false, false)) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val refTypes = selfReferenceTypes(cteDef, anchor)
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None, Some(refTypes)),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth)
                  loop.setTagValue(LOOP_ORIGIN, cteDef.child)
                  cteDef.copy(child = alias.copy(child = loop))
                }

              // Simple case of duplicating (UNION ALL) clause.
              case alias @ SubqueryAlias(_, withCTE @ WithCTE(
                Union(Seq(anchor, recursion), false, false), innerCteDefs)) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val refTypes = selfReferenceTypes(cteDef, anchor)
                  // We need to check whether any of the inner CTEs has a self reference and replace
                  // it if needed
                  val newInnerCteDefs = innerCteDefs.map { innerCteDef =>
                    innerCteDef.copy(child = rewriteRecursiveCTERefs(
                      innerCteDef.child, anchor, cteDef.id, None, Some(refTypes)))
                  }
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None, Some(refTypes)),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth)
                  loop.setTagValue(LOOP_ORIGIN, cteDef.child)
                  cteDef.copy(child = alias.copy(child = withCTE.copy(
                    plan = loop, cteDefs = newInnerCteDefs)))
                }

              // The case of CTE name followed by a parenthesized list of column name(s), eg.
              // WITH RECURSIVE t(n).
              case alias @ SubqueryAlias(_,
                  columnAlias @ UnresolvedSubqueryColumnAliases(
                  colNames,
                  Union(Seq(anchor, recursion), false, false)
                )) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val refTypes = selfReferenceTypes(cteDef, anchor)
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(
                      recursion, anchor, cteDef.id, Some(colNames), Some(refTypes)),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth)
                  loop.setTagValue(LOOP_ORIGIN, cteDef.child)
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                }

              // The case of CTE name followed by a parenthesized list of column name(s), eg.
              // WITH RECURSIVE t(n).
              case alias @ SubqueryAlias(_,
                   columnAlias @ UnresolvedSubqueryColumnAliases(
                   colNames,
                   withCTE @ WithCTE(Union(Seq(anchor, recursion), false, false), innerCteDefs)
                )) =>
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val refTypes = selfReferenceTypes(cteDef, anchor)
                  // We need to check whether any of the inner CTEs has a self reference and replace
                  // it if needed
                  val newInnerCteDefs = innerCteDefs.map { innerCteDef =>
                    innerCteDef.copy(child = rewriteRecursiveCTERefs(
                      innerCteDef.child, anchor, cteDef.id, Some(colNames), Some(refTypes)))
                  }
                  val loop = UnionLoop(
                    cteDef.id,
                    anchor,
                    rewriteRecursiveCTERefs(
                      recursion, anchor, cteDef.id, Some(colNames), Some(refTypes)),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth)
                  loop.setTagValue(LOOP_ORIGIN, cteDef.child)
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(
                    child = withCTE.copy(plan = loop, cteDefs = newInnerCteDefs))))
                }

              // If the recursion is described with a UNION (deduplicating) clause then the
              // recursive term should not return those rows that have been calculated previously,
              // and we exclude those rows from the current iteration result.
              case alias @ SubqueryAlias(_,
                  Distinct(Union(Seq(anchor, recursion), false, false))) =>
                cteDef.failAnalysis(
                  errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
                  messageParameters = Map.empty)
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    ),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth
                  )
                  cteDef.copy(child = alias.copy(child = loop))
                }

              // UNION case with CTEs inside.
              case alias @ SubqueryAlias(_, withCTE @ WithCTE(
                   Distinct(Union(Seq(anchor, recursion), false, false)), innerCteDefs)) =>
                cteDef.failAnalysis(
                  errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
                  messageParameters = Map.empty)
                if (!anchor.resolved) {
                  cteDef
                } else {
                  // We need to check whether any of the inner CTEs has a self reference and replace
                  // it if needed
                  val newInnerCteDefs = innerCteDefs.map { innerCteDef =>
                    innerCteDef.copy(child = rewriteRecursiveCTERefs(
                      innerCteDef.child, anchor, cteDef.id, None))
                  }
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, None),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    ),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth
                  )
                  cteDef.copy(child = alias.copy(child = withCTE.copy(
                    plan = loop, cteDefs = newInnerCteDefs)))
                }

              // The case of CTE name followed by a parenthesized list of column name(s).
              case alias @ SubqueryAlias(_,
                  columnAlias@UnresolvedSubqueryColumnAliases(
                  colNames,
                  Distinct(Union(Seq(anchor, recursion), false, false))
                )) =>
                cteDef.failAnalysis(
                  errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
                  messageParameters = Map.empty)
                if (!anchor.resolved) {
                  cteDef
                } else {
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    ),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth
                  )
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(child = loop)))
                }

              // The case of CTE name followed by a parenthesized list of column name(s).
              case alias @ SubqueryAlias(_,
                   columnAlias@UnresolvedSubqueryColumnAliases(
                   colNames,
                   WithCTE(Distinct(Union(Seq(anchor, recursion), false, false)), innerCteDefs)
                )) =>
                cteDef.failAnalysis(
                  errorClass = "UNION_NOT_SUPPORTED_IN_RECURSIVE_CTE",
                  messageParameters = Map.empty)
                if (!anchor.resolved) {
                  cteDef
                } else {
                  // We need to check whether any of the inner CTEs has a self reference and replace
                  // it if needed
                  val newInnerCteDefs = innerCteDefs.map { innerCteDef =>
                    innerCteDef.copy(child = rewriteRecursiveCTERefs(
                      innerCteDef.child, anchor, cteDef.id, Some(colNames)))
                  }
                  val loop = UnionLoop(
                    cteDef.id,
                    Distinct(anchor),
                    Except(
                      rewriteRecursiveCTERefs(recursion, anchor, cteDef.id, Some(colNames)),
                      UnionLoopRef(cteDef.id, anchor.output, true),
                      isAll = false
                    ),
                    anchor.output.map(_.newInstance().exprId),
                    maxDepth = cteDef.maxDepth
                  )
                  cteDef.copy(child = alias.copy(child = columnAlias.copy(
                    child = withCTE.copy(plan = loop, cteDefs = newInnerCteDefs))))
                }

              case other =>
                // We do not support cases of sole Union (needs a SubqueryAlias above it), nor
                // Project (as UnresolvedSubqueryColumnAliases have not been substituted with the
                // Project yet), leaving us with cases of SubqueryAlias->Union and SubqueryAlias->
                // UnresolvedSubqueryColumnAliases->Union. The same applies to Distinct Union.
                throw new AnalysisException(
                  errorClass = "INVALID_RECURSIVE_CTE",
                  messageParameters = Map.empty)
            }
        }
        withCTE.copy(cteDefs = newCTEDefs)

      // This is a non-recursive reference to a definition.
      case ref: CTERelationRef if !ref.resolved =>
        cteDefMap.get(ref.cteId).map { cteDef =>
          // cteDef is certainly resolved, otherwise it would not have been in the map.
          CTERelationRef(
            cteDef.id, cteDef.resolved, cteDef.output, cteDef.isStreaming, maxRows = cteDef.maxRows,
              isUnlimitedRecursion = ref.isUnlimitedRecursion)
        }.getOrElse {
          ref
        }

      case other =>
        other.transformExpressionsWithPruning(_.containsAllPatterns(PLAN_EXPRESSION, CTE)) {
          case e: SubqueryExpression => e.withNewPlan(resolveWithCTE(e.plan, cteDefMap))
        }
    }
  }

  // Substitutes a recursive CTERelationRef with UnionLoopRef if CTERelationRef refers to cteDefId.
  // Assumes that `anchor` is already resolved. If `columnNames` is set (which happens if an
  // UnresolvedSubqueryColumnAliases is atop a Union) also places an UnresolvedSubqueryColumnAliases
  // node above UnionLoopRef. At some later stage UnresolvedSubqueryColumnAliases gets resolved to
  // a Project node. `refTypes` overrides the anchor's data types for the UnionLoopRef output.
  private def rewriteRecursiveCTERefs(
      recursion: LogicalPlan,
      anchor: LogicalPlan,
      cteDefId: Long,
      columnNames: Option[Seq[String]],
      refTypes: Option[Seq[DataType]] = None) = {
    val types = refTypes.getOrElse(anchor.output.map(_.dataType))
    recursion.transformUpWithSubqueriesAndPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive && r.cteId == cteDefId =>
        // We mark the output of UnionLoopRef as nullable as it may become NULL in the following
        // iterations.
        val output = anchor.output.zip(types).map { case (a, dt) =>
          a.newInstance().withNullability(true).withDataType(dt)
        }
        val ref = UnionLoopRef(r.cteId, output, false)
        columnNames.map(UnresolvedSubqueryColumnAliases(_, ref)).getOrElse(ref)
    }
  }

  // The anchor's types, unless widenSelfReference has found wider ones.
  private def selfReferenceTypes(cteDef: CTERelationDef, anchor: LogicalPlan): Seq[DataType] =
    cteDef.getTagValue(SELF_REFERENCE_TYPES).getOrElse(anchor.output.map(_.dataType))

  // A recursion can write NULLs into nested types that the self-reference declares non-nullable,
  // e.g. ARRAY(CAST(NULL AS INT)) for an ARRAY(1) anchor. Once the loop is resolved, widens the
  // nullability of such self-reference types to the anchor's and recursion's and rebuilds the
  // loop from the original definition. This repeats until no type widens.
  private def widenSelfReference(cteDef: CTERelationDef): CTERelationDef = {
    val loop = if (cteDef.hasSelfReferenceAsUnionLoopRef) {
      cteDef.child.collectFirst { case l: UnionLoop if l.id == cteDef.id => l }
    } else {
      None
    }
    loop.filter(_.resolved) match {
      case Some(l) =>
        val refTypes = cteDef.child.collectFirstWithSubqueries {
          case r: UnionLoopRef if r.loopId == cteDef.id => r.output.map(_.dataType)
        }.get
        val children = Seq(l.anchor, l.recursion).map(withUdfNullHandling)
        val widened = refTypes.zipWithIndex.map { case (dt, i) =>
          children.foldLeft(dt)((t, c) => StructType.unionLikeMerge(t, c.output(i).dataType))
        }
        if (widened == refTypes) {
          cteDef
        } else {
          l.getTagValue(LOOP_ORIGIN) match {
            case Some(child) =>
              val restored = cteDef.copy(child = child)
              restored.setTagValue(SELF_REFERENCE_TYPES, widened)
              restored
            case None =>
              throw SparkException.internalError("The self-reference of recursive CTE " +
                s"${cteDef.id} needs wider types ${widened.map(_.sql).mkString(", ")}, but the " +
                "loop has no origin to rebuild it from.")
          }
        }
      case None => cteDef
    }
  }

  // Primitive Scala UDFs only become nullable in the later "ScalaUDF Null Handling" batch. Applies
  // that batch to a copy, so the self-reference also covers the NULLs those UDFs write.
  private def withUdfNullHandling(plan: LogicalPlan): LogicalPlan = {
    if (plan.containsPattern(SCALA_UDF)) {
      val handled = UpdateAttributeNullability(HandleNullInputsForUDF(plan))
      if (handled.fastEquals(plan)) plan else withUdfNullHandling(handled)
    } else {
      plan
    }
  }

  /**
   * Throws error if self-reference is placed in places which are not allowed:
   * right side of left outer/semi/anti joins, left side of right outer joins,
   * in full outer joins and in aggregates
   */
  def checkIfSelfReferenceIsPlacedCorrectly(
      plan: LogicalPlan,
      cteId: Long,
      allowRecursiveRef: Boolean = true): Unit = {
    plan match {
      case Join(left, right, Inner, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef)
      case Join(left, right, Cross, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef)
      case Join(left, right, LeftOuter, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef = false)
      case Join(left, right, RightOuter, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef = false)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef)
      case Join(left, right, LeftSemi, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef = false)
      case Join(left, right, LeftAnti, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef = false)
      case Join(left, right, _, _, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(left, cteId, allowRecursiveRef = false)
        checkIfSelfReferenceIsPlacedCorrectly(right, cteId, allowRecursiveRef = false)
      case Aggregate(_, _, child, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(child, cteId, allowRecursiveRef = false)
      case Window(_, _, _, child, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(child, cteId, allowRecursiveRef = false)
      case Sort(_, _, child, _) =>
        checkIfSelfReferenceIsPlacedCorrectly(child, cteId, allowRecursiveRef = false)
      case r: UnionLoopRef if !allowRecursiveRef && r.loopId == cteId =>
        throw new AnalysisException(
          errorClass = "INVALID_RECURSIVE_REFERENCE.PLACE",
          messageParameters = Map.empty)
      case other =>
        other.children.foreach(checkIfSelfReferenceIsPlacedCorrectly(_, cteId, allowRecursiveRef))
    }
    plan.subqueries.foreach(checkIfSelfReferenceIsPlacedCorrectly(_, cteId, allowRecursiveRef))
  }
}

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

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StructType}

/**
 * Updates CTE references with the resolve output attributes of corresponding CTE definitions.
 */
object ResolveWithCTE extends Rule[LogicalPlan] {
  // The CTE definition child a UnionLoop was built from, to rebuild the loop with wider
  // self-reference types.
  private val LOOP_ORIGIN = TreeNodeTag[LogicalPlan]("unionLoopOrigin")

  // The self-reference types and nullability to rebuild a recursive CTE definition with.
  private val SELF_REFERENCE_TYPES = TreeNodeTag[Seq[(DataType, Boolean)]]("selfReferenceTypes")

  // The anchor and recursion a UnionLoop's self-reference types were last found stable for.
  private val VERIFIED_CHILDREN = TreeNodeTag[(LogicalPlan, LogicalPlan)]("unionLoopVerified")

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
        val newCTEDefs = cteDefs.map(widenSelfReference(_, cteDefMap, cteDefs)).map {
          // cteDef in the first case is either recursive and all the recursive CTERelationRefs
          // are already substituted to UnionLoopRef in the previous pass, or it is not recursive
          // at all. In both cases we need to put it in the map in case it is resolved.
          // Second case is performing the substitution of recursive CTERelationRefs.
          case cteDef if !cteDef.hasSelfReferenceAsCTERef =>
            if (cteDef.resolved && !awaitsEnclosingCheck(cteDef)) {
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
  // a Project node. `refTypes` overrides the anchor's data types and nullability for the
  // UnionLoopRef output.
  private def rewriteRecursiveCTERefs(
      recursion: LogicalPlan,
      anchor: LogicalPlan,
      cteDefId: Long,
      columnNames: Option[Seq[String]],
      refTypes: Option[Seq[(DataType, Boolean)]] = None): LogicalPlan = {
    val types = refTypes.getOrElse(outputTypes(anchor))
    recursion.transformUpWithSubqueriesAndPruning(_.containsPattern(CTE)) {
      case r: CTERelationRef if r.recursive && r.cteId == cteDefId =>
        val output = anchor.output.zip(types).map { case (a, (dt, nullable)) =>
          a.newInstance().withNullability(nullable).withDataType(dt)
        }
        val ref = UnionLoopRef(r.cteId, output, false)
        columnNames.map(UnresolvedSubqueryColumnAliases(_, ref)).getOrElse(ref)
      // An inner loop built earlier keeps the raw reference in its origin. Rewrite it there too,
      // on a new loop instance, so that rebuilding the inner loop keeps the UnionLoopRef.
      case l: UnionLoop if l.getTagValue(LOOP_ORIGIN).isDefined =>
        val origin = l.getTagValue(LOOP_ORIGIN).get
        val newOrigin = rewriteRecursiveCTERefs(origin, anchor, cteDefId, columnNames, refTypes)
        if (newOrigin eq origin) {
          l
        } else {
          val newLoop = l.copy()
          newLoop.copyTagsFrom(l)
          newLoop.setTagValue(LOOP_ORIGIN, newOrigin)
          newLoop
        }
    }
  }

  private def outputTypes(plan: LogicalPlan): IndexedSeq[(DataType, Boolean)] =
    plan.output.map(a => (a.dataType, a.nullable)).toIndexedSeq

  private def useAnchorNestedNullability: Boolean =
    conf.getConf(SQLConf.LEGACY_RECURSIVE_CTE_USE_ANCHOR_NESTED_NULLABILITY)

  // The anchor's types, unless widenSelfReference has found wider ones.
  private def selfReferenceTypes(
      cteDef: CTERelationDef,
      anchor: LogicalPlan): Seq[(DataType, Boolean)] = {
    if (useAnchorNestedNullability) {
      anchor.output.map(a => (a.dataType, true))
    } else {
      cteDef.getTagValue(SELF_REFERENCE_TYPES).getOrElse(outputTypes(anchor))
    }
  }

  // The self-reference starts with the anchor's types and nullability, but the recursion can
  // write NULLs that they do not allow, e.g. ARRAY(CAST(NULL AS INT)) for an ARRAY(1) anchor. Once
  // the loop is resolved, widens the self-reference's nullability, top-level and nested, to the
  // anchor's and recursion's, predicted on copies until nothing widens, and rebuilds the loop from
  // the original definition. The loop can reference the enclosing definitions in `cteDefMap`, its
  // siblings and its inner definitions.
  // TODO: Copies of other values that Spark types as nullable whatever their inputs, such as arr[0]
  // with ANSI mode off, still widen even if they never hold NULLs.
  private def widenSelfReference(
      cteDef: CTERelationDef,
      cteDefMap: collection.Map[Long, CTERelationDef],
      siblingDefs: Seq[CTERelationDef]): CTERelationDef = {
    val loop = if (cteDef.hasSelfReferenceAsUnionLoopRef && !useAnchorNestedNullability) {
      cteDef.child.collectFirst { case l: UnionLoop if l.id == cteDef.id => l }
    } else {
      None
    }
    loop.filter(l => l.resolved && !isVerified(l)) match {
      case Some(l) =>
        val refTypes = cteDef.child.collectFirstWithSubqueries {
          case r: UnionLoopRef if r.loopId == cteDef.id => outputTypes(r)
        }.get
        val innerDefs = cteDef.child.collectWithSubqueries { case w: WithCTE => w.cteDefs }.flatten
        val defs = (cteDefMap ++ (siblingDefs ++ innerDefs).map(d => d.id -> d)).toMap
        val prediction = new SelfReferencePrediction(cteDef.id, defs, innerDefs.map(_.id).toSet)
        val widened = if (refTypes.forall(isNullableEverywhere)) {
          refTypes
        } else {
          val anchorTypes = prediction.outputTypes(l.anchor, refTypes)
          @tailrec def fixpoint(types: Seq[(DataType, Boolean)]): Seq[(DataType, Boolean)] = {
            val next = merge(types, anchorTypes, prediction.outputTypes(l.recursion, types))
            if (next == types) types else fixpoint(next)
          }
          fixpoint(refTypes)
        }
        if (widened == refTypes) {
          // A prediction that missed a referenced definition, e.g. in a subquery's own analysis,
          // leaves the check to the analysis that sees it.
          if (prediction.sawAllDefinitions) {
            l.setTagValue(VERIFIED_CHILDREN, (l.anchor, l.recursion))
          }
          cteDef
        } else {
          l.getTagValue(LOOP_ORIGIN) match {
            case Some(child) =>
              // Rebuilt rather than patched in place: patching the resolved recursion could leave
              // bound attributes, e.g. outer references, narrower than the data.
              val restored = cteDef.copy(
                child = withCurrentInnerDefinitions(child, cteDef.child, cteDef.id))
              restored.setTagValue(SELF_REFERENCE_TYPES, widened)
              restored
            case None =>
              throw SparkException.internalError("The self-reference of recursive CTE " +
                s"${cteDef.id} needs wider types ${widened.map(_._1.sql).mkString(", ")}, but " +
                "the loop has no origin to rebuild it from.")
          }
        }
      case None => cteDef
    }
  }

  // Whether a subquery's own analysis left the recursive definition's loop unverified, because its
  // prediction missed a definition of the enclosing query. Its references then wait for the
  // enclosing analysis to verify or rebuild it, so they bind to its final output.
  private def awaitsEnclosingCheck(cteDef: CTERelationDef): Boolean =
    AnalysisContext.get.outerPlan.isDefined && cteDef.hasSelfReferenceAsUnionLoopRef &&
      !useAnchorNestedNullability &&
      cteDef.child.collectFirst { case l: UnionLoop if l.id == cteDef.id => l }
        .exists(!isVerified(_))

  private def isNullableEverywhere(t: (DataType, Boolean)): Boolean =
    t._2 && t._1 == t._1.asNullable

  // Whether the loop's self-reference types were found stable for its current children.
  private def isVerified(loop: UnionLoop): Boolean =
    loop.getTagValue(VERIFIED_CHILDREN).exists { case (anchor, recursion) =>
      (anchor eq loop.anchor) && (recursion eq loop.recursion)
    }

  private def merge(
      types: Seq[(DataType, Boolean)],
      outputs: IndexedSeq[(DataType, Boolean)]*): Seq[(DataType, Boolean)] =
    types.zipWithIndex.map { case (t, i) =>
      outputs.foldLeft(t) { case ((dt, nullable), output) =>
        (StructType.unionLikeMerge(dt, output(i)._1), nullable || output(i)._2)
      }
    }

  // `origin` with the CTEs nested in it brought up to date with `current`, so that rebuilding loop
  // `loopId` neither resolves nor widens inner loops again: a definition that does not read the
  // loop, directly or through another CTE, is taken as resolved in `current`, and a recursive one
  // that does is rebuilt with the self-reference types it has reached.
  private def withCurrentInnerDefinitions(
      origin: LogicalPlan,
      current: LogicalPlan,
      loopId: Long): LogicalPlan = {
    val currentDefs = current.collectWithSubqueries { case d: CTERelationDef => d.id -> d }.toMap
    if (currentDefs.isEmpty) {
      origin
    } else {
      def references(d: CTERelationDef): Set[Long] =
        d.child.collectWithSubqueries { case r: CTERelationRef => r.cteId }.toSet
      @tailrec def readingLoop(ids: Set[Long]): Set[Long] = {
        val more = currentDefs.values.filter(d => references(d).exists(ids)).map(_.id).toSet
        if (more.subsetOf(ids)) ids else readingLoop(ids ++ more)
      }
      val reading = readingLoop(currentDefs.values.filter { d =>
        d.child.collectFirstWithSubqueries { case r: UnionLoopRef if r.loopId == loopId => r }
          .isDefined
      }.map(_.id).toSet)
      val reached = currentDefs.values.flatMap { d =>
        d.child.collectFirstWithSubqueries {
          case r: UnionLoopRef if r.loopId == d.id => outputTypes(r)
        }.orElse(d.getTagValue(SELF_REFERENCE_TYPES)).map(d.id -> _)
      }.toMap
      origin.transformDownWithSubqueries {
        case d: CTERelationDef if !reading.contains(d.id) &&
            currentDefs.get(d.id).exists(_.resolved) =>
          currentDefs(d.id)
        case d: CTERelationDef if reached.contains(d.id) =>
          val child = d.child.collectFirst { case l: UnionLoop if l.id == d.id => l }
            .flatMap(_.getTagValue(LOOP_ORIGIN)).getOrElse(d.child)
          val restored = d.copy(child = child)
          restored.setTagValue(SELF_REFERENCE_TYPES, reached(d.id))
          restored
      }
    }
  }

  // Predicts the output types of copies of loop `loopId`'s anchor and recursion once later
  // analysis adds nullability (see withLaterNullability), reading the CTEs they reference from
  // `defs`. Only `innerDefIds` can read the self-reference.
  private class SelfReferencePrediction(
      loopId: Long,
      defs: Map[Long, CTERelationDef],
      innerDefIds: Set[Long]) {
    // Whether every CTE that the copies referenced had a resolved definition in `defs`.
    var sawAllDefinitions = true
    private val outerDefTypes = mutable.HashMap.empty[Long, IndexedSeq[(DataType, Boolean)]]

    // The output types of `plan` with `refTypes` for the self-reference. Fully nullable if the copy
    // does not resolve.
    def outputTypes(
        plan: LogicalPlan,
        refTypes: Seq[(DataType, Boolean)]): IndexedSeq[(DataType, Boolean)] = {
      val innerDefTypes = mutable.HashMap.empty[Long, IndexedSeq[(DataType, Boolean)]]
      def predict(p: LogicalPlan): IndexedSeq[(DataType, Boolean)] = {
        val copy = p.transformUpWithSubqueries {
          case r: UnionLoopRef if r.loopId == loopId =>
            r.copy(output = r.output.zip(refTypes).map { case (a, (dt, nullable)) =>
              a.withNullability(nullable).withDataType(dt)
            })
          case r: CTERelationRef if !r.recursive && !defs.get(r.cteId).exists(_.resolved) =>
            sawAllDefinitions = false
            r
          case r: CTERelationRef if !r.recursive =>
            val memo = if (innerDefIds.contains(r.cteId)) innerDefTypes else outerDefTypes
            val types = memo.getOrElse(r.cteId, {
              memo.put(r.cteId, ResolveWithCTE.outputTypes(r))
              val t = predict(defs(r.cteId).child)
              memo.put(r.cteId, t)
              t
            })
            r.copy(output = r.output.zip(types).map { case (a, (dt, nullable)) =>
              widenAttribute(a, dt, nullable)
            })
        }
        val handled = withLaterNullability(withPreciseNullability(copy))
        if (handled.resolved) {
          ResolveWithCTE.outputTypes(handled)
        } else {
          p.output.map(a => (a.dataType.asNullable, true)).toIndexedSeq
        }
      }
      predict(plan)
    }
  }

  // Retypes, in a copy, values whose declared nested nullability ignores their inputs: appending
  // or prepending an element, which never pads the array unless the legacy negative index is set,
  // and constant-index reads, which with ANSI mode on fail rather than return NULL for a missing
  // element.
  private def withPreciseNullability(plan: LogicalPlan): LogicalPlan = {
    def concat(arrays: Expression*): Option[Expression] = {
      val types = arrays.map(_.dataType)
      if (TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(types).isDefined) {
        Some(Concat(arrays))
      } else {
        None
      }
    }
    def isLiteralArray(array: Expression): Boolean = array match {
      case _: CreateArray | GetArrayStructFields(_: CreateArray, _, _, _, _) => true
      case _ => false
    }
    def legacyNegativeIndex(insert: Expression): Boolean =
      insert.asInstanceOf[ArrayInsert].legacyNegativeIndex
    // A non-foldable stand-in for a constant index, so the read's nullability comes from the array.
    def index(ordinal: Expression): Expression =
      AttributeReference("index", ordinal.dataType, nullable = false)()
    plan.transformUpWithSubqueries { case p =>
      p.transformExpressionsUp {
        case a @ ArrayAppend(array, item) if !legacyNegativeIndex(a.replacement) =>
          concat(array, CreateArray(Seq(item))).getOrElse(a)
        case a @ ArrayPrepend(array, item) =>
          concat(CreateArray(Seq(item)), array).getOrElse(a)
        case a @ ArrayInsert(array, Literal(1, IntegerType), item, _) =>
          concat(CreateArray(Seq(item)), array).getOrElse(a)
        case a @ ArrayInsert(array, Literal(-1, IntegerType), item, false) =>
          concat(array, CreateArray(Seq(item))).getOrElse(a)
        case g @ GetArrayItem(array, ordinal, true)
            if ordinal.foldable && !ordinal.nullable && !isLiteralArray(array) =>
          g.copy(ordinal = index(ordinal))
        case e @ ElementAt(array, ordinal, None, true)
            if array.dataType.isInstanceOf[ArrayType] && ordinal.foldable && !ordinal.nullable &&
              !isLiteralArray(array) =>
          e.copy(right = index(ordinal))
      }
    }
  }

  // Applies to a copy the nullability that later analysis adds: primitive Scala UDFs only become
  // nullable in the "ScalaUDF Null Handling" batch, and references keep the types they were
  // resolved with. So the self-reference also covers those NULLs. Subquery plans come first, so
  // that the operators reading them see their refreshed output.
  private def withLaterNullability(plan: LogicalPlan): LogicalPlan = {
    val withSubqueries = plan.transformAllExpressionsWithPruning(
        _.containsPattern(PLAN_EXPRESSION)) {
      case s: SubqueryExpression => s.withNewPlan(withLaterNullability(s.plan))
    }
    val handled = refreshAttributeTypes(HandleNullInputsForUDF(withSubqueries))
    if (handled.fastEquals(plan)) plan else withLaterNullability(handled)
  }

  // Like UpdateAttributeNullability, but also takes the data type of the child's output attribute
  // where it is more nullable. A cast carries NULLs through, so its target takes the nullability of
  // a more nullable input instead of rejecting it.
  private def refreshAttributeTypes(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case p if p.children.nonEmpty && p.childrenResolved =>
      val childOutput = AttributeMap(p.children.flatMap(_.output).map(a => a -> a))
      p.transformExpressionsUp {
        case a: AttributeReference if childOutput.contains(a) =>
          val c = childOutput(a)
          widenAttribute(a, c.dataType, c.nullable)
        case c: Cast if !c.resolved && c.childrenResolved =>
          c.copy(dataType = withNullabilityOf(c.dataType, c.child.dataType))
      }
  }

  private def widenAttribute(a: Attribute, dataType: DataType, nullable: Boolean): Attribute = {
    val dt = if (DataType.equalsIgnoreCompatibleNullability(a.dataType, dataType)) {
      dataType
    } else {
      a.dataType
    }
    a.withNullability(a.nullable || nullable).withDataType(dt)
  }

  // `to` with its nested positions nullable wherever `from`'s are.
  private def withNullabilityOf(to: DataType, from: DataType): DataType = (to, from) match {
    case (ArrayType(te, tn), ArrayType(fe, fn)) => ArrayType(withNullabilityOf(te, fe), tn || fn)
    case (MapType(tk, tv, tn), MapType(fk, fv, fn)) =>
      MapType(withNullabilityOf(tk, fk), withNullabilityOf(tv, fv), tn || fn)
    case (StructType(tf), StructType(ff)) if tf.length == ff.length =>
      StructType(tf.zip(ff).map { case (t, f) =>
        t.copy(
          dataType = withNullabilityOf(t.dataType, f.dataType),
          nullable = t.nullable || f.nullable)
      })
    case _ => to
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

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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{
  InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites multiple references to the same view into one `CTERelationDef` with multiple
 * `CTERelationRef`s, so the view's underlying plan is computed once (through exchange
 * reuse at the physical layer) instead of once per reference. The conversion trades that
 * recomputation for the shuffle `ReplaceCTERefWithRepartition` adds at each reference.
 * It is off by default ([[SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE]]) and does nothing
 * when exchange reuse is disabled, since the added shuffles would then not be shared.
 *
 * It runs first in `FinishAnalysis`, while `View` nodes are still present, so a definition
 * body taken from a subquery occurrence is cleaned by the rules that run next on the main
 * plan (`Optimizer.scala`).
 *
 * Which occurrences of one view may convert, and why hint differences and seedless
 * samples disqualify a body, is defined on `qualifies` and its helpers below.
 *
 * A view is analyzed by itself when created, so an outer reference that does not resolve
 * inside the body fails view analysis and never reaches this rule. Correlations internal
 * to the body resolve within the converted definition.
 */
object ConvertViewToMaterializedCTE extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = SQLConf.get
    if (!conf.getConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE) || !conf.exchangeReuseEnabled) {
      return plan
    }
    // FinishAnalysis re-runs the rule batch on Subquery roots and requires the result
    // to stay a Subquery; the top-level pass already covers views inside subqueries.
    if (plan.isInstanceOf[Subquery]) return plan
    val occurrences = plan.collectWithSubqueries { case v: View => v }
    if (occurrences.length < 2) return plan

    // input_file_name() and friends read the scanning task's current file block, which is
    // gone above the added shuffle. Leave the whole plan unchanged so they keep running on
    // the scan.
    val readsInputFile = plan.collectFirstWithSubqueries {
      case p if p.expressions.exists(_.exists {
        case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
        case _ => false
      }) => true
    }.nonEmpty
    if (readsInputFile) return plan

    // The rewrite below matches occurrences by view name (`desc.identifier`). Convert a
    // name only when every occurrence of it qualifies and shares one canonical body: a
    // captured Dataset can hold a view body that was replaced after another occurrence
    // captured it, and such an occurrence must not be rebound to the new definition.
    // Inner CTE ids are re-minted on each re-analysis; `normalizeCteIds` removes that
    // noise before the bodies are compared.
    val qualifiedIdentifiers = occurrences
      .groupBy(_.desc.identifier)
      .collect {
        case (identifier, occs)
          if qualifies(occs) && occs.map(occ =>
            normalizeCteIds(occ.child).canonicalized).distinct.length == 1 => identifier
      }
      .toSet
    if (qualifiedIdentifiers.isEmpty) return plan

    // transformUp visits nested views first, so each definition is appended before the
    // definitions that reference it. Every occurrence becomes a bare reference with its
    // own output attributes, so duplicated expressions that contained the same view stay
    // equal; later CTE rules rebind the attributes to the definition by position.
    val cteDefs = mutable.ArrayBuffer.empty[CTERelationDef]
    val defByGroup = mutable.HashMap.empty[TableIdentifier, (CTERelationDef, Seq[HintInfo])]

    val rewritten = plan.transformUpWithSubqueries {
      case v: View if qualifiedIdentifiers.contains(v.desc.identifier) =>
        // Hints at the top of the body bind to the consuming join, but
        // `EliminateResolvedHint` cannot walk into a `CTERelationDef`. Build the
        // definition from the hint-free body and re-attach those hints above every
        // reference. `qualifies` has already required the occurrences' hints to match.
        defByGroup.get(v.desc.identifier) match {
          case Some((cteDef, bodyHints)) =>
            withBodyHints(bodyHints, CTERelationRef(
              cteDef.id,
              _resolved = true,
              output = v.output,
              isStreaming = false,
              maxRows = cteDef.maxRows))

          case None =>
            // `forceSkipInline` is set below, once live references are counted.
            val (hintFreeBody, bodyHints) =
              EliminateResolvedHint.extractHintsFromPlan(v.child)
            val cteDef = CTERelationDef(hintFreeBody)
            defByGroup.put(v.desc.identifier, (cteDef, bodyHints))
            cteDefs += cteDef
            withBodyHints(bodyHints, CTERelationRef(
              cteDef.id,
              _resolved = true,
              output = v.output,
              isStreaming = false,
              maxRows = cteDef.maxRows))
        }
    }

    if (cteDefs.isEmpty) {
      plan
    } else {
      // Two or more live references can share the added shuffle, so materialize only
      // those. A dropped occurrence body, or a reference inside an unreferenced user
      // CTE, can leave fewer; keep those inlinable and let `InlineCTE` flatten them.
      val newDefIds = cteDefs.map(_.id).toSet
      val withDefs = attachDefs(rewritten, cteDefs.toSeq)
      val liveRefCounts = countLiveRefs(withDefs)
      withDefs.transformUpWithSubqueries {
        case d: CTERelationDef if newDefIds.contains(d.id) =>
          d.copy(forceSkipInline = liveRefCounts.getOrElse(d.id, 0) >= 2)
      }
    }
  }

  // References reachable from the query root, per definition. A reference inside a
  // definition counts only when that definition is itself live, matching `InlineCTE`.
  // The visited set stops a recursive CTE from being walked forever.
  private def countLiveRefs(plan: LogicalPlan): Map[Long, Int] = {
    // The region of a reference is its innermost enclosing definition's id, or
    // ROOT_REGION at the query root.
    val ROOT_REGION = -1L
    val outgoing = mutable.HashMap.empty[Long, mutable.ArrayBuffer[Long]]

    def walk(p: LogicalPlan, region: Long): Unit = {
      p match {
        case d: CTERelationDef => walk(d.child, d.id)
        case r: CTERelationRef =>
          outgoing.getOrElseUpdate(region, mutable.ArrayBuffer.empty) += r.cteId
        case _ => p.children.foreach(walk(_, region))
      }
      p.subqueries.foreach(walk(_, region))
    }
    walk(plan, ROOT_REGION)

    val liveRefCounts = mutable.HashMap.empty[Long, Int].withDefaultValue(0)
    val visitedDefs = mutable.HashSet.empty[Long]
    val pending = mutable.Queue.empty[Long] ++=
      outgoing.getOrElse(ROOT_REGION, mutable.ArrayBuffer.empty)
    while (pending.nonEmpty) {
      val target = pending.dequeue()
      liveRefCounts(target) += 1
      if (visitedDefs.add(target)) {
        pending ++= outgoing.getOrElse(target, mutable.ArrayBuffer.empty)
      }
    }
    liveRefCounts.toMap
  }

  // Re-analysis mints a fresh id for each inner `CTERelationDef`, so equal bodies would
  // otherwise canonicalize apart. Rewriting ids to appearance ordinals lets them compare
  // equal: the same shape is walked in the same order, so genuinely different wiring keeps
  // different ordinals. Recursive CTEs also re-mint `UnionLoop.id`, `UnionLoopRef.loopId`,
  // and `UnionLoop.outputAttrIds`; those are not rewritten, so WITH RECURSIVE bodies stay
  // distinct and are never converted.
  private def normalizeCteIds(plan: LogicalPlan): LogicalPlan = {
    val ids = mutable.HashMap.empty[Long, Long]
    plan.transformUpWithSubqueries {
      case d: CTERelationDef => d.copy(id = ids.getOrElseUpdate(d.id, ids.size))
      case r: CTERelationRef => r.copy(cteId = ids.getOrElseUpdate(r.cteId, ids.size))
    }
  }

  // Canonicalization drops `ResolvedHint`, so the hint check cannot come from the body
  // comparison; compare them here.
  private def hintsOf(v: View): Seq[HintInfo] =
    v.child.collectWithSubqueries { case h: ResolvedHint => h.hints }

  // Deterministic and non-streaming alone do not cover a seedless `Sample`: it has no
  // expressions, so `deterministic` is true, but `SampleExec` draws a fresh seed per
  // physical node. One shared definition would make every reference read one sample.
  private def qualifies(occs: Seq[View]): Boolean = {
    val first = occs.head
    occs.length >= 2 && occs.forall { v =>
      v.resolved &&
        v.child.deterministic &&
        !v.child.isStreaming &&
        v.child.collectWithSubqueries { case s: Sample if s.seed.isEmpty => s }.isEmpty &&
        !hasMainRegionSort(v.child) &&
        hintsOf(v) == hintsOf(first) &&
        v.desc.viewSQLConfigs == first.desc.viewSQLConfigs &&
        schemasAlign(first, v)
    }
  }

  // The added shuffle drops any order the body's main plan exposes, including sorts
  // reachable through `children`: under LIMIT/OFFSET, in inner views, in WITH
  // definitions, and in set-operation branches. Sorts inside subquery expressions can
  // choose which rows the body produces, but they do not order its output, and they are
  // not reachable through `children`.
  private def hasMainRegionSort(plan: LogicalPlan): Boolean =
    plan.isInstanceOf[Sort] || plan.children.exists(hasMainRegionSort)

  /**
   * Resolution renews a view's attributes and preserves column order, name, type, and
   * nullability. A mismatch would zip occurrences against the wrong columns and silently
   * change results.
   */
  private def schemasAlign(first: View, other: View): Boolean =
    first.output.length == other.output.length &&
      first.output.zip(other.output).forall { case (l, r) =>
        l.name == r.name && l.dataType == r.dataType && l.nullable == r.nullable
      }

  // The extracted hint list is top-down; foldRight re-nests it in that order above the
  // reference.
  private def withBodyHints(hints: Seq[HintInfo], site: LogicalPlan): LogicalPlan =
    hints.foldRight(site)((h, acc) => ResolvedHint(acc, h))

  /**
   * Places definitions where `CTESubstitution` would: into an existing top-level
   * `WithCTE`, onto a `CTEInChildren` command, or in a wrapping `WithCTE`. Subquery
   * expressions resolve against that top-level scope. Only the top-level pass attaches
   * definitions; `apply` returns early for `Subquery` roots.
   */
  private def attachDefs(plan: LogicalPlan, newDefs: Seq[CTERelationDef]): LogicalPlan =
    plan match {
      case WithCTE(child, cteDefs) => WithCTE(child, newDefs ++ cteDefs)
      case cmd: LogicalPlan with CTEInChildren => cmd.withCTEDefs(newDefs)
      case other => WithCTE(other, newDefs)
    }
}

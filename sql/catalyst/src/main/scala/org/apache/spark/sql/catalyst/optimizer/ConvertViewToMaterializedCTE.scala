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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites multiple references to the same view into a single `CTERelationDef` with multiple
 * `CTERelationRef`s, so that the view's underlying plan is computed once (through exchange
 * reuse at the physical layer) instead of once per reference.
 *
 * The rule runs in `FinishAnalysis`, immediately before `EliminateView`: after `EliminateView`
 * no `View` nodes remain and every reference site holds an independent copy of the view's plan.
 *
 * A converted definition always sets `forceSkipInline = true`; otherwise `InlineCTE` would
 * immediately flatten it back into duplicated subtrees (the definition body is deterministic
 * in every case we convert), making the rule a no-op.
 *
 * Only deterministic, batch views are eligible: a multi-reference CTE guarantees that its
 * definition is evaluated exactly once (even for non-deterministic definitions), while
 * multiple references to a non-deterministic view are evaluated independently today.
 * Converting such views would change query results.
 *
 * A view body may contain correlated subqueries whose outer references resolve to relations
 * inside the same body (e.g. `t WHERE x IN (SELECT y FROM s WHERE s.k = t.k)`). The view is
 * analyzed standalone when it is created, so an outer reference that does not resolve inside
 * the body fails view analysis and can never escape to the outer query. The converted
 * definition contains the whole body, so internal correlations resolve within it and these
 * bodies are safe to convert. `InlineCTE`'s rejection of boundary-crossing outer references
 * is only a generic safety net for non-view `forceSkipInline` producers.
 *
 * Each reference site gains a shuffle boundary added by `ReplaceCTERefWithRepartition` and
 * deduplicated by exchange reuse, so the conversion trades recomputation for a
 * shuffle plus reuse; it is therefore gated behind
 * [[SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE]] and off by default.
 */
object ConvertViewToMaterializedCTE extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.getConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE)) return plan
    // FinishAnalysis re-runs the rule batch on Subquery roots and requires the result
    // to stay a Subquery; the top-level pass already covers views inside subqueries.
    if (plan.isInstanceOf[Subquery]) return plan
    val occurrences = plan.collectWithSubqueries { case v: View => v }
    if (occurrences.length < 2) return plan

    // Admit an identifier only when every occurrence of it forms one qualifying group.
    // Grouping is by identifier first because the rewrite below matches occurrences by
    // identifier: an identifier carried by occurrences with divergent bodies (e.g. one
    // occurrence resolved against a view definition that was replaced after another
    // occurrence captured it), or whose occurrences fail qualification for any other
    // reason, must not convert at all - otherwise the rewrite would rebind those
    // occurrences against a definition their plan does not match, silently changing
    // query results.
    //
    // Inner CTE definition ids are normalized before the bodies are compared: a view
    // body is re-analyzed per occurrence, and the re-analysis re-substitutes the body's
    // inner CTEs, minting a fresh `CTERelationDef` id each time (e.g. for a SQL view
    // whose body has a WITH clause over a persistent table). The re-minted ids are the
    // only difference between such occurrences' bodies, so the normalization lets them
    // group together; bodies that differ in anything else still stay apart.
    val qualifiedIdentifiers = occurrences
      .groupBy(_.desc.identifier)
      .collect {
        case (identifier, occs)
          if qualifies(occs) && occs.map(occ =>
            normalizeCteIds(occ.child).canonicalized).distinct.length == 1 => identifier
      }
      .toSet
    if (qualifiedIdentifiers.isEmpty) return plan

    // Bottom-up rewrite: nested views are visited (and their definitions appended) before
    // the views containing them, so a referenced definition always precedes its referrer
    // in `cteDefs`. The first occurrence of a group creates the definition, replaced by a
    // bare reference; later occurrences are wrapped in a `Project` re-minting the
    // occurrence's ids from the definition output, so consumers above need no rewriting.
    val cteDefs = mutable.ArrayBuffer.empty[CTERelationDef]
    val defByGroup = mutable.HashMap.empty[TableIdentifier, CTERelationDef]

    val rewritten = plan.transformUpWithSubqueries {
      case v: View if qualifiedIdentifiers.contains(v.desc.identifier) =>
        defByGroup.get(v.desc.identifier) match {
          case Some(cteDef) =>
            // Later occurrence: re-bind the reference output to this occurrence's
            // attributes positionally. The group qualification has already asserted that
            // name, type and nullability align element-wise.
            val ref = CTERelationRef(
              cteDef.id,
              _resolved = true,
              output = cteDef.output,
              isStreaming = false,
              maxRows = cteDef.maxRows)
            Project(rebindingProjectList(v.output, cteDef.output), ref)

          case None =>
            // First occurrence: consumers above already reference this occurrence's
            // expression ids, which are exactly the definition output, so the bare
            // reference is output-compatible.
            val cteDef = CTERelationDef(v.child, forceSkipInline = true)
            defByGroup.put(v.desc.identifier, cteDef)
            cteDefs += cteDef
            CTERelationRef(
              cteDef.id,
              _resolved = true,
              output = v.child.output,
              isStreaming = false,
              maxRows = cteDef.maxRows)
        }
    }

    if (cteDefs.isEmpty) {
      plan
    } else {
      attachDefs(rewritten, cteDefs.toSeq)
    }
  }

  // Group by view identity: the rule dedupes references of the SAME view, not distinct
  // views with coincidentally equal bodies, and all occurrences of one view resolve
  // through the same (db-qualified) identifier. The canonicalized body comparison is a
  // precondition guard in the identifier admission above: if occurrences of one view
  // ever diverge structurally, we skip conversion instead of building a wrong shared
  // definition.
  //
  // Rewrites every inner CTE definition id and reference to an appearance ordinal, so
  // that bodies differing only in re-minted inner CTE ids canonicalize equal. Same tree
  // shapes are visited in the same order, so equal bodies map to equal ordinals, while
  // genuinely different structures (e.g. two sibling identical CTEs) still produce
  // distinct ordinals and stay distinguishable.
  private def normalizeCteIds(plan: LogicalPlan): LogicalPlan = {
    val ids = mutable.HashMap.empty[Long, Long]
    plan.transformUpWithSubqueries {
      case d: CTERelationDef => d.copy(id = ids.getOrElseUpdate(d.id, ids.size))
      case r: CTERelationRef => r.copy(cteId = ids.getOrElseUpdate(r.cteId, ids.size))
    }
  }

  private def qualifies(occs: Seq[View]): Boolean = {
    val first = occs.head
    occs.length >= 2 && occs.forall { v =>
      v.resolved &&
        v.child.deterministic &&
        !v.child.isStreaming &&
        !hasTopLevelSort(v.child) &&
        v.desc.viewSQLConfigs == first.desc.viewSQLConfigs &&
        schemasAlign(first, v)
    }
  }

  // The per-reference shuffle boundary is added above the definition, so a
  // top-level ORDER BY in the view body would be destroyed by it.
  private def hasTopLevelSort(plan: LogicalPlan): Boolean = plan match {
    case _: Sort => true
    case Project(_, child) => hasTopLevelSort(child)
    case Filter(_, child) => hasTopLevelSort(child)
    case SubqueryAlias(_, child) => hasTopLevelSort(child)
    case GlobalLimit(_, child) => hasTopLevelSort(child)
    case LocalLimit(_, child) => hasTopLevelSort(child)
    case _ => false
  }

  /**
   * Occurrences of the same view are produced by resolution-time attribute renewal, which
   * preserves column order, name, type and nullability. Assert the invariant element-wise
   * before zipping positions: silent wrong results are the failure mode we cannot tolerate.
   */
  private def schemasAlign(first: View, other: View): Boolean =
    first.output.length == other.output.length &&
      first.output.zip(other.output).forall { case (l, r) =>
        l.name == r.name && l.dataType == r.dataType && l.nullable == r.nullable
      }

  /**
   * Builds a project list that re-mints `target`'s attributes from the definition output
   * positionally, preserving names, expression ids, qualifiers and metadata so that
   * consumers referencing `target` resolve unchanged.
   */
  private def rebindingProjectList(target: Seq[Attribute], source: Seq[Attribute]): Seq[Alias] =
    target.zip(source).map { case (t, s) =>
      Alias(s, t.name)(exprId = t.exprId, qualifier = t.qualifier,
        explicitMetadata = Some(t.metadata))
    }

  /**
   * Attaches the new definitions at the scope root, mirroring how `CTESubstitution` groups
   * definitions: merged into an existing top-level `WithCTE`, spread onto command children
   * for plans implementing `CTEInChildren`, or wrapped around the plan otherwise. References
   * inside subquery expressions resolve against the top-level scope, as they do for regular
   * user-written CTEs. The top-level pass is the only one that attaches definitions: apply
   * returns early for `Subquery` roots, whose wrapper must be preserved for the caller.
   */
  private def attachDefs(plan: LogicalPlan, newDefs: Seq[CTERelationDef]): LogicalPlan =
    plan match {
      case WithCTE(child, cteDefs) => WithCTE(child, newDefs ++ cteDefs)
      case cmd: LogicalPlan with CTEInChildren => cmd.withCTEDefs(newDefs)
      case other => WithCTE(other, newDefs)
    }
}

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
    val occurrences = plan.collectWithSubqueries { case v: View => v }
    if (occurrences.length < 2) return plan

    // Group occurrences of the same view by their canonicalized body and captured SQL
    // configs. Occurrences of one view differ only in renewed expression ids, which
    // canonicalization normalizes away.
    val qualifiedGroups = occurrences.groupBy(groupKey).values.filter(qualifies)
    if (qualifiedGroups.isEmpty) return plan
    // Match by identifier during the transform, not by the full group key: the key embeds
    // the canonicalized body, and by the time an outer view is visited in the bottom-up
    // traversal, nested views inside its body have already been rewritten into
    // `CTERelationRef`s, so the body no longer canonicalizes to the key computed here.
    // An identifier mapping to more than one qualified group would mean occurrences whose
    // canonicalized bodies diverge; refuse conversion rather than rewrite all of them
    // against whichever definition happens to be visited first.
    val qualifiedIdentifiers = qualifiedGroups
      .groupBy(g => groupKeyOf(g)._1)
      .values
      .filter(_.size == 1)
      .map(_.head)
      .map(groupKeyOf(_)._1)
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
              isStreaming = v.child.isStreaming)
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
              isStreaming = v.child.isStreaming)
        }
    }

    if (cteDefs.isEmpty) {
      plan
    } else {
      attachDefs(rewritten, cteDefs.toSeq)
    }
  }

  private type GroupKey = (TableIdentifier, LogicalPlan)

  // Group by view identity, not by structural body fingerprint: the rule dedupes references
  // of the SAME view, not distinct views with coincidentally equal bodies. Because the
  // analyzer resolves each occurrence of one view through the same deterministic path, all
  // occurrences share a canonically equal body and (db-qualified) identifier, so identity
  // alone is sufficient and unambiguous. The canonicalized body is kept in the key as a
  // precondition guard: if a future change ever made two occurrences of one view diverge
  // structurally, we skip conversion instead of building a wrong shared definition.
  private def groupKey(v: View): GroupKey =
    (v.desc.identifier, v.child.canonicalized)

  private def groupKeyOf(occs: Seq[View]): GroupKey = groupKey(occs.head)

  private def qualifies(occs: Seq[View]): Boolean = {
    val first = occs.head
    occs.length >= 2 && occs.forall { v =>
      v.resolved &&
        v.child.deterministic &&
        !v.child.isStreaming &&
        v.desc.viewSQLConfigs == first.desc.viewSQLConfigs &&
        schemasAlign(first, v)
    }
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
   * user-written CTEs.
   */
  private def attachDefs(plan: LogicalPlan, newDefs: Seq[CTERelationDef]): LogicalPlan =
    plan match {
      case WithCTE(child, cteDefs) => WithCTE(child, newDefs ++ cteDefs)
      case cmd: LogicalPlan with CTEInChildren => cmd.withCTEDefs(newDefs)
      case other => WithCTE(other, newDefs)
    }
}

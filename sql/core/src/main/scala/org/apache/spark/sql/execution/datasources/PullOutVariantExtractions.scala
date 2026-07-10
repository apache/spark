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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Cast, Expression,
  NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.VariantType

/**
 * Hoists variant extractions out of operators that variant-into-scan pushdown cannot see through,
 * into a [[Project]] directly below that operator, so the extraction becomes visible to the
 * pushdown rules ([[PushVariantIntoScan]] for v1 and
 * [[org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown]] for v2). The rule
 * applies uniformly to all three table read paths: DS1 (e.g. `spark.read.parquet`), DS2 (e.g.
 * `spark.read.parquet` with `spark.sql.sources.useV1SourceList` cleared), and Spark native
 * catalog tables (managed or external tables created with `CREATE TABLE ... USING PARQUET`). In
 * every case the downstream pushdown rule sees the hoisted extraction in the
 * `Project`/`Filter` chain that [[org.apache.spark.sql.catalyst.planning.PhysicalOperation]]
 * collapses above the scan; an extraction embedded in an `Aggregate`/`Join`/`Sort` is invisible
 * to them and the whole variant column is read raw (or, when the column is also read raw for
 * pass-through, shredded with a wasteful full-variant slot).
 *
 * This complements the rules that already relocate variant extractions into such a `Project`:
 * `PhysicalOperation` (Project/Filter), `PullOutGroupingExpressions` (GROUP BY / DISTINCT keys),
 * and `ExtractWindowExpressions` (window partition/order keys and window function arguments). The
 * residual, un-relocated extractions live in aggregate function arguments, join conditions, and
 * sort orders. This rule handles those three:
 *
 *  - Aggregate: `Aggregate([name], [name, max(variant_get(v, '$.p'))], child)` becomes
 *    `Aggregate([name], [name, max(_ve0)], Project([name, variant_get(v, '$.p') AS _ve0], child))`.
 *    The Aggregate defines its own output, so the raw `v` is simply not passed through.
 *
 *  - Sort / Join: these operators pass their child's columns through, so a naive hoist would keep
 *    a bare `v` alongside the hoisted extraction and the pushdown would still request the full
 *    variant. To avoid that we match the `Project` sitting above the barrier -- its `references`
 *    are exactly the columns still live above the barrier -- and drop a variant column that is no
 *    longer referenced once its extraction has been hoisted. A variant genuinely needed raw above
 *    the barrier stays in `references` and is preserved (keeping its full-variant slot).
 *
 *    A `Join`, and a `Sort` directly under a narrowing `Project`, are handled with that Project's
 *    `references` telling us which columns remain live: a variant used only via the hoisted
 *    extraction is dropped (no redundant full-variant slot). This is the common `Sort` shape when
 *    only a subset of columns is selected (e.g. `SELECT name ... ORDER BY variant_get(v, ...)`),
 *    where the analyzer adds a `Project` above the `Sort` to project away the order-only columns.
 *
 *    A bare/root `Sort` -- one with no narrowing `Project` above it, as in
 *    `SELECT * FROM t ORDER BY variant_get(v, '$.b')` (all order columns are already selected, so
 *    the analyzer adds no narrowing `Project`) -- is also handled, by a second `transformUp` pass
 *    (see `apply`). Its output must equal its child's output exactly, so ALL child columns stay
 *    live: a variant present in the output stays raw AND gains an extra `_ve` slot, producing a
 *    multi-slot shredded struct with a full-variant slot -- the same shape as a `Sort`-under-
 *    `Project` whose `v` is also selected. `Sort`s under a `GlobalLimit`/`LocalLimit` (a
 *    `SELECT * ... ORDER BY ... LIMIT n`) are covered by the same second pass. A `Sort`/`Join` in
 *    any other shape -- under a `Filter`/`Aggregate`/another non-`Project` operator that is not a
 *    limit wrapper -- is left unchanged: the extraction stays in place, the variant is read raw,
 *    and the plan is correct (just not shredded). When the `Sort` itself sits over a `Join`, the
 *    hoisted order-key aliases are additionally pushed *through* the join to the scan side (see the
 *    through-a-`Join` note below), not merely parked in a `Project` above it.
 *
 * Pushing extractions *through* a `Join`: hoisting an extraction into a `Project`/child directly
 * above a `Join` is not enough on its own, because `PhysicalOperation` (which both pushdown rules
 * rely on) collapses only a contiguous `Project`/`Filter` chain and stops at the `Join`. A variant
 * column `v` that reaches the barrier only via a hoisted `variant_get` would still flow up through
 * the join as a bare attribute and be read raw at the scan. So once an extraction is hoisted above
 * a `Join` (by the `Aggregate` or `Sort` case, or by the projection/condition handling of the
 * `Project`-over-`Join` case), `pushSideAliases` pushes the resulting `_ve` aliases *down* through
 * the join tree, routing each to the side whose output owns the referenced attribute, until it
 * lands in a `Project` directly above a non-join child (the scan side). This descends any depth of
 * chained joins in a single pass, so a variant fact-table column joined to several dimensions
 * shreds to just the requested typed fields. Correctness is per join type: `Inner`/`Cross` and the
 * outer joins (`LeftOuter`/`RightOuter`/`FullOuter`) push to either side -- the outer nullable side
 * is value-preserving because `variant_get(NULL) = NULL` and null-padding commutes with the
 * extraction. `LeftSemi`/`LeftAnti`/`ExistenceJoin` push only left-side aliases (their right side
 * is not in the join output); a right-side alias on such a join is not pushed and the parent keeps
 * a `Project` referencing it above the join. A variant still needed raw above the join stays live
 * and keeps its full-variant slot, exactly as in the non-join cases.
 *
 * Every rewrite is a plain alias/`Project` introduction and is semantics-preserving; the pushdown
 * shred/rewrite machinery is unchanged. The rule is gated on
 * [[SQLConf.PUSH_VARIANT_INTO_SCAN_PULL_OUT_EXTRACTIONS]] (and is a no-op unless
 * [[SQLConf.PUSH_VARIANT_INTO_SCAN]] is also enabled) and only fires when a hoistable extraction is
 * present, so non-variant plans are untouched.
 */
object PullOutVariantExtractions extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Hoisting is only useful when variant-into-scan pushdown is enabled to consume the relocated
    // extractions, and is independently gated so it can be turned off on its own.
    if (!SQLConf.get.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN) ||
        !SQLConf.get.getConf(SQLConf.PUSH_VARIANT_INTO_SCAN_PULL_OUT_EXTRACTIONS)) {
      plan
    } else {
      // First pass: handle operators where a narrowing Project above the barrier tells us which
      // columns remain live. `transformUp` so inner operators are rewritten before their parents.
      // Sort/Join are matched together with the Project above them (see class doc).
      val afterProjectCases = plan.transformUp {
        case agg: Aggregate => rewriteAggregate(agg)
        case p @ Project(projectList, s: Sort) => rewriteSortUnderProject(p, projectList, s)
        case p @ Project(projectList, j: Join) => rewriteJoinUnderProject(p, projectList, j)
      }
      // Second pass: handle a Sort NOT under a narrowing Project -- a bare/root Sort (as in
      // `SELECT * ORDER BY variant_get(...)`), or one under a GlobalLimit/LocalLimit. This must be
      // a separate pass, not another arm in the first `transformUp`: that traversal is bottom-up,
      // so a bare-Sort arm would fire on the inner Sort of a `Project(_, Sort)` shape before the
      // parent Project is visited, keeping v raw and pre-empting `rewriteSortUnderProject`. After
      // the first pass, any Sort that was under a Project has its order rewritten to non-hoistable
      // `_ve` attribute references, so `rewriteBareSort` is a no-op on it (idempotent).
      afterProjectCases.transformUp {
        case s: Sort => rewriteBareSort(s)
      }
    }
  }

  // A variant column path: an `Attribute` (possibly through `GetStructField`) typed `VariantType`.
  // Used by `isHoistable` to verify the extraction's source is a scan column, not a computed
  // expression. Non-column variants (e.g. `variant_get(parse_json(x), ...)`) are excluded.
  private def isVariantColumnPath(e: Expression): Boolean =
    e.dataType.isInstanceOf[VariantType] && StructPath.unapply(e).isDefined

  private def isHoistable(e: Expression): Boolean = e match {
    // Exclude the two-arg form variant_get(v, path) whose targetType is VariantType: it produces a
    // variant-typed alias that the pushdown drops (see SPARK-57499), so hoisting adds a Project for
    // no benefit. It would also break Once-batch idempotence: the hoisted `_ve0` is a VariantType
    // attribute, so a wrapping `cast(_ve0 as string)` (e.g. from `v:a::string`, which desugars to
    // `cast(variant_get(v, '$.a') as string)`) would match the Cast branch below on the next pass.
    case g: VariantGet =>
      !g.targetType.isInstanceOf[VariantType] && g.path.foldable && isVariantColumnPath(g.child)
    // A variant-to-variant `Cast` (e.g. `cast(v as variant)`) is a whole-variant read: its sole
    // requested field targets `VariantType`, which the pushdown drops (see SPARK-57499) because a
    // lone variant-typed slot saves no I/O and is mishandled by the reader. Hoisting it would add a
    // Project + alias for no shredding benefit and leave the column raw anyway, so exclude it.
    // A hoistable `Cast` yields a non-variant-typed alias, so it is never re-hoisted (idempotent).
    case c: Cast => !c.dataType.isInstanceOf[VariantType] && isVariantColumnPath(c.child)
    case _ => false
  }

  /**
   * Collects hoisted extractions as aliases, de-duplicated by canonical form so a repeated
   * extraction maps to a single output slot.
   */
  private class ExtractionHoister {
    private val extracted = mutable.LinkedHashMap.empty[Expression, Alias]

    def aliases: Seq[NamedExpression] = extracted.values.toSeq

    def isEmpty: Boolean = extracted.isEmpty

    /** Returns (creating if needed) the alias attribute for a single hoistable extraction. */
    def aliasFor(ex: Expression): Attribute =
      extracted.getOrElseUpdate(ex.canonicalized, Alias(ex, s"_ve${extracted.size}")()).toAttribute

    /** Replaces every hoistable extraction in `e` with a reference to its (new) alias. */
    def hoist(e: Expression): Expression = e.transformDown {
      case ex if isHoistable(ex) => aliasFor(ex)
    }
  }

  // Recursively pushes hoisted extraction aliases down through a join tree until each lands in a
  // `Project` directly above a non-join child (the scan side, or the `Filter`/`Project` chain above
  // it that `PhysicalOperation` collapses). Hoisting an extraction into a `Project` above a `Join`
  // is not sufficient on its own, because `PhysicalOperation` stops at the `Join`: the variant
  // would still be read raw at the scan. This descends any depth of chained joins in a single call.
  //
  // `aliases` are the extraction aliases still to be placed. Each is routed to the join side whose
  // output owns its referenced attribute. `needed` is the set of attributes that must remain in the
  // pushed-into child's output -- the columns referenced above the push point plus the
  // join-condition references accumulated while recursing, so join keys are never dropped. A
  // variant consumed solely via a hoisted extraction is thereby not passed through (no redundant
  // full-variant slot); one used raw elsewhere stays in `needed` and keeps its full-variant slot.
  private def pushSideAliases(
      child: LogicalPlan,
      aliases: Seq[NamedExpression],
      needed: AttributeSet): LogicalPlan = {
    if (aliases.isEmpty) {
      child
    } else {
      child match {
        case join: Join =>
          val leftOutput = join.left.outputSet
          val rightOutput = join.right.outputSet
          // For LeftSemi/LeftAnti the right side is not in the join output, so a right-side alias
          // could never be referenced above the join. Route only left-side aliases there; leave any
          // right-side alias in place (it will not have been produced for such joins in practice).
          val rightEligible = join.joinType match {
            case LeftExistence(_) => false
            case _ => true
          }
          val (leftAliases, rest) =
            aliases.partition(_.references.subsetOf(leftOutput))
          val (rightAliases, unrouted) =
            if (rightEligible) rest.partition(_.references.subsetOf(rightOutput))
            else (Seq.empty[NamedExpression], rest)
          if (leftAliases.isEmpty && rightAliases.isEmpty) {
            // Nothing cleanly routable (e.g. an ExistenceJoin, or an alias not confined to one
            // side). Fall back to a Project above the join keeping the live columns.
            val keep = join.output.filter(needed.contains)
            Project(keep ++ aliases, join)
          } else {
            val neededHere =
              needed ++ join.condition.map(_.references).getOrElse(AttributeSet.empty)
            val newLeft = pushSideAliases(join.left, leftAliases, neededHere)
            val newRight = pushSideAliases(join.right, rightAliases, neededHere)
            val newJoin = join.copy(left = newLeft, right = newRight)
            // An alias that could not be routed to a single side stays above the join.
            if (unrouted.isEmpty) {
              newJoin
            } else {
              val keep = newJoin.output.filter(needed.contains)
              Project(keep ++ unrouted, newJoin)
            }
          }
        // Descend through a pass-through `Project` (as `PhysicalOperation` does) to reach the
        // join/scan beneath the intermediate Projects the optimizer leaves between chained joins.
        // A `pushable` alias resolves against the grandchild and is pushed there; a `stay` alias
        // references a column this Project introduces and cannot be pushed.
        case project @ Project(projectList, grandChild) =>
          val (pushable, stay) =
            aliases.partition(_.references.subsetOf(grandChild.outputSet))
          if (pushable.isEmpty) {
            val keep = project.output.filter(needed.contains)
            Project(keep ++ aliases, project)
          } else {
            // Prune to the still-live columns before re-adding the pushed alias references, else a
            // bare variant consumed only via a now-pushed extraction would stay live and be read
            // raw. Retain any projection a `stay` alias references so it stays resolvable.
            val stayRefs = AttributeSet(stay.flatMap(_.references))
            val keptProjections = projectList.filter { e =>
              needed.contains(e.toAttribute) || stayRefs.contains(e.toAttribute)
            }
            val newGrandChild = pushSideAliases(
              grandChild, pushable, needed ++ AttributeSet(keptProjections.flatMap(_.references)))
            Project(keptProjections ++ pushable.map(_.toAttribute) ++ stay, newGrandChild)
          }
        // Any other operator -- notably a `Filter` above the scan: wrap a `Project` above it. We do
        // NOT descend through a `Filter`, as that would force its variant predicate reference into
        // `needed` and pass the raw variant through. The Filter and this Project collapse into the
        // same `PhysicalOperation` chain, so the hoisted extraction is still seen at the scan.
        case other =>
          val keep = other.output.filter(needed.contains)
          Project(keep ++ aliases, other)
      }
    }
  }

  // Pushes hoisted extraction aliases into a `Join` child: routes each alias to the join side whose
  // output owns its referenced attribute and recurses via `pushSideAliases` so it lands in a
  // `Project` directly above that side's scan (descending nested joins). The join propagates the
  // `_ve` aliases up through its output, where the parent operator (Aggregate/Sort) references
  // them.
  // A `Project` wrapper above the join would leave the aliases invisible to the pushdown
  // (`PhysicalOperation` stops at the join) and would not be revisited by the outer `transformUp`,
  // so we push down instead.
  //
  // `needed` is the set of attributes that must remain live above the join (what the parent
  // references). The join condition's references are added so join keys are never dropped. If any
  // alias cannot be routed to exactly one pushable side -- a right-side alias on a `LeftExistence`
  // join, or one that straddles both sides -- we fall back to wrapping a `Project` of the live
  // columns plus every alias above the join, which keeps the parent's `_ve` references resolved (at
  // the cost of reading that side's variant raw).
  private def pushAliasesIntoJoin(
      join: Join,
      aliases: Seq[NamedExpression],
      needed: AttributeSet): LogicalPlan = {
    val leftOutput = join.left.outputSet
    val rightOutput = join.right.outputSet
    // For LeftSemi/LeftAnti/ExistenceJoin the right side is not in the join output, so a right-side
    // alias could never be referenced above the join; only the left side is pushable.
    val rightEligible = join.joinType match {
      case LeftExistence(_) => false
      case _ => true
    }
    val leftAliases = aliases.filter(_.references.subsetOf(leftOutput))
    val rightAliases =
      if (rightEligible) {
        aliases.filter(a =>
          !a.references.subsetOf(leftOutput) && a.references.subsetOf(rightOutput))
      } else {
        Seq.empty[NamedExpression]
      }
    if (leftAliases.size + rightAliases.size != aliases.size) {
      val keep = join.output.filter(needed.contains)
      Project(keep ++ aliases, join)
    } else {
      val neededHere = needed ++ join.condition.map(_.references).getOrElse(AttributeSet.empty)
      join.copy(
        left = pushSideAliases(join.left, leftAliases, neededHere),
        right = pushSideAliases(join.right, rightAliases, neededHere))
    }
  }

  // Routes hoistable extractions found in `projectList` to the owning-side hoister and returns the
  // projectList with those extractions replaced by references to their (new) alias attributes.
  // Mirrors the join-condition routing in `rewriteJoinUnderProject`: a single variant extraction
  // references exactly one attribute, hence one join side. Anything not cleanly on one side is left
  // in place.
  private def hoistProjectListExtractions(
      projectList: Seq[NamedExpression],
      leftOutput: AttributeSet,
      rightOutput: AttributeSet,
      leftHoister: ExtractionHoister,
      rightHoister: ExtractionHoister): Seq[NamedExpression] = {
    projectList.map { e =>
      e.transformDown {
        case ex if isHoistable(ex) =>
          if (ex.references.subsetOf(leftOutput)) {
            leftHoister.aliasFor(ex)
          } else if (ex.references.subsetOf(rightOutput)) {
            rightHoister.aliasFor(ex)
          } else {
            ex
          }
      }.asInstanceOf[NamedExpression]
    }
  }

  private def rewriteAggregate(agg: Aggregate): LogicalPlan = {
    val hoister = new ExtractionHoister
    // Only hoist extractions that sit inside an aggregate function's arguments (or filter). A
    // top-level extraction in `aggregateExpressions` is a grouping-key reference (grouping keys are
    // already pulled out by `PullOutGroupingExpressions`); hoisting it would leave the `Aggregate`
    // referencing a column that is neither grouped nor aggregated.
    val newAggExprs = agg.aggregateExpressions.map {
      _.transform {
        case ae: AggregateExpression => ae.withNewChildren(ae.children.map(hoister.hoist))
      }.asInstanceOf[NamedExpression]
    }

    if (hoister.isEmpty) {
      agg
    } else {
      // `referenced` is what the rewritten Aggregate still needs directly (aggregate-function args
      // after hoisting, plus grouping keys). A variant consumed solely via a hoisted extraction is
      // not in it and so is dropped rather than passed through raw -- see the class doc.
      val referenced = AttributeSet(
        newAggExprs.flatMap(_.references) ++ agg.groupingExpressions.flatMap(_.references))
      val newChild = agg.child match {
        // Fuse into the Project that PullOutGroupingExpressions/PhysicalOperation already placed
        // below the Aggregate; the Once-batch earlyScanPushDownRules has no CollapseProject to
        // flatten a second stacked Project. Sound only when the fused Project still resolves
        // against `grandChild.output` (see `canFuseIntoChildProject`); else keep the nested one.
        case Project(projectList, grandChild)
            if canFuseIntoChildProject(projectList, referenced, hoister, grandChild) =>
          val keptProjections = projectList.filter(e => referenced.contains(e.toAttribute))
          val fused = Project(keptProjections ++ hoister.aliases, grandChild)
          grandChild match {
            // The fused Project sits directly above a Join, which `PhysicalOperation` cannot see
            // through -- and this newly created node will not be revisited by the outer
            // `transformUp` in this pass. Push the hoisted aliases the rest of the way down through
            // the join immediately, reusing the Project-over-Join handling.
            case grandChildJoin: Join =>
              rewriteJoinUnderProject(fused, fused.projectList, grandChildJoin)
            case _ => fused
          }
        // No fusable child Project. Only the still-referenced columns plus the hoisted aliases
        // reach the child, so a variant used solely via a hoisted extraction is dropped.
        case join: Join =>
          // The Aggregate's child is a Join. Push the hoisted aliases down through the join tree
          // onto the owning side(s), where they land directly above the scan; the Aggregate
          // references the `_ve` aliases, which the join propagates up through its output. Any
          // variant still needed raw (in `referenced`) stays live. See `pushAliasesIntoJoin`.
          pushAliasesIntoJoin(join, hoister.aliases, referenced)
        case other =>
          val passthrough = other.output.filter(referenced.contains)
          Project(passthrough ++ hoister.aliases, other)
      }
      agg.copy(aggregateExpressions = newAggExprs, child = newChild)
    }
  }

  // Fusing the hoisted aliases into an existing child `Project` (collapsing it onto its own child)
  // is sound only if everything the fused Project carries still resolves against `grandChild`:
  //  - the retained projections (those whose output is still referenced), and
  //  - the hoisted alias expressions.
  // If any of these references an attribute the Project introduces itself (not present in
  // `grandChild.output`) -- e.g. a nested-variant extraction materialized as an intermediate alias
  // -- collapsing would leave that reference unresolved, so we must not fuse.
  private def canFuseIntoChildProject(
      projectList: Seq[NamedExpression],
      referenced: AttributeSet,
      hoister: ExtractionHoister,
      grandChild: LogicalPlan): Boolean = {
    val grandChildOutput = grandChild.outputSet
    val keptProjections = projectList.filter(e => referenced.contains(e.toAttribute))
    val carried = keptProjections ++ hoister.aliases
    carried.forall(_.references.subsetOf(grandChildOutput))
  }

  private def rewriteSortUnderProject(
      project: Project, projectList: Seq[NamedExpression], sort: Sort): LogicalPlan = {
    val hoister = new ExtractionHoister
    val newOrder = sort.order.map(hoister.hoist(_).asInstanceOf[SortOrder])
    if (hoister.isEmpty) {
      project
    } else {
      // Columns still needed from the Sort's child: what the Project above consumes, plus what the
      // rewritten order references directly (the alias attributes are supplied below, not from the
      // child). A variant used only by the (now hoisted) order key is thereby dropped.
      val needed =
        AttributeSet(projectList.flatMap(_.references) ++ newOrder.flatMap(_.references))
      // `pushSideAliases` places the aliases in a Project directly above the child's non-join
      // descendant, descending through any Join/Project chain between the Sort and the scan. For a
      // flat child (scan / Filter above a scan) this is a single Project directly below the Sort,
      // matching the previous behavior; when the Sort sits over a Join the aliases are pushed
      // through it so the extraction reaches the scan instead of being read raw.
      val newChild = pushSideAliases(sort.child, hoister.aliases, needed)
      // Reproduce the original projectList so the alias columns do not leak into the output.
      project.copy(child = sort.copy(order = newOrder, child = newChild))
    }
  }

  // Handles a `Sort` that is NOT directly under a narrowing `Project` -- a bare/root Sort such as
  // `SELECT * FROM t ORDER BY variant_get(v, '$.b')` (all order columns are already selected, so
  // the analyzer adds no narrowing Project above the Sort), or a Sort under a
  // GlobalLimit/LocalLimit.
  // Unlike `rewriteSortUnderProject`, there is no Project above to tell us which columns remain
  // live; a bare Sort's output IS its child's output, so ALL child columns must stay live. A
  // variant present in the output (SELECT *) therefore stays raw AND gains an extra `_ve` slot,
  // yielding a multi-slot shredded struct with a full-variant slot -- the same shape as a
  // Sort-under-Project whose `v` is also selected. See the class doc.
  private def rewriteBareSort(sort: Sort): LogicalPlan = {
    val hoister = new ExtractionHoister
    val newOrder = sort.order.map(hoister.hoist(_).asInstanceOf[SortOrder])
    if (hoister.isEmpty) {
      sort
    } else {
      // All child output columns stay live: the bare Sort's output equals its child's output. The
      // hoisted `_ve` aliases are supplied to `pushSideAliases` separately (not via `needed`).
      val needed = sort.child.outputSet
      // `pushSideAliases` places the aliases in a Project directly above the child's non-join
      // descendant, descending through any Join/Project chain between the Sort and the scan.
      val newChild = pushSideAliases(sort.child, hoister.aliases, needed)
      // Wrap in a Project restoring the original output so the `_ve` alias columns do not leak.
      // `sort.child.output` (original attributes, in order) is the correct projectList -- NOT
      // `newSort.output`, which would include the `_ve` columns `pushSideAliases` added below.
      // `sort.copy` preserves `sort.global` and `sort.hint`.
      Project(sort.child.output, sort.copy(order = newOrder, child = newChild))
    }
  }

  private def rewriteJoinUnderProject(
      project: Project, projectList: Seq[NamedExpression], join: Join): LogicalPlan = {
    val leftOutput = join.left.outputSet
    val rightOutput = join.right.outputSet
    val leftHoister = new ExtractionHoister
    val rightHoister = new ExtractionHoister

    // A single variant extraction references exactly one attribute, hence one join side; route it
    // to that side's hoister. Anything not cleanly on one side is left in place. We hoist from both
    // the join condition (its extractions feed the equi-key comparison) and the Project above the
    // join (its extractions -- e.g. aggregate arguments hoisted here by `rewriteAggregate`, or a
    // user's `SELECT variant_get(...)`), so the pushdown sees them below the join.
    val newCondition = join.condition.map(_.transformDown {
      case ex if isHoistable(ex) =>
        if (ex.references.subsetOf(leftOutput)) {
          leftHoister.aliasFor(ex)
        } else if (ex.references.subsetOf(rightOutput)) {
          rightHoister.aliasFor(ex)
        } else {
          ex
        }
    })
    val newProjectList =
      hoistProjectListExtractions(projectList, leftOutput, rightOutput, leftHoister, rightHoister)

    if (leftHoister.isEmpty && rightHoister.isEmpty) {
      project
    } else {
      // Columns still live above the Join: what the (rewritten) Project consumes and what the
      // rewritten condition references directly (alias attributes are supplied by the side Projects
      // below). A side's variant used only by a hoisted extraction is dropped rather than passed
      // through, so no redundant full-variant slot is requested.
      val needed = AttributeSet(
        newProjectList.flatMap(_.references) ++
          newCondition.map(_.references).getOrElse(AttributeSet.empty))
      // `pushSideAliases` places the aliases in a Project directly above the side's non-join child
      // (recursing through nested joins), so the extractions reach the scan even when the side is
      // itself a chain of joins. For a flat side (scan / Filter above a scan) this is a single
      // Project, matching the previous behavior.
      val newJoin = join.copy(
        left = pushSideAliases(join.left, leftHoister.aliases, needed),
        right = pushSideAliases(join.right, rightHoister.aliases, needed),
        condition = newCondition)
      project.copy(projectList = newProjectList, child = newJoin)
    }
  }
}

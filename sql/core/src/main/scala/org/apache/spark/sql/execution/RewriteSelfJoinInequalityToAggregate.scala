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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.IN_SUBQUERY
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Rewrites a self-join with an inequality into GROUP BY + HAVING COUNT(DISTINCT) > 1.
 *
 * Targets the two uncorrelated InSubquery shapes exercised by TPC-DS Q95:
 *
 *   - Pattern A': the subquery top-level InnerJoin is a direct self-join.
 *   - Pattern A2: the subquery contains an outer InnerJoin with a self-join child; only the
 *     self-join child is replaced with Aggregate and the outer join is preserved.
 *
 * Both patterns require an existence-only membership context so row-count multiplicity from the
 * original self-join cross-product does not affect semantics. Correlated InSubquery expressions are
 * intentionally fail-closed because the ExprId remapping performed here does not rewrite correlated
 * predicates.
 *
 * This rule runs in `extendedOperatorOptimizationRules`, which is part of the operator optimization
 * batch and therefore executes before `RewritePredicateSubquery` turns the predicate subquery into
 * a semi/anti/existence join. It only observes the uncorrelated `InSubquery` shape at that phase,
 * so there is no separate LeftSemi/LeftAnti ("Pattern A") or `Exists` handling.
 *
 * Both patterns share:
 *   - [[buildAggregateHavingDistinctGt1]] to construct `Filter(cnt > 1, Aggregate)`
 *   - [[canonicalizeWrapper]] to rebuild a wrapping Project so every equi-key reference points to
 *     the sjLeft-side attribute, with **fresh exprIds** (Spark's SPARK-21835 style -- no reuse of
 *     original exprIds), returning an old->new attribute remap for downstream rewrite.
 *
 * Controlled by `spark.sql.optimizer.rewriteSelfJoinInequalityToAggregate.enabled`
 * (default false, opt-in).
 */
object RewriteSelfJoinInequalityToAggregate extends Rule[LogicalPlan] with PredicateHelper {

  private val CountDistinctAliasName = "_rewrite_selfjoin_inequality_cnt_distinct"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.REWRITE_SELF_JOIN_INEQUALITY_TO_AGGREGATE_ENABLED)) {
      return plan
    }

    // Pattern A' / A2: rewrite uncorrelated InSubquery plans.
    // Correlated subqueries carry outer references / correlated join conditions in
    // `SubqueryExpression.children`; fail closed because this rule does not remap them.
    val rewritten = plan.transformAllExpressionsWithPruning(_.containsPattern(IN_SUBQUERY)) {
      case in @ InSubquery(_, lq: ListQuery) if lq.children.isEmpty =>
        rewriteSubqueryPlan(lq.plan) match {
          case Some(newSub) => in.copy(query = lq.copy(plan = newSub))
          case None => in
        }
    }
    if (!(rewritten eq plan)) {
      logDebug(
        "RewriteSelfJoinInequalityToAggregate: rewrote self-join to " +
          "GROUP BY + HAVING COUNT(DISTINCT) > 1")
    }
    rewritten
  }
  // ============================================================================
  //  Shared helpers
  // ============================================================================

  /**
   * Build `Filter(cnt > 1, Aggregate(equiKeys, [equiKeys, cnt_alias], Filter(IsNotNull(equiKeys),
   * child)))`. Returns the Filter node whose output is `equiKeys ++ [count_alias_attr]`.
   *
   * The extra `IsNotNull(equiKeys)` filter is essential to preserve the original equi-join's NULL
   * semantics. Under SQL 3VL, `left.k = right.k` never matches when either side is NULL, so the
   * original self-join drops rows with NULL equi-keys. Aggregate, in contrast, groups NULL keys
   * together into a single "NULL group" -- if that group has >= 2 distinct non-null neq values,
   * COUNT(DISTINCT) > 1 fires and injects NULL into the subquery output. That leaked NULL then
   * turns `NOT IN` into a spurious empty result (Spark's null-aware anti-join uses
   * `Or(equi, IsNull(equi))` which any NULL sub-row satisfies) and can flip IN/NOT IN outcomes. The
   * neq column needs no such filter: `COUNT(DISTINCT col)` already ignores NULL.
   */
  private def buildAggregateHavingDistinctGt1(
      equiKeys: Seq[Attribute],
      neqCol: Attribute,
      child: LogicalPlan): LogicalPlan = {
    val countExpr = AggregateExpression(
      Count(Seq(neqCol)),
      mode = Complete,
      isDistinct = true,
      filter = None,
      NamedExpression.newExprId)
    val countAlias = Alias(countExpr, CountDistinctAliasName)()
    // Seq[Attribute] is a Seq[NamedExpression] via covariance; no cast needed.
    val aggExprs: Seq[NamedExpression] = equiKeys :+ countAlias
    val nonNullChild = equiKeys
      .map(a => IsNotNull(a): Expression)
      .reduceOption(And)
      .map(Filter(_, child))
      .getOrElse(child)
    val agg = Aggregate(equiKeys, aggExprs, nonNullChild)
    Filter(GreaterThan(countAlias.toAttribute, Literal(1L, LongType)), agg)
  }

  /**
   * Canonicalize a Project so every equi-key reference points at the sjLeft-side attribute.
   * [[parseSelfJoinCondition]] has already verified that each pair refers to the same output
   * position on the two structurally identical self-join sides. Uses **fresh exprIds** (no reuse of
   * original wrapper output exprIds) -- the same technique Spark's own `dedupSubqueryOnSelfJoin`
   * uses when it needs to change subquery output.
   *
   * Returns the rebuilt Project and a map `oldWrapperOutputExprId -> newWrapperOutputAttr`, so
   * downstream references (outer join condition, top-level Project) can be updated consistently.
   *
   * `equiPairs` provides the definitive ExprId-based lookup: `equiPair (l, r)` binds
   * `l.exprId -> l` (identity) and `r.exprId -> l` (sjRight -> sjLeft). Attribute identity in
   * Catalyst is ExprId, not name; two columns can share a name with distinct ExprIds. Name-based
   * lookup would silently drop such entries via `.toMap`.
   *
   * Fails (returns None) when a projectList entry is neither an equi-key Attribute (by ExprId) nor
   * `Alias(equi-key Attribute, _)`. Fail-closed.
   */
  private def canonicalizeWrapper(
      projectList: Seq[NamedExpression],
      equiPairs: Seq[(Attribute, Attribute)],
      newChild: LogicalPlan): Option[(Project, Map[ExprId, Attribute])] = {
    // ExprId-based canonical map: any equi-key attribute (either side) -> sjLeft attribute.
    val exprIdToLeft: Map[ExprId, Attribute] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId -> l, r.exprId -> l) }.toMap
    val oldOutput: Seq[Attribute] = projectList.map(_.toAttribute)
    val mapped: Seq[Option[NamedExpression]] = projectList.map {
      case a: Attribute if exprIdToLeft.contains(a.exprId) =>
        // Wrap every rewritten output slot in a fresh Alias.
        //
        // When a wrapper reprojects BOTH sides of the same equi pair (e.g.
        // `SELECT s1.k, s2.k FROM T s1 JOIN T s2 ON s1.k = s2.k AND s1.v <> s2.v`),
        // both entries collapse to the same sjLeft Attribute after the self-join is
        // rewritten. Duplicate output ExprIds are not illegal in Spark (`SELECT a, a`
        // is a valid Project), but fresh Aliases give each output slot an independent
        // identity, which keeps the `oldOutput -> newOutput` remap 1-to-1 and lets
        // downstream references (outer join condition, top-level Project) be updated
        // unambiguously via ExprId.
        //
        // The fresh ExprId is on the Alias ITSELF; the referenced child keeps its
        // original ExprId. Spark's logical-plan integrity checks reject reusing a
        // referenced ExprId as the Alias's own ExprId, not duplication across slots.
        Some(Alias(exprIdToLeft(a.exprId), a.name)(): NamedExpression)
      case al @ Alias(a: Attribute, _) if exprIdToLeft.contains(a.exprId) =>
        // Fresh exprId; do NOT reuse `al.exprId`. Reusing another expression's exprId
        // is the pattern that Spark 3.3 flags via structural-integrity checks.
        Some(Alias(exprIdToLeft(a.exprId), al.name)(): NamedExpression)
      case _ => None
    }
    if (mapped.exists(_.isEmpty)) {
      None
    } else {
      val newProjectList = mapped.flatten
      val newWrapper = Project(newProjectList, newChild)
      val newOutput = newWrapper.output
      val remap: Map[ExprId, Attribute] =
        oldOutput.zip(newOutput).map { case (o, n) => o.exprId -> n }.toMap
      Some((newWrapper, remap))
    }
  }

  /**
   * Replace equi-key attribute references inside a NamedExpression according to `remap`, while
   * preserving the NamedExpression shape.
   *
   * `Expression.transformUp` returns `Expression`, not `NamedExpression`. We avoid a blanket
   * `asInstanceOf[NamedExpression]` by handling the two shapes that can appear in a Project's
   * `projectList` explicitly: a bare Attribute (whose top-level may itself be replaced) and an
   * Alias (which stays an Alias while its child is transformed). Any other NamedExpression shape we
   * do not rewrite is left as-is ONLY if it does not reference a replaced self-join output;
   * otherwise it would carry a stale ExprId, so returns None to fail the whole rewrite closed.
   */
  private def remapNamedExpressionAttributes(
      ne: NamedExpression,
      remap: Map[ExprId, Attribute]): Option[NamedExpression] = ne match {
    case a: Attribute if remap.contains(a.exprId) => Some(remap(a.exprId))
    case a: Attribute => Some(a)
    case al: Alias =>
      val newChild = al.child.transformUp {
        case a: Attribute if remap.contains(a.exprId) => remap(a.exprId)
      }
      Some(
        if (newChild eq al.child) al
        else Alias(newChild, al.name)(al.exprId, al.qualifier, al.explicitMetadata))
    case other if other.references.exists(a => remap.contains(a.exprId)) =>
      // Fail-closed: a NamedExpression we do not rewrite (neither a bare Attribute nor an Alias)
      // that still references a replaced self-join output would be left with a dangling ExprId.
      // Refuse the rewrite rather than emit a plan with a stale reference.
      None
    case other => Some(other)
  }

  // ============================================================================
  //  Pattern A' / A2 dispatch (subquery plans of InSubquery)
  // ============================================================================

  private def rewriteSubqueryPlan(plan: LogicalPlan): Option[LogicalPlan] = {
    // Candidate-level nondeterminism guard: reject if ANY node in the whole subquery plan
    // is non-repeatable (Rand, LIMIT-without-ORDER-BY, Sample, Offset, streaming). This catches
    // nondeterminism that lives ABOVE the self-join rather than on either side -- e.g. a Pattern
    // A2 outer join whose condition is `d.k = sj.k AND rand() < 0.5`. Both self-join sides stay
    // repeatable there, so the per-side `isSameBaseRelation` check would pass, yet the enclosing
    // subquery is not repeatable.
    if (!isRepeatablePlan(plan)) return None

    val (projectListOpt, innerJoin): (Option[Seq[NamedExpression]], Join) = plan match {
      case Project(pl, j: Join) if j.joinType == Inner && j.condition.isDefined =>
        (Some(pl), j)
      case j: Join if j.joinType == Inner && j.condition.isDefined =>
        (None, j)
      case _ => return None
    }

    if (isSameBaseRelation(innerJoin.left, innerJoin.right)) {
      rewriteDirectSelfJoin(projectListOpt, innerJoin)
    } else {
      rewriteNestedSelfJoin(projectListOpt, innerJoin)
    }
  }

  // ============================================================================
  //  Pattern A' : direct self-join at subquery top level
  // ============================================================================

  private def rewriteDirectSelfJoin(
      projectListOpt: Option[Seq[NamedExpression]],
      innerJoin: Join): Option[LogicalPlan] = {
    val innerLeft = innerJoin.left
    val innerRight = innerJoin.right
    val innerCond = innerJoin.condition.get

    val parsed = parseSelfJoinCondition(innerCond, innerLeft, innerRight)
    if (parsed.isEmpty) return None
    // parseSelfJoinCondition has validated column correspondence and equi-key uniqueness.
    val (equiPairs, neqPairs) = parsed.get

    val innerLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val innerLeftNeqAttr: Attribute = neqPairs.head._1
    val filtered = buildAggregateHavingDistinctGt1(innerLeftEquiAttrs, innerLeftNeqAttr, innerLeft)

    // Fail-closed on bare-Join subqueries: without a wrapping Project the subquery output
    // is the full self-join output (both sides' columns). Replacing that with
    // `Project(equiKeys, filtered)` shrinks the output; if the enclosing InSubquery
    // referenced a non-equi column by position, `values.zip(sub.output).map(EqualTo.tupled)`
    // inside RewritePredicateSubquery would build an incorrect semi condition. Q95's
    // subqueries all have an explicit Project wrapper, so this branch does not affect it.
    projectListOpt match {
      case None =>
        None
      case Some(pl) =>
        canonicalizeWrapper(pl, equiPairs, filtered).map {
          case (newWrapper, _) =>
            logDebug(
              s"Pattern A' - equiKeys=[${innerLeftEquiAttrs.map(_.name).mkString(",")}]" +
                s", neqCol=${innerLeftNeqAttr.name}" +
                s", outCols=[${newWrapper.projectList.map(_.name).mkString(",")}]")
            newWrapper
        }
    }
  }

  // ============================================================================
  //  Pattern A2 : self-join nested inside another InnerJoin in the subquery
  // ============================================================================

  private def rewriteNestedSelfJoin(
      projectListOpt: Option[Seq[NamedExpression]],
      outerJoin: Join): Option[LogicalPlan] = {
    val outerCond = outerJoin.condition.get

    val (selfJoinSide, selfJoinOnRight) =
      tryExtractSelfJoin(outerJoin.right) match {
        case Some(_) => (outerJoin.right, true)
        case None =>
          tryExtractSelfJoin(outerJoin.left) match {
            case Some(_) => (outerJoin.left, false)
            case None => return None
          }
      }

    val (selfJoinProjectOpt, selfJoin) = selfJoinSide match {
      case p @ Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined =>
        (Some(p), j)
      case j: Join if j.joinType == Inner && j.condition.isDefined =>
        (None, j)
      case _ => return None
    }

    val sjLeft = selfJoin.left
    val sjRight = selfJoin.right
    val sjCond = selfJoin.condition.get
    if (!isSameBaseRelation(sjLeft, sjRight)) return None

    val parsed = parseSelfJoinCondition(sjCond, sjLeft, sjRight)
    if (parsed.isEmpty) return None
    // parseSelfJoinCondition has validated column correspondence and equi-key uniqueness.
    val (equiPairs, neqPairs) = parsed.get

    val sjLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val sjLeftNeqAttr: Attribute = neqPairs.head._1

    val selfJoinOutputSet = selfJoinSide.outputSet
    val sjEquiExprIds: Set[ExprId] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    // wrapper Project may reproject equi-keys under fresh alias exprIds; include those.
    val wrapperEquiExprIds: Set[ExprId] = selfJoinProjectOpt.toSeq.flatMap {
      p =>
        p.projectList.flatMap {
          case a: Attribute if sjEquiExprIds.contains(a.exprId) => Some(a.exprId)
          case al @ Alias(a: Attribute, _) if sjEquiExprIds.contains(a.exprId) => Some(al.exprId)
          case _ => None
        }
    }.toSet
    val allEquiExprIds = sjEquiExprIds ++ wrapperEquiExprIds

    // Outer join condition may reference only equi-key attrs from the self-join side.
    val outerCondRefs = outerCond.references.filter(selfJoinOutputSet.contains)
    if (!outerCondRefs.forall(a => allEquiExprIds.contains(a.exprId))) return None

    // Top-level subquery Project may reference only equi-key attrs from the self-join side.
    val projectOk = projectListOpt.forall {
      pl =>
        val refs = pl.flatMap(_.references).filter(selfJoinOutputSet.contains)
        refs.forall(a => allEquiExprIds.contains(a.exprId))
    }
    if (!projectOk) return None

    val filtered = buildAggregateHavingDistinctGt1(sjLeftEquiAttrs, sjLeftNeqAttr, sjLeft)

    val (newSelfJoinSide, outputRemap): (LogicalPlan, Map[ExprId, Attribute]) =
      selfJoinProjectOpt match {
        case Some(wp) =>
          canonicalizeWrapper(wp.projectList, equiPairs, filtered) match {
            case Some((newWrapper, remap)) => (newWrapper, remap)
            case None => return None
          }
        case None if projectListOpt.isEmpty =>
          // Fail-closed: with neither a wrapper Project around the self-join nor a top-level
          // subquery Project, the outer join currently exposes every self-join column, and
          // replacing the self-join with `Project(equiKeys, filtered)` would shrink the outer
          // join's right-hand output arity. RewritePredicateSubquery's positional zip
          // (`values.zip(sub.output).map(EqualTo.tupled)`) would then bind semi predicates to
          // the wrong attributes -- silently dropping components of a tuple IN. A
          // top-level Project (`projectListOpt`) is what would let the arity be preserved
          // by the top-level rewrite loop; without one, refuse to rewrite.
          return None
        case None =>
          // No wrapper Project but there IS a top-level subquery Project: shrinking the outer
          // join's self-join-side output is safe because the top-level Project is rewritten
          // consistently via `outputRemap` below and the top-level rewrite loop ensures
          // subquery output arity matches what the enclosing InSubquery expects.
          // Outer references may point at sjRight equi-attributes; remap them to sjLeft
          // (same output position in a valid self-join).
          val newP = Project(sjLeftEquiAttrs, filtered)
          val remap: Map[ExprId, Attribute] =
            equiPairs.map { case (l, r) => r.exprId -> l }.toMap
          (newP, remap)
      }

    // Rewrite outer join condition to use new wrapper output attributes.
    val newOuterCond = outerCond.transformUp {
      case a: Attribute if outputRemap.contains(a.exprId) => outputRemap(a.exprId)
    }

    val newOuterJoin = if (selfJoinOnRight) {
      outerJoin.copy(right = newSelfJoinSide, condition = Some(newOuterCond))
    } else {
      outerJoin.copy(left = newSelfJoinSide, condition = Some(newOuterCond))
    }

    // Rewrite top-level Project references.
    val result = projectListOpt match {
      case Some(pl) =>
        val remapped = pl.map(ne => remapNamedExpressionAttributes(ne, outputRemap))
        if (remapped.exists(_.isEmpty)) return None
        Project(remapped.flatten, newOuterJoin)
      case None => newOuterJoin
    }

    logDebug(
      s"Pattern A2 - equiKeys=[${sjLeftEquiAttrs.map(_.name).mkString(",")}]" +
        s", neqCol=${sjLeftNeqAttr.name}")
    Some(result)
  }

  private def tryExtractSelfJoin(plan: LogicalPlan): Option[Join] = {
    val join = plan match {
      case Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => j
      case j: Join if j.joinType == Inner && j.condition.isDefined => j
      case _ => return None
    }
    if (!isSameBaseRelation(join.left, join.right)) return None
    val parsed = parseSelfJoinCondition(join.condition.get, join.left, join.right)
    if (parsed.isEmpty) return None
    Some(join)
  }

  // ============================================================================
  //  parseSelfJoinCondition + isSameBaseRelation
  // ============================================================================

  private def outputOrdinal(plan: LogicalPlan, attr: Attribute): Int =
    plan.output.indexWhere(_.exprId == attr.exprId)

  private def sameOutputPosition(
      leftPlan: LogicalPlan,
      rightPlan: LogicalPlan,
      leftAttr: Attribute,
      rightAttr: Attribute): Boolean = {
    val leftPos = outputOrdinal(leftPlan, leftAttr)
    val rightPos = outputOrdinal(rightPlan, rightAttr)
    leftPos >= 0 && rightPos >= 0 && leftPos == rightPos
  }

  /**
   * Parse a join condition into equi-pairs and inequality-pairs. Accepts only:
   *   - `EqualTo(attr, attr)` where the two attrs come from opposite sides,
   *   - `Not(EqualTo(attr, attr))` -- same side rule,
   *   - `IsNotNull(attr)` where the attr is one of the join columns.
   * Anything else in the condition disqualifies the whole rewrite (fail-closed).
   */
  private def parseSelfJoinCondition(
      condition: Expression,
      leftPlan: LogicalPlan,
      rightPlan: LogicalPlan)
      : Option[(Seq[(Attribute, Attribute)], Seq[(Attribute, Attribute)])] = {

    val leftOutput = leftPlan.outputSet
    val rightOutput = rightPlan.outputSet
    val predicates = splitConjunctivePredicates(condition)

    val equiPairs = predicates.collect {
      case EqualTo(l: Attribute, r: Attribute)
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
      case EqualTo(r: Attribute, l: Attribute)
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
    }

    val neqPairs = predicates.collect {
      case Not(EqualTo(l: Attribute, r: Attribute))
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
      case Not(EqualTo(r: Attribute, l: Attribute))
          if leftOutput.contains(l) && rightOutput.contains(r) =>
        (l, r)
    }

    // Only IsNotNull predicates on join columns are safe to drop -- they're redundant with
    // the join semantics or auto-added by InferFiltersFromConstraints. IsNotNull on other
    // columns changes semantics if we drop it; bail out.
    val joinAttrIds: Set[ExprId] =
      (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    val isNotNullOnJoinCols = predicates.count {
      case IsNotNull(a: Attribute) if joinAttrIds.contains(a.exprId) => true
      case _ => false
    }

    val totalMatched = equiPairs.size + neqPairs.size + isNotNullOnJoinCols
    if (totalMatched != predicates.size) return None

    if (equiPairs.isEmpty || neqPairs.isEmpty) return None

    // Only rewrite the single-inequality case. Multiple inequality conjuncts cannot be represented
    // by COUNT(DISTINCT) over a single column.
    if (neqPairs.size != 1) return None

    // The rewrite swaps SQL comparison equality for grouping/DISTINCT equality: `<>` becomes
    // COUNT(DISTINCT neqCol) and `=` becomes GROUP BY equiKey. It is only sound on types where
    // those two notions of equality coincide, so gate every equi-key and the neq column on a
    // positive `isSafeComparisonGroupingType` allowlist rather than `RowOrdering.isOrderable`:
    // orderable only proves an order/hash exists, not that comparison and grouping agree. Fail
    // closed on anything not proven safe (float -0.0/NaN, complex types, non-binary collations,
    // and future/unknown types).
    //
    // Check BOTH ends of every pair, not just the sjLeft attribute. `isSameBaseRelation` proves
    // the two sides are canonically equal, but `AttributeReference.canonicalized` rewrites the
    // reference to drop metadata, so canonical equality does NOT prove the sjRight attribute
    // carries the same CHAR/VARCHAR metadata that `isSafeComparisonGroupingAttribute` reads. Gate
    // each side independently rather than assume they match.
    val keyAttrs =
      (equiPairs ++ neqPairs).flatMap { case (left, right) => Seq(left, right) }
    if (!keyAttrs.forall(isSafeComparisonGroupingAttribute)) return None

    // Canonicalization intentionally erases cosmetic Alias names, so name equality cannot prove
    // that the two predicate ends refer to the same underlying column. Resolve each end by its own
    // ExprId against its child output and require matching output ordinals instead.
    val equiValid = equiPairs.forall {
      case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r)
    }
    val neqValid = neqPairs.forall {
      case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r)
    }
    if (!equiValid || !neqValid) return None

    // Equi-key output positions must be distinct across pairs. Keep the same positional identity
    // here so swapped or duplicate aliases cannot make two different underlying columns look equal.
    val leftEquiOrdinals = equiPairs.map { case (l, _) => outputOrdinal(leftPlan, l) }
    if (leftEquiOrdinals.exists(_ < 0)) return None
    if (leftEquiOrdinals.distinct.size != leftEquiOrdinals.size) return None

    // Defensive: reject when the neq column overlaps an equi-key column
    // (e.g. `t1.k = t2.k AND t1.k <> t2.k`).
    val neqLeftOrdinal = outputOrdinal(leftPlan, neqPairs.head._1)
    if (neqLeftOrdinal < 0 || leftEquiOrdinals.contains(neqLeftOrdinal)) return None
    Some((equiPairs, neqPairs))
  }

  /**
   * Attribute-aware gate applied to the equi keys and the neq column. A CHAR/VARCHAR column reaches
   * the optimizer as StringType with its declared type recorded in the attribute metadata
   * (CharVarcharUtils stamps it when the relation output is built), so checking `attr.dataType`
   * alone would let it through the StringType branch of [[isSafeComparisonGroupingType]]. Recover
   * the declared raw type from the metadata (falling back to `dataType` when there is no marker)
   * and run it through the datatype allowlist, so CHAR/VARCHAR fail closed there in every config.
   */
  private def isSafeComparisonGroupingAttribute(attr: Attribute): Boolean = {
    val rawType = CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)
    isSafeComparisonGroupingType(rawType)
  }

  /**
   * Types whose SQL comparison equality (`=` / `<>`) is provably identical to grouping / DISTINCT
   * equality (and hashing). The rewrite turns `<>` into COUNT(DISTINCT) and `=` into GROUP BY, so
   * it must never be applied to a type where those two notions of equality can diverge. This is a
   * conservative positive allowlist: it admits only the types where the equivalence is well
   * established and fails closed on everything else, including any new type a future version might
   * add.
   *
   * Deliberately rejected:
   *   - Float / Double: this rewrite relies on comparison equality (`=` / `<>`) and
   *     grouping/DISTINCT equality having exactly the same contract. Signed zero and NaN are the
   *     representative edge cases that make that contract non-trivial for floating point, and
   *     whether the two paths agree rests on normalization details (e.g. NormalizeFloatingNumbers).
   *     Rather than depend on that, floats fail closed. Complex types are rejected wholesale below,
   *     which also covers floats nested inside a struct/array/map.
   *   - Complex types (Array / Struct / Map) and UDTs / Variant: their equality-vs-grouping
   *     equivalence is harder to prove and can itself embed floats.
   *   - Char / Varchar: CHAR / VARCHAR have dedicated declared-type semantics and are
   *     conservatively kept outside this rewrite. Table columns may reach the optimizer as
   *     annotated StringType, so the caller ([[isSafeComparisonGroupingAttribute]]) recovers their
   *     declared type from metadata first; this branch is the single rejection point for both that
   *     path and first-class CHAR/VARCHAR types.
   *   - Non-binary collated strings: comparison and grouping can route through different collation
   *     code paths, so only byte-wise `supportsBinaryEquality` strings are admitted.
   */
  private def isSafeComparisonGroupingType(dt: DataType): Boolean = dt match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _: DecimalType => true
    case BooleanType => true
    case DateType => true
    case TimestampType | TimestampNTZType => true
    case BinaryType => true

    case _: CharType | _: VarcharType => false
    case st: StringType if st.supportsBinaryEquality => true

    case _ => false
  }

  /**
   * True iff `plan` produces the same row bag on every evaluation.
   *
   * This is the primary safety guard for the rewrite, which folds two occurrences of the same
   * subtree into one aggregate -- sound only when both occurrences produce identical row bags. We
   * check it at TWO levels:
   *   - candidate level: the enclosing subquery, before descending into the self-join. Catches
   *     nondeterminism that lives above the self-join rather than on either side -- e.g. a Pattern
   *     A2 outer join whose condition is `d.k = sj.k AND rand() < 0.5`. Without this,
   *     `isSameBaseRelation(sjLeft, sjRight)` could pass (both sides look deterministic) while the
   *     enclosing plan still contains `Rand`.
   *   - relation level: [[isSameBaseRelation]] additionally requires the two sides to be
   *     structurally identical.
   *
   * Attribute-level `plan.deterministic` alone is NOT sufficient. Catalyst's
   * `Expression.deterministic` only checks explicit `Nondeterministic` annotation; several
   * operators produce a runtime-nondeterministic row bag even though every expression they contain
   * is `deterministic == true`:
   *   - `Aggregate` with `First` / `Last` / `collect_list` / `min_by` / `max_by` (tie order),
   *   - `Window` with `row_number()` / `rank()` over a non-total order,
   *   - `Limit` / `LocalLimit` / `Sample` / `Offset` (row-bag operator-level nondeterminism),
   *   - streaming sources.
   *
   * This rule collapses two evaluations of the same subtree into one aggregate; repeatability must
   * be provable, not assumed. That is why both the operator check and the expression check below
   * are WHITELISTS rather than blacklists -- unknown operators and unknown expression types default
   * to reject.
   *
   * Expression support is allowlisted, not blacklisted. `plan.deterministic` relies on each
   * expression's reported `deterministic` contract; that is necessary but insufficient for unknown
   * expression types whose repeatability has not been established -- a builtin that carries hidden
   * state yet reports `deterministic == true` would otherwise be trusted silently. For a rewrite
   * that folds two evaluations of a subtree into one aggregate we prefer to miss an optimization
   * than to misapply one, so new expression types are added to [[isRepeatableExpression]] only
   * after their repeatability has been established. `plan.deterministic` is kept as a cheap
   * fast-reject, but the expression allowlist is what actually proves repeatability.
   *
   * `plan.subqueriesAll.isEmpty` additionally fail-closes on any embedded expression subquery
   * (scalar / IN / EXISTS). `plan.exists` in `isRowBagRepeatable` walks only the operator tree and
   * does not descend into expression subqueries, and `plan.deterministic` does not prove a nested
   * subquery is row-bag repeatable (e.g. an uncorrelated `LIMIT 1` without `ORDER BY`). Rejecting
   * any embedded subquery keeps the repeatability proof confined to the operator whitelist below.
   */
  private def isRepeatablePlan(plan: LogicalPlan): Boolean = {
    // The operator/source whitelist is checked before the expression whitelist so that a plan whose
    // operator is itself unknown -- e.g. Aggregate (carries AggregateExpression) or Window (carries
    // WindowExpression / SortOrder) -- is attributed to isRowBagRepeatable rather than being masked
    // by the fact that those operators also carry non-allowlisted expressions.
    plan.deterministic &&
    !plan.isStreaming &&
    plan.subqueriesAll.isEmpty &&
    isRowBagRepeatable(plan) &&
    hasRepeatableExpressions(plan)
  }

  /**
   * Operator whitelist for `isRepeatablePlan`. A plan is row-bag repeatable only when every node is
   * known to produce a repeatable output row bag from repeatable children. Unknown operators and
   * unknown leaf sources fail closed.
   *
   * Kept intentionally narrow -- the target workload (Q95-shape self-join in a subquery) only needs
   * a Parquet relation scan optionally wrapped in Project / Filter / SubqueryAlias plus the
   * self-join itself. Range and LocalRelation are also trusted deterministic leaves. Adding an
   * operator here requires proving that its output row values and multiplicities are repeatable.
   * Pure row ordering is irrelevant to the row-bag contract, but operators not needed by the
   * target shape remain fail-closed until explicitly reviewed.
   *
   * Arbitrary `LeafNode`s are intentionally not trusted. For example, `LogicalRDD` may wrap an
   * arbitrary RDD lineage whose runtime behavior is invisible to Catalyst's `plan.deterministic`;
   * `InMemoryRelation`, `DataSourceV2Relation` and custom leaves are likewise rejected until
   * proven. Streaming sources reach here as `LeafNode`s but are already filtered upstream by
   * `plan.isStreaming` in [[isRepeatablePlan]].
   *
   * `LogicalRelation` is trusted only when its underlying relation is a `HadoopFsRelation` whose
   * `fileFormat` is EXACTLY `ParquetFileFormat` (`getClass == classOf[ParquetFileFormat]`, not
   * `isInstanceOf`). `HadoopFsRelation.fileFormat` can be any `FileFormat`, including custom
   * formats whose scan is not provably a repeatable row bag; `ParquetFileFormat` is also non-final,
   * so a third-party subclass could override its scan. The target workload only needs stock
   * Parquet, so every other `FileFormat` -- subclasses of `ParquetFileFormat` included -- and every
   * non-`HadoopFsRelation` fail closed.
   *
   * This helper checks operators and leaf sources only; expression-type repeatability is a separate
   * concern handled by [[hasRepeatableExpressions]], and both are joined in [[isRepeatablePlan]].
   */
  private def isRowBagRepeatable(plan: LogicalPlan): Boolean = !plan.exists {
    // TreeNode exposes `exists` but not `forall`, so invert: a whitelisted operator maps to `false`
    // ("does not break repeatability") and everything else to `true`; negating the whole `exists`
    // then means "every operator is whitelisted".
    case _: Project => false
    case _: Filter => false
    case _: SubqueryAlias => false
    // Join is included because our target pattern IS a Join; both children get recursed into.
    case _: Join => false
    // Explicitly trusted leaves.
    case _: Range => false
    case _: LocalRelation => false
    case relation: LogicalRelation =>
      relation.relation match {
        case h: HadoopFsRelation if h.fileFormat.getClass == classOf[ParquetFileFormat] => false
        case _ => true
      }
    // Everything else -- Aggregate, Window, Limit, Sample, Offset, Distinct, Union, Except,
    // Intersect, Sort, Expand, Generate, and opaque leaf sources -- is conservatively
    // unsupported and fails closed. (Row ordering itself is irrelevant to the row-bag contract;
    // these are rejected because their row values/multiplicities are not proven repeatable, or
    // simply because the target shape does not need them.)
    case _ => true
  }

  /**
   * Expression-type check for [[isRepeatablePlan]], kept separate from the operator whitelist in
   * [[isRowBagRepeatable]] so the two safety layers read independently. A plan passes only when
   * every expression carried by every operator node is repeatable per [[isRepeatableExpression]]; a
   * whitelisted operator holding an unknown expression -- e.g. `Project(Abs(v))` -- fails closed.
   */
  private def hasRepeatableExpressions(plan: LogicalPlan): Boolean = {
    !plan.exists(node => node.expressions.exists(expr => !isRepeatableExpression(expr)))
  }

  /**
   * Expression repeatability is allowlisted, consistently with the operator whitelist in
   * [[isRowBagRepeatable]]. Only expression types whose result is determined entirely by repeatable
   * children are accepted; unknown expression types fail closed. This rule folds two evaluations of
   * the same subtree into one, so missing an optimization is preferable to assuming repeatability
   * for an expression whose runtime behavior has not been proven.
   *
   * The whole tree is checked: an expression is repeatable only when its root type is on the
   * allowlist AND all of its children are themselves repeatable, so e.g. `Add(v, Abs(w))` is
   * rejected even though `Add` is allowlisted.
   *
   * The initial set covers what TPC-DS Q95 and the tests need: column / literal references, Alias,
   * Cast, basic arithmetic, boolean connectives, the six comparisons plus null-safe equality, and
   * null checks. Expressions such as `Abs` / `Coalesce` / `CaseWhen` are intentionally absent -- a
   * self-join over them is simply not rewritten (a missed optimization, not a correctness bug)
   * until each is added here after its repeatability has been established. Decimal wrappers such as
   * `PromotePrecision` / `CheckOverflow` are likewise absent and may fail closed; that is
   * acceptable and must not be worked around by trusting them just to make an arithmetic variant
   * fire.
   */
  private def isRepeatableExpression(expr: Expression): Boolean = expr match {
    case _: Attribute | _: Literal =>
      true
    case _: Alias | _: Cast | _: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder |
        _: And | _: Or | _: Not | _: EqualTo | _: EqualNullSafe | _: LessThan |
        _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual | _: IsNull | _: IsNotNull =>
      expr.children.forall(isRepeatableExpression)
    case _ =>
      false
  }

  /**
   * True iff `left` and `right` are the same plan modulo canonicalization AND each side is a
   * repeatable plan. See [[isRepeatablePlan]] for the repeatability contract.
   */
  private def isSameBaseRelation(left: LogicalPlan, right: LogicalPlan): Boolean = {
    left.sameResult(right) &&
    isRepeatablePlan(left) && isRepeatablePlan(right)
  }

  // splitConjunctivePredicates is provided by the mixed-in PredicateHelper trait.
}

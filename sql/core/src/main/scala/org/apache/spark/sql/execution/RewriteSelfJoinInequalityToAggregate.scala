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
 * Rewrites supported uncorrelated IN-subquery inequality self-joins into
 * `GROUP BY + HAVING MIN(neq) <> MAX(neq)`, avoiding the self-join cross-product.
 *
 * Supports a direct self-join (Pattern A') and a self-join nested under an outer inner join
 * (Pattern A2, where only the self-join child becomes an Aggregate). Unsupported and correlated
 * shapes fail closed.
 *
 * Runs in `extendedOperatorOptimizationRules`, before `RewritePredicateSubquery` turns the
 * predicate subquery into a semi/anti/existence join, so it only sees the uncorrelated
 * `InSubquery` shape.
 *
 * Controlled by `spark.sql.optimizer.rewriteSelfJoinInequalityToAggregate.enabled`
 * (default false, opt-in).
 */
object RewriteSelfJoinInequalityToAggregate extends Rule[LogicalPlan] with PredicateHelper {

  private val MinNeqAliasName = "_rewrite_selfjoin_inequality_min"
  private val MaxNeqAliasName = "_rewrite_selfjoin_inequality_max"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.REWRITE_SELF_JOIN_INEQUALITY_TO_AGGREGATE_ENABLED)) {
      return plan
    }

    // Fail closed on correlated subqueries: `lq.children` holds the outer references this rule
    // does not remap.
    plan.transformAllExpressionsWithPruning(_.containsPattern(IN_SUBQUERY)) {
      case in @ InSubquery(_, lq: ListQuery) if lq.children.isEmpty =>
        rewriteSubqueryPlan(lq.plan) match {
          case Some(newSub) => in.copy(query = lq.copy(plan = newSub))
          case None => in
        }
    }
  }

  // ============================================================================
  //  Shared helpers
  // ============================================================================

  /**
   * Build `Filter(min <> max, Aggregate(equiKeys, child))`, taking MIN and MAX over the neq column.
   * `MIN(neqCol) <> MAX(neqCol)` is true exactly when the group holds two or more distinct non-null
   * values -- the same test as `COUNT(DISTINCT neqCol) > 1`, but avoids the distinct-dedup
   * aggregation stages and supports partial aggregation.
   *
   * The `IsNotNull(equiKeys)` filter preserves the equi-join's NULL semantics: `=` never matches a
   * NULL key, but GROUP BY would fold all NULL keys into one group that can leak NULL into a
   * `NOT IN`. The neq column needs no filter -- MIN/MAX ignore NULL, and a group with fewer than
   * two non-null values has `min = max` (or both NULL, which makes `<>` NULL), so `<>` is never
   * true for it and the group is dropped.
   */
  private def buildAggregateHavingMultipleDistinct(
      equiKeys: Seq[Attribute],
      neqCol: Attribute,
      child: LogicalPlan): LogicalPlan = {
    val minAlias = Alias(Min(neqCol).toAggregateExpression(), MinNeqAliasName)()
    val maxAlias = Alias(Max(neqCol).toAggregateExpression(), MaxNeqAliasName)()
    val aggExprs: Seq[NamedExpression] = equiKeys :+ minAlias :+ maxAlias
    val nonNullChild = equiKeys
      .map(a => IsNotNull(a): Expression)
      .reduceOption(And)
      .map(Filter(_, child))
      .getOrElse(child)
    val agg = Aggregate(equiKeys, aggExprs, nonNullChild)
    Filter(Not(EqualTo(minAlias.toAttribute, maxAlias.toAttribute)), agg)
  }

  /**
   * Rebuild the wrapper Project so every equi-key reference points at the sjLeft attribute with a
   * fresh output ExprId, returning `oldOutputExprId -> newOutputAttr` for downstream references
   * (outer join condition, top-level Project). Lookup is by ExprId (Catalyst attribute identity),
   * not name. Fails closed when an entry is neither an equi-key Attribute nor `Alias(equi-key, _)`.
   */
  private def canonicalizeWrapper(
      projectList: Seq[NamedExpression],
      equiPairs: Seq[(Attribute, Attribute)],
      newChild: LogicalPlan): Option[(Project, Map[ExprId, Attribute])] = {
    val exprIdToLeft: Map[ExprId, Attribute] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId -> l, r.exprId -> l) }.toMap
    val oldOutput: Seq[Attribute] = projectList.map(_.toAttribute)
    val mapped: Seq[Option[NamedExpression]] = projectList.map {
      case a: Attribute if exprIdToLeft.contains(a.exprId) =>
        // Fresh exprId, but carry over qualifier / metadata so this branch stays consistent with
        // the Alias branch and a column keeps its metadata.
        Some(
          Alias(exprIdToLeft(a.exprId), a.name)(
            qualifier = a.qualifier,
            explicitMetadata = Some(a.metadata)): NamedExpression)
      case al @ Alias(a: Attribute, _) if exprIdToLeft.contains(a.exprId) =>
        // withNewChild preserves name/qualifier/metadata and exprId; newInstance then re-stamps a
        // fresh exprId, so Alias keeps ownership of its own metadata contract instead of us
        // re-listing its fields (which drift when Alias gains one).
        Some(al.withNewChild(exprIdToLeft(a.exprId)).newInstance())
      case _ => None
    }
    if (mapped.exists(_.isEmpty)) {
      None
    } else {
      val newProjectList = mapped.flatten
      val newWrapper = Project(newProjectList, newChild)
      val remap: Map[ExprId, Attribute] =
        oldOutput.zip(newWrapper.output).map { case (o, n) => o.exprId -> n }.toMap
      Some((newWrapper, remap))
    }
  }

  /**
   * Replace equi-key references inside a NamedExpression per `remap`, preserving Attribute/Alias
   * shape. Any other expression still referencing a replaced output returns None (fail-closed) to
   * avoid a dangling ExprId.
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
      // withNewChild preserves the same exprId/qualifier/metadata the manual copy did.
      Some(if (newChild eq al.child) al else al.withNewChild(newChild))
    case other if other.references.exists(a => remap.contains(a.exprId)) =>
      None
    case other => Some(other)
  }

  // ============================================================================
  //  Pattern A' / A2 dispatch (subquery plans of InSubquery)
  // ============================================================================

  private def rewriteSubqueryPlan(plan: LogicalPlan): Option[LogicalPlan] = {
    // Match the candidate shape first -- a top-level Inner Join, optionally under one wrapper
    // Project. This structural match is cheap, so run it before the whole-subquery
    // `isRepeatablePlan` walk and skip that walk entirely for the many subqueries that are not
    // even shaped like a self-join.
    val (projectListOpt, innerJoin): (Option[Seq[NamedExpression]], Join) = plan match {
      case Project(pl, j: Join) if j.joinType == Inner && j.condition.isDefined =>
        (Some(pl), j)
      case j: Join if j.joinType == Inner && j.condition.isDefined =>
        (None, j)
      case _ => return None
    }

    // Candidate-level guard: reject if any node in the whole subquery is non-repeatable, catching
    // nondeterminism hoisted above the self-join that the per-side `isSameBaseRelation` misses.
    if (!isRepeatablePlan(plan)) return None

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
    // Fail closed on an explicit join hint: it is a directive about the join this rule deletes.
    if (!innerJoin.hint.isEmpty) return None

    val innerLeft = innerJoin.left
    val innerCond = innerJoin.condition.get

    val parsed = parseSelfJoinCondition(innerCond, innerLeft, innerJoin.right)
    if (parsed.isEmpty) return None
    val (equiPairs, neqPairs) = parsed.get

    val innerLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val innerLeftNeqAttr: Attribute = neqPairs.head._1
    val filtered =
      buildAggregateHavingMultipleDistinct(innerLeftEquiAttrs, innerLeftNeqAttr, innerLeft)

    // Fail closed on a bare-Join subquery: with no wrapper Project, replacing the self-join output
    // with `Project(equiKeys, filtered)` shrinks arity and RewritePredicateSubquery's positional
    // `values.zip(sub.output)` would misbind semi predicates. Q95 subqueries always have a Project.
    projectListOpt match {
      case None => None
      case Some(pl) =>
        canonicalizeWrapper(pl, equiPairs, filtered).map { case (newWrapper, _) => newWrapper }
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
    val sjCond = selfJoin.condition.get
    if (!isSameBaseRelation(sjLeft, selfJoin.right)) return None

    val parsed = parseSelfJoinCondition(sjCond, sjLeft, selfJoin.right)
    if (parsed.isEmpty) return None
    val (equiPairs, neqPairs) = parsed.get

    val sjLeftEquiAttrs: Seq[Attribute] = equiPairs.map(_._1)
    val sjLeftNeqAttr: Attribute = neqPairs.head._1

    val selfJoinOutputSet = selfJoinSide.outputSet
    val sjEquiExprIds: Set[ExprId] =
      equiPairs.flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    // A wrapper Project may reproject equi-keys under fresh alias exprIds; include those.
    val wrapperEquiExprIds: Set[ExprId] = selfJoinProjectOpt.toSeq.flatMap { p =>
      p.projectList.flatMap {
        case a: Attribute if sjEquiExprIds.contains(a.exprId) => Some(a.exprId)
        case al @ Alias(a: Attribute, _) if sjEquiExprIds.contains(a.exprId) => Some(al.exprId)
        case _ => None
      }
    }.toSet
    val allEquiExprIds = sjEquiExprIds ++ wrapperEquiExprIds

    // The outer join condition and any top-level Project may reference only equi-key attrs from the
    // self-join side (the neq column does not survive the rewrite).
    val outerCondRefs = outerCond.references.filter(selfJoinOutputSet.contains)
    if (!outerCondRefs.forall(a => allEquiExprIds.contains(a.exprId))) return None
    val projectOk = projectListOpt.forall { pl =>
      val refs = pl.flatMap(_.references).filter(selfJoinOutputSet.contains)
      refs.forall(a => allEquiExprIds.contains(a.exprId))
    }
    if (!projectOk) return None

    val filtered = buildAggregateHavingMultipleDistinct(sjLeftEquiAttrs, sjLeftNeqAttr, sjLeft)

    val (newSelfJoinSide, outputRemap): (LogicalPlan, Map[ExprId, Attribute]) =
      selfJoinProjectOpt match {
        case Some(wp) =>
          canonicalizeWrapper(wp.projectList, equiPairs, filtered) match {
            case Some((newWrapper, remap)) => (newWrapper, remap)
            case None => return None
          }
        case None if projectListOpt.isEmpty =>
          // Fail closed: with no wrapper and no top-level Project, `Project(equiKeys, filtered)`
          // shrinks the outer join's arity and RewritePredicateSubquery's positional zip misbinds.
          return None
        case None =>
          // Top-level Project preserves arity via `outputRemap`; remap sjRight equi-refs to sjLeft
          // (same output position in a valid self-join).
          val newP = Project(sjLeftEquiAttrs, filtered)
          val remap: Map[ExprId, Attribute] = equiPairs.map { case (l, r) => r.exprId -> l }.toMap
          (newP, remap)
      }

    val newOuterCond = outerCond.transformUp {
      case a: Attribute if outputRemap.contains(a.exprId) => outputRemap(a.exprId)
    }

    val newOuterJoin = if (selfJoinOnRight) {
      outerJoin.copy(right = newSelfJoinSide, condition = Some(newOuterCond))
    } else {
      outerJoin.copy(left = newSelfJoinSide, condition = Some(newOuterCond))
    }

    projectListOpt match {
      case Some(pl) =>
        val remapped = pl.map(ne => remapNamedExpressionAttributes(ne, outputRemap))
        if (remapped.exists(_.isEmpty)) return None
        Some(Project(remapped.flatten, newOuterJoin))
      case None => Some(newOuterJoin)
    }
  }

  private def tryExtractSelfJoin(plan: LogicalPlan): Option[Join] = {
    val join = plan match {
      case Project(_, j: Join) if j.joinType == Inner && j.condition.isDefined => j
      case j: Join if j.joinType == Inner && j.condition.isDefined => j
      case _ => return None
    }
    // A hinted self-join is not an extraction candidate; see `rewriteDirectSelfJoin`.
    if (!join.hint.isEmpty) return None
    if (!isSameBaseRelation(join.left, join.right)) return None
    if (parseSelfJoinCondition(join.condition.get, join.left, join.right).isEmpty) return None
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
   * Parse a join condition into equi-pairs and inequality-pairs. Accepts only `EqualTo(attr, attr)`
   * and `Not(EqualTo(attr, attr))` across opposite sides, and `IsNotNull(attr)` on a join column;
   * anything else fails the whole rewrite closed.
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

    // Only IsNotNull on a join column is safe to drop -- redundant with the join or auto-added by
    // InferFiltersFromConstraints. IsNotNull on any other column changes semantics; bail out.
    val joinAttrIds: Set[ExprId] =
      (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l.exprId, r.exprId) }.toSet
    val isNotNullOnJoinCols = predicates.count {
      case IsNotNull(a: Attribute) if joinAttrIds.contains(a.exprId) => true
      case _ => false
    }

    val totalMatched = equiPairs.size + neqPairs.size + isNotNullOnJoinCols
    if (totalMatched != predicates.size) return None
    if (equiPairs.isEmpty || neqPairs.isEmpty) return None

    // A single inequality only: MIN/MAX over one column cannot represent multiple neqs.
    if (neqPairs.size != 1) return None

    // The rewrite swaps comparison equality (`=`/`<>`) for grouping and MIN/MAX ordering equality,
    // so gate every equi-key and the neq column -- both ends of each pair, since canonicalization
    // drops the metadata that `isSafeComparisonGroupingAttribute` reads and the two ends may differ
    // -- on a positive type allowlist. Fail closed on anything not proven safe.
    val keyAttrs = (equiPairs ++ neqPairs).flatMap { case (l, r) => Seq(l, r) }
    if (!keyAttrs.forall(isSafeComparisonGroupingAttribute)) return None

    // The rewrite expresses "two or more distinct values" as MIN(neq) <> MAX(neq), so the neq
    // column must be orderable. The allowlist above already implies this, but assert Spark's own
    // MIN/MAX input contract (RowOrdering.isOrderable, the same check Min/Max run) explicitly, so
    // the requirement is visible at the rewrite site.
    if (!RowOrdering.isOrderable(neqPairs.head._1.dataType)) return None

    // Resolve each predicate end by ExprId and require matching output ordinals, not name equality
    // (canonicalization erases cosmetic Alias names).
    val equiValid =
      equiPairs.forall { case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r) }
    val neqValid = neqPairs.forall { case (l, r) => sameOutputPosition(leftPlan, rightPlan, l, r) }
    if (!equiValid || !neqValid) return None

    // Equi-key output positions must be distinct, so swapped/duplicate aliases cannot collide.
    val leftEquiOrdinals = equiPairs.map { case (l, _) => outputOrdinal(leftPlan, l) }
    if (leftEquiOrdinals.exists(_ < 0)) return None
    if (leftEquiOrdinals.distinct.size != leftEquiOrdinals.size) return None

    // Reject when the neq column overlaps an equi-key column (e.g. `t1.k = t2.k AND t1.k <> t2.k`).
    val neqLeftOrdinal = outputOrdinal(leftPlan, neqPairs.head._1)
    if (neqLeftOrdinal < 0 || leftEquiOrdinals.contains(neqLeftOrdinal)) return None
    Some((equiPairs, neqPairs))
  }

  /**
   * Type gate applied to each equi-key and the neq column. CHAR/VARCHAR reach the optimizer as
   * StringType with the declared type recorded in the attribute metadata, so recover the raw type
   * from metadata (falling back to `dataType`) before running the datatype allowlist -- otherwise
   * they would slip through the StringType branch.
   */
  private def isSafeComparisonGroupingAttribute(attr: Attribute): Boolean = {
    val rawType = CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType)
    isSafeComparisonGroupingType(rawType)
  }

  /**
   * Positive allowlist of types where comparison equality (`=`/`<>`) provably coincides with
   * grouping and MIN/MAX ordering equality, so a key can move into GROUP BY / MIN-MAX. Float/Double
   * (NaN, signed zero), CHAR/VARCHAR (declared-type/padding), non-binary collated strings, complex
   * types, UDTs / Variant and unknown types fail closed.
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
   * Primary safety guard: the rewrite folds two occurrences of one subtree into a single aggregate,
   * so a plan qualifies only when its operators, leaves and expressions are all allowlisted as
   * repeatable. `plan.deterministic` alone is insufficient -- Aggregate(First), Window row_number
   * over a non-total order and Limit/Sample are row-bag nondeterministic yet report deterministic.
   * Embedded expression subqueries also fail closed.
   */
  private def isRepeatablePlan(plan: LogicalPlan): Boolean = {
    plan.deterministic &&
    !plan.isStreaming &&
    plan.subqueriesAll.isEmpty &&
    isRowBagRepeatable(plan) &&
    hasRepeatableExpressions(plan)
  }

  /**
   * Operator/leaf allowlist for repeatable row bags; everything unknown fails closed. Kept narrow:
   * the target shape needs only a Parquet scan optionally wrapped in Project / Filter /
   * SubqueryAlias plus the self-join. Row ordering is irrelevant to the row-bag contract.
   *
   * The narrowness is deliberate, not a correctness requirement, but it stays intentionally
   * conservative: the exact-`ParquetFileFormat` leaf check admits only stock Parquet scans. Other
   * file formats (ORC, JSON, CSV) reach the same FileSourceScan but remain rejected until each is
   * separately validated, and likewise for inverting the allowlist into a leaf blocklist. This
   * keeps the rule fail-closed on any leaf not proven repeatable.
   */
  private def isRowBagRepeatable(plan: LogicalPlan): Boolean = !plan.exists {
    // Whitelisted operator => false ("does not break repeatability"); negating `exists` then means
    // "every operator is whitelisted".
    case _: Project => false
    case _: Filter => false
    case _: SubqueryAlias => false
    case _: Join => false
    case _: Range => false
    case _: LocalRelation => false
    case relation: LogicalRelation =>
      // Trust a Parquet scan only: exact `ParquetFileFormat` (getClass, not isInstanceOf, since it
      // is non-final); any other FileFormat is not provably repeatable.
      relation.relation match {
        case h: HadoopFsRelation if h.fileFormat.getClass == classOf[ParquetFileFormat] => false
        case _ => true
      }
    case _ => true
  }

  private def hasRepeatableExpressions(plan: LogicalPlan): Boolean = {
    !plan.exists(node => node.expressions.exists(expr => !isRepeatableExpression(expr)))
  }

  /**
   * Expression allowlist: repeatable only when the root type is allowlisted and all children are,
   * so `Add(v, Abs(w))` is rejected. Unknown types fail closed (a missed optimization, not a bug).
   * Decimal wrappers such as `PromotePrecision` / `CheckOverflow` are absent and may fail closed.
   */
  private def isRepeatableExpression(expr: Expression): Boolean = expr match {
    case _: Attribute | _: Literal =>
      true
    // A STRING -> TIMESTAMP_LTZ cast (micro or nanosecond precision) is not repeatable: for a
    // time-only string SparkDateTimeUtils fills the missing date from LocalDate.now(zoneId), which
    // ComputeCurrentTime does not stabilize on this path, so two scans that straddle midnight can
    // produce different timestamps while the rewrite folds them into a single evaluation. The NTZ
    // parse is clock-independent (it returns null for a time-only string), so only the LTZ
    // directions are rejected. Reject whenever such a conversion appears at any nesting level,
    // including inside array/map/struct element casts -- e.g. CAST(CAST(ss AS ARRAY<TIMESTAMP>) AS
    // STRING). Each nested Cast node is itself visited here, so a per-node check catches the inner
    // conversion. Fail closed.
    case c: Cast if castHasClockDependentStringToTimestamp(c.child.dataType, c.dataType) =>
      false
    case _: Alias | _: Cast | _: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder |
        _: And | _: Or | _: Not | _: EqualTo | _: EqualNullSafe | _: LessThan |
        _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual | _: IsNull | _: IsNotNull =>
      expr.children.forall(isRepeatableExpression)
    case _ =>
      false
  }

  /**
   * True when casting `from` to `to` performs a clock-dependent STRING -> TIMESTAMP_LTZ conversion
   * (microsecond or nanosecond precision) at any nesting level: directly, or inside matching
   * array / map / struct element casts. Such a cast supplies a time-only string's missing date from
   * the runtime clock, so it is not repeatable; see [[isRepeatableExpression]]. The scalar
   * String-source decision is delegated to [[Cast.needsTimeZone]], which enumerates exactly the
   * zone-dependent (hence LTZ, hence clock-dependent for a bare time) String conversions and stays
   * in sync as Spark adds timestamp types; String -> TIMESTAMP_NTZ is clock-independent and absent
   * there.
   */
  private def castHasClockDependentStringToTimestamp(from: DataType, to: DataType): Boolean =
    (from, to) match {
      case (s: StringType, t) => Cast.needsTimeZone(s, t)
      case (ArrayType(fromEl, _), ArrayType(toEl, _)) =>
        castHasClockDependentStringToTimestamp(fromEl, toEl)
      case (MapType(fromKey, fromVal, _), MapType(toKey, toVal, _)) =>
        castHasClockDependentStringToTimestamp(fromKey, toKey) ||
          castHasClockDependentStringToTimestamp(fromVal, toVal)
      case (StructType(fromFields), StructType(toFields))
          if fromFields.length == toFields.length =>
        fromFields.zip(toFields).exists { case (f, t) =>
          castHasClockDependentStringToTimestamp(f.dataType, t.dataType)
        }
      case _ => false
    }

  /** True iff `left`/`right` are the same plan modulo canonicalization AND each is repeatable. */
  private def isSameBaseRelation(left: LogicalPlan, right: LogicalPlan): Boolean = {
    left.sameResult(right) &&
    isRepeatablePlan(left) && isRepeatablePlan(right)
  }

  // splitConjunctivePredicates is provided by the mixed-in PredicateHelper trait.
}

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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.{HashMap, HashSet}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{
  Resolver => NameComparator,
  UnresolvedAttribute,
  UnresolvedDataFrameStar
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  AttributeSeq,
  AttributeSet,
  ExprId,
  MetadataAttributeWithLogicalName,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[PlanIdTargetRegistry]] stores resolved DataFrame plan-id targets visible from a scope.
 *
 * A DataFrame column reference carries the `PLAN_ID_TAG` of the node it was taken from, and that
 * node can sit several operators below the reference. For example:
 *
 * {{{
 * val df = spark.range(10).select($"id", ($"id" + 1).as("a"))
 * df.filter($"a" > 5).select(df("id"))
 * }}}
 *
 * tags `df`'s `Project` with a plan id and the `df("id")` reference with the same id:
 *
 * {{{
 * Project ['id]                             -- 'id carries plan_id=1
 * +- Filter ('a > 5)
 *    +- Project [id#0L, (id#0L + 1) AS a#1L] [plan_id=1]
 *       +- Range (0, 10)
 * }}}
 *
 * The registry records that tagged `Project`'s output when it resolves, advances the recorded
 * target up through the enclosing `Filter`, and resolves `'id` against it at the top `Project`,
 * without walking any of the nodes below.
 *
 * A plan-id-tagged reference resolves to the output of the plan node carrying the same id, filtered
 * by the columns still reachable on the path from that node up to the reference. Because
 * single-pass resolution must not traverse existing plan nodes, the registry accumulates targets
 * bottom-up: it records a node's output when the node resolves (`register`), advances every
 * recorded target through each enclosing operator as resolution climbs (`advanceThroughOperator`),
 * and picks a single candidate on lookup (`resolve`), so no top-down re-walk of the plan is needed.
 *
 * As a target advances, each of its attributes is mutated *non-destructively*: it keeps the tagged
 * node's original name and qualifiers (so name matching sees the tagged node's own columns, as the
 * legacy analyzer's `p.resolve` does), only its nullability is refreshed to the enclosing
 * operator's (which an outer join may widen), and a column that drops out of reach is flagged
 * unreachable rather than removed (so an ambiguity present at the tagged node is still reported
 * even after a narrowing projection above it). See [[PlanIdTargetAttribute]].
 */
class PlanIdTargetRegistry {
  private val targetsByPlanId = new HashMap[Long, ArrayBuffer[PlanIdTarget]]

  /** Whether any target has been recorded; used to skip work on plans without plan-id tags. */
  def isEmpty: Boolean = targetsByPlanId.isEmpty

  /**
   * Advances every recorded target through one enclosing operator as resolution climbs the tree.
   *
   * Each target attribute absent from both `output` and `hiddenOutput` is flagged unreachable,
   * because it is no longer reachable from this point in the plan, and every target's depth is
   * incremented so a lookup can prefer a direct match over a deeper one. The
   * `output`/`hiddenOutput` lookups are built once and shared across all targets.
   */
  def advanceThroughOperator(output: Seq[Attribute], hiddenOutput: Seq[Attribute]): Unit = {
    if (!targetsByPlanId.isEmpty) {
      val outputById = createAttributeMap(output)
      val hiddenOutputById = createAttributeMap(hiddenOutput)
      targetsByPlanId.values().forEach { targets =>
        targets.foreach(target => target.advanceThroughOperator(outputById, hiddenOutputById))
      }
    }
  }

  /**
   * Records a resolved target for `planId` once the plan node carrying that id has resolved.
   *
   * It captures that node's `output` together with the qualified-access / metadata columns (from
   * `hiddenOutput`) as a separate hidden `metadataOutput`, so a later lookup can resolve a
   * reference either as a regular column over the output, or as a metadata column for
   * `IS_METADATA_COL` access.
   */
  def register(planId: Long, output: Seq[Attribute], hiddenOutput: Seq[Attribute]): Unit = {
    val outputIds = createAttributeIds(output)
    val metadataOutput = hiddenOutput.filter { attribute =>
      !outputIds.contains(attribute.exprId) &&
      (attribute.isMetadataCol || attribute.qualifiedAccessOnly)
    }
    val target = new PlanIdTarget(
      output = toTargetAttributes(output, hidden = false),
      metadataOutput = toTargetAttributes(metadataOutput, hidden = true),
      depth = 0
    )

    targetsByPlanId.computeIfAbsent(planId, _ => new ArrayBuffer[PlanIdTarget]) += target
  }

  /**
   * Unions the targets of another child registry into this one by plan id.
   *
   * A single plan id can appear on more than one child (e.g. a self join reusing a subplan on both
   * sides), so both children's targets are retained under that id, giving the regular-over-hidden
   * and depth-0-over-deeper disambiguation in `resolve` the full candidate set to choose from.
   */
  def merge(other: PlanIdTargetRegistry): Unit = {
    other.targetsByPlanId.asScala.foreach {
      case (planId, targets) =>
        targetsByPlanId.computeIfAbsent(planId, _ => new ArrayBuffer[PlanIdTarget]) ++= targets
    }
  }

  /**
   * Resolves `unresolvedAttribute` against the targets registered for `planId`.
   *
   * `planId` is supplied by the caller, which reads it off `unresolvedAttribute`'s
   * [[LogicalPlan.PLAN_ID_TAG]] -- the tag whose presence routed the reference here in the first
   * place. When no target was recorded for the id the result is `NoTarget`, which the caller turns
   * into `CANNOT_RESOLVE_DATAFRAME_COLUMN`.
   */
  def resolve(
      unresolvedAttribute: UnresolvedAttribute,
      planId: Long,
      nameComparator: NameComparator): PlanIdResolutionResult = {
    val targets = targetsByPlanId.get(planId)
    if (targets == null) {
      PlanIdResolutionResult.NoTarget
    } else {
      pickCandidate(unresolvedAttribute, targets, nameComparator)
    }
  }

  /**
   * Resolves `unresolvedDataFrameStar` (`df("*")`) to the output of the plan node carrying its plan
   * id.
   *
   * A target contributes its output only when every column registered at the tagged node is still
   * reachable here (tracked by [[PlanIdTarget.outputComplete]]). This mirrors the fixed-point
   * analyzer's all-or-nothing star reachability: a target that lost a column along the path -- e.g.
   * `df.select("id").select(df("*"))` where the intervening projection drops a column -- is
   * discarded, matching the fixed-point analyzer dropping the whole star rather than a subset. A
   * column that is still reachable but was pushed to hidden output by an intervening operator (e.g.
   * a NATURAL/USING join moving the duplicate key out of the regular output) is retained: the star
   * expands to [[PlanIdTarget.reachableOutputAttributes]], which includes such columns, matching
   * the fixed-point analyzer expanding `df("*")` to the tagged node's full original output.
   *
   * Throws `CANNOT_RESOLVE_DATAFRAME_COLUMN` when no complete target matches the plan id (an
   * illegal cross-DataFrame reference, or a target below a set-operation leaf boundary) and
   * `AMBIGUOUS_COLUMN_REFERENCE` when more than one does (e.g. a self-join reusing the subplan on
   * both sides).
   */
  def resolveStar(unresolvedDataFrameStar: UnresolvedDataFrameStar): Seq[Attribute] = {
    val targets = targetsByPlanId.get(unresolvedDataFrameStar.planId)
    val completeTargets =
      if (targets == null) Seq.empty else targets.iterator.filter(_.outputComplete).toSeq
    completeTargets match {
      case Seq(target) =>
        target.reachableOutputAttributes
      case Seq() =>
        throw QueryCompilationErrors.cannotResolveDataFrameColumn(unresolvedDataFrameStar)
      case _ =>
        throw QueryCompilationErrors.ambiguousColumnReferences(unresolvedDataFrameStar)
    }
  }

  /**
   * Chooses a single candidate from the recorded targets, or reports that the plan id matched a
   * node but no column resolved against it.
   *
   * A plan id can resolve to several candidates: `register` appends one per resolved node and
   * `merge` keeps a target per branch, so a DataFrame reused on both sides of a self-join has two.
   * They are reduced to one in this priority order:
   *
   *  1. No candidate resolves the name at any matched target: return `MatchedWithoutCandidate` and
   *     fall back to the normal name-resolution path.
   *     Example: `df.select(df("v")).sort(df("id"))` -- `df("id")` matches `df`, but `id` was
   *     dropped by the select, so it resolves later without a plan id.
   *  2. Regular over hidden: a candidate never hidden on its path wins over one hidden at the
   *     tagged node or at any operator since (see [[PlanIdTargetAttribute.hidden]]).
   *     Example: `df1.join(df2, "a").select(df1("a"))` picks the join's regular `a`, not the
   *     hidden per-side key.
   *  3. Direct over deeper: a depth-0 match (the exact tagged node) wins over a deeper one.
   *     Example: with
   *       `df2 = df1.withColumnRenamed("a", "b")`,
   *       `df1.join(df2, df1("a") === df2("b"))`
   *     takes df1's direct match over the nested one.
   *  4. Otherwise the reference is ambiguous: throw `AMBIGUOUS_COLUMN_REFERENCE`.
   *     Example: `df.join(df).select(df("a"))` -- `df("a")` matches both sides equally.
   */
  private def pickCandidate(
      unresolvedAttribute: UnresolvedAttribute,
      targets: ArrayBuffer[PlanIdTarget],
      nameComparator: NameComparator): PlanIdResolutionResult = {
    val targetSeq = targets.toSeq
    val candidates = targetSeq.flatMap(
      _.resolve(
        nameParts = unresolvedAttribute.nameParts,
        isMetadataAccess = unresolvedAttribute.containsTag(
          LogicalPlan.IS_METADATA_COL
        ),
        nameComparator = nameComparator
      )
    )

    if (candidates.isEmpty) {
      PlanIdResolutionResult.MatchedWithoutCandidate
    } else {
      val (regularCandidates, hiddenCandidates) = candidates.partition(!_.hidden)
      val finalists = if (regularCandidates.nonEmpty) regularCandidates else hiddenCandidates
      val candidate = finalists.sortBy(_.depth).foldLeft(Option.empty[PlanIdCandidate]) {
        case (None, currentCandidate) =>
          Some(currentCandidate)
        case (Some(currentCandidate), nextCandidate)
            if currentCandidate.depth == 0 && nextCandidate.depth != 0 =>
          Some(currentCandidate)
        case _ =>
          throw QueryCompilationErrors.ambiguousColumnReferences(unresolvedAttribute)
      }.get

      PlanIdResolutionResult.Resolved(toNameTarget(candidate))
    }
  }

  /**
   * Wraps the chosen candidate in a [[NameTarget]], unwrapping an `Alias` candidate to its child
   * with the alias name carried separately.
   */
  private def toNameTarget(candidate: PlanIdCandidate): NameTarget = {
    candidate.expression match {
      case Alias(child, aliasName) =>
        NameTarget(candidates = Seq(child), aliasName = Some(aliasName))
      case other =>
        NameTarget(candidates = Seq(other))
    }
  }

  /**
   * Copies `attributes` into a fresh buffer of reachable [[PlanIdTargetAttribute]]s for a new
   * target.
   */
  private def toTargetAttributes(
      attributes: Seq[Attribute],
      hidden: Boolean): ArrayBuffer[PlanIdTargetAttribute] = {
    val result = new ArrayBuffer[PlanIdTargetAttribute](attributes.size)
    attributes.foreach { attribute =>
      result += new PlanIdTargetAttribute(attribute, hidden = hidden, reachable = true)
    }
    result
  }

  /**
   * Builds an `ExprId` lookup set so `register` can partition output from metadata columns in O(1).
   */
  private def createAttributeIds(attributes: Seq[Attribute]): HashSet[ExprId] = {
    val result = new HashSet[ExprId](attributes.size)
    attributes.foreach(attribute => result.add(attribute.exprId))
    result
  }

  /**
   * Builds an `ExprId`-keyed lookup so `advanceThroughOperator` can, in O(1), both test each target
   * attribute against an operator's output and fetch that operator's current version of it (whose
   * nullability -- e.g. widened by an outer join -- may differ from the originally registered one).
   */
  private def createAttributeMap(attributes: Seq[Attribute]): HashMap[ExprId, Attribute] = {
    val result = new HashMap[ExprId, Attribute](attributes.size)
    attributes.foreach(attribute => result.put(attribute.exprId, attribute))
    result
  }
}

/**
 * A single recorded resolution target: one plan node whose id was matched, captured so a
 * plan-id-tagged reference can resolve against it without walking the plan.
 *
 * `output` and `metadataOutput` are the columns visible at that node; each attribute keeps the
 * tagged node's original name/qualifiers for matching and tracks its own reachability and current
 * nullability as resolution climbs (see [[PlanIdTargetAttribute]]). `depth` is how many operators
 * resolution has climbed since it was recorded -- used to prefer direct matches over deeper ones.
 *
 * {{{
 * // b = createDataFrame(..., "id int, q struct<id:int>")
 * // b.alias("q").select(b("q.id"))
 * //   the attributes keep b's names [id, q] (no `q` qualifier), so `q.id` resolves to the struct
 * //   field q.id -- not to the alias-qualified column `id` the enclosing SubqueryAlias exposes.
 * }}}
 */
class PlanIdTarget(
    val output: ArrayBuffer[PlanIdTargetAttribute],
    val metadataOutput: ArrayBuffer[PlanIdTargetAttribute],
    var depth: Int) {

  /**
   * All attributes still reachable at the current operator, including ones now visible only via
   * hidden output (e.g. a NATURAL/USING join key moved out of the regular output). This is what
   * `df("*")` expands to (see [[PlanIdTargetRegistry.resolveStar]]): the tagged node's full
   * original output, in original order, minus only the columns that became unreachable.
   */
  def reachableOutputAttributes: Seq[Attribute] =
    output.iterator.filter(_.reachable).map(_.attribute).toSeq

  /**
   * Whether every column registered at the tagged node is still reachable here, so `df("*")` can
   * expand to the target's full original output (see [[PlanIdTargetRegistry.resolveStar]]). A
   * column dropped by an intervening operator clears it, mirroring the fixed-point analyzer's
   * all-or-nothing star reachability. A still-reachable column pushed to hidden output (e.g. a
   * NATURAL/USING join key) does not clear it -- `df("*")` still includes it, so completeness turns
   * on reachability alone, not on whether the column stayed in the regular output.
   */
  def outputComplete: Boolean = output.forall(_.reachable)

  /**
   * Advances this target's attributes through one enclosing operator and increments `depth`.
   *
   * Each attribute refreshes its nullability and reachability against the operator (see
   * [[PlanIdTargetAttribute.advanceThrough]]); nothing is removed, so name matching keeps seeing
   * the tagged node's full original output.
   *
   * `hiddenOutputById` here is expected to already have columns shadowed by a same-named regular
   * output column removed (see `NameScopeStack.updatePlanIdTargetsForCurrentOperator`), so an
   * attribute overwritten by e.g. `withColumn` does not linger as a stale plan-id candidate.
   */
  def advanceThroughOperator(
      outputById: HashMap[ExprId, Attribute],
      hiddenOutputById: HashMap[ExprId, Attribute]): Unit = {
    output.foreach(_.advanceThrough(outputById, hiddenOutputById))
    metadataOutput.foreach(_.advanceThrough(outputById, hiddenOutputById))
    depth += 1
  }

  /**
   * Resolves `nameParts` against this target, producing candidates tagged with depth and hidden.
   *
   * A metadata access (`IS_METADATA_COL`) is resolved against the metadata columns, everything else
   * against the regular output. Each resulting candidate is tagged `hidden` when all its references
   * are hidden (see [[PlanIdTargetAttribute.hidden]]), so `pickCandidate` can apply the
   * regular-over-hidden preference.
   */
  def resolve(
      nameParts: Seq[String],
      isMetadataAccess: Boolean,
      nameComparator: NameComparator): Seq[PlanIdCandidate] = {
    val candidateExpressions = if (isMetadataAccess) {
      resolveMetadataAttribute(nameParts, nameComparator)
    } else {
      resolveOutputAttribute(nameParts, nameComparator)
    }
    val hiddenAttributes = hiddenReachableAttributes

    candidateExpressions.map { expression =>
      PlanIdCandidate(
        expression = expression,
        depth = depth,
        hidden = expression.references.nonEmpty &&
          expression.references.subsetOf(hiddenAttributes)
      )
    }
  }

  /**
   * Resolves a non-metadata reference against the target, preferring regular over metadata output.
   *
   * A DataFrame column resolves as a regular column when possible and only otherwise as a metadata
   * column: try the regular output first, and only if nothing matches try the metadata output.
   */
  private def resolveOutputAttribute(
      nameParts: Seq[String],
      nameComparator: NameComparator): Seq[NamedExpression] = {
    val resolvedOutputAttributes = resolveAttribute(
      attributesForResolution = AttributeSeq.fromNormalOutput(output.map(_.attribute).toSeq),
      nameParts = nameParts,
      nameComparator = nameComparator
    )

    if (resolvedOutputAttributes.nonEmpty) {
      resolvedOutputAttributes
    } else {
      resolveAttribute(
        attributesForResolution = new AttributeSeq(metadataOutput.map(_.attribute).toSeq),
        nameParts = nameParts,
        nameComparator = nameComparator
      )
    }
  }

  /**
   * Resolves `nameParts` against all of the tagged node's original columns, then keeps the result
   * only if it is still reachable.
   *
   * Matching (including nested-field extraction and ambiguity) runs against every attribute -- even
   * ones no longer reachable -- via [[AttributeSeq.resolve]], so qualifiers and ambiguity are
   * decided by the tagged node's own columns exactly as the legacy analyzer's `p.resolve` does: two
   * columns matching the name raise `AMBIGUOUS_REFERENCE` here even if one was dropped above. A
   * single match whose attribute is no longer reachable is dropped, so a `Sort`/`Filter` above can
   * recover it or name resolution can retry without a plan id. The matched attribute already
   * carries the current nullability, because [[PlanIdTargetAttribute.advanceThrough]] refreshed it
   * in place.
   */
  private def resolveAttribute(
      attributesForResolution: AttributeSeq,
      nameParts: Seq[String],
      nameComparator: NameComparator): Seq[NamedExpression] = {
    attributesForResolution.resolve(nameParts, nameComparator) match {
      case Some(expression) if expression.references.subsetOf(reachableAttributes) =>
        Seq(expression)
      case _ =>
        Seq.empty
    }
  }

  /**
   * Resolves a single-part metadata reference (`IS_METADATA_COL` access) against the target.
   *
   * Metadata access resolves only a single name part, matched against each still-reachable
   * attribute's logical metadata name, and yields at most one attribute. A multi-part metadata
   * reference resolves to nothing.
   */
  private def resolveMetadataAttribute(
      nameParts: Seq[String],
      nameComparator: NameComparator): Seq[NamedExpression] = {
    if (nameParts.size == 1) {
      (metadataOutput.iterator ++ output.iterator).filter(_.reachable).flatMap { targetAttribute =>
        targetAttribute.attribute match {
          case attributeReference: AttributeReference =>
            MetadataAttributeWithLogicalName.unapply(attributeReference) match {
              case Some((attribute, logicalName)) if nameComparator(nameParts.head, logicalName) =>
                Some(attribute)
              case _ =>
                None
            }
          case _ =>
            None
        }
      }.take(1).toSeq
    } else {
      Seq.empty
    }
  }

  /** All attributes still reachable at the current operator (regular and metadata). */
  private def reachableAttributes: AttributeSet =
    AttributeSet(
      (output.iterator ++ metadataOutput.iterator).filter(_.reachable).map(_.attribute).toSeq
    )

  /**
   * The reachable attributes that were hidden at the tagged node or at any operator since (see
   * [[PlanIdTargetAttribute.hidden]]).
   */
  private def hiddenReachableAttributes: AttributeSet =
    AttributeSet(
      (output.iterator ++ metadataOutput.iterator)
        .filter(entry => entry.reachable && entry.hidden)
        .map(_.attribute)
        .toSeq
    )
}

/**
 * One attribute of a [[PlanIdTarget]], mutated non-destructively as resolution climbs.
 *
 * It keeps the tagged node's original `attribute` (name and qualifiers) for name matching and only
 * refreshes that attribute's nullability to the enclosing operator's; `reachable` records whether
 * the column is still reachable (it is flagged, never removed, so an ambiguity present at the
 * tagged node survives a narrowing projection above it), and `hidden` whether it was hidden
 * (metadata / side-channel only) at the tagged node or at any operator since; once set, `hidden`
 * stays set even if an operator above re-selects the column.
 */
class PlanIdTargetAttribute(
    var attribute: Attribute,
    var hidden: Boolean,
    var reachable: Boolean) {

  /**
   * Advances this attribute through one enclosing operator, in place.
   *
   * An attribute still in the operator's regular output keeps its original name/qualifiers and
   * adopts that operator's nullability; one that survives only in the hidden output is likewise
   * refreshed and marked `hidden` (e.g. a natural/USING join key accessible only as a metadata
   * column); one in neither is flagged unreachable but kept, so it still participates in
   * ambiguity detection. Once unreachable it stays unreachable.
   */
  def advanceThrough(
      outputById: HashMap[ExprId, Attribute],
      hiddenOutputById: HashMap[ExprId, Attribute]): Unit = {
    if (reachable) {
      if (outputById.containsKey(attribute.exprId)) {
        attribute = attribute.withNullability(outputById.get(attribute.exprId).nullable)
      } else if (hiddenOutputById.containsKey(attribute.exprId)) {
        attribute = attribute.withNullability(hiddenOutputById.get(attribute.exprId).nullable)
        hidden = true
      } else {
        reachable = false
      }
    }
  }
}

/**
 * A resolved candidate for a plan-id-tagged reference, threaded through disambiguation.
 *
 * `depth` is how far the target sits from the current operator and `hidden` marks a match whose
 * references were hidden at some operator on the path to it, even if re-selected above (see
 * [[PlanIdTargetAttribute.hidden]]) -- the two keys `pickCandidate` uses to break ties.
 */
case class PlanIdCandidate(
    expression: NamedExpression,
    depth: Int,
    hidden: Boolean)

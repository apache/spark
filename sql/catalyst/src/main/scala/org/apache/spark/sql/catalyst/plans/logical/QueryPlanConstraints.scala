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

package org.apache.spark.sql.catalyst.plans.logical

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils.isBinaryStable
import org.apache.spark.sql.types.UpCastRule


trait QueryPlanConstraints extends ConstraintHelper { self: LogicalPlan =>

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = {
    if (conf.constraintPropagationEnabled) {
      validConstraints
        .union(inferAllAdditionalConstraints(validConstraints))
        .union(constructIsNotNullConstraints(validConstraints, output))
        .filter { c =>
          c.references.nonEmpty && c.references.subsetOf(outputSet) && c.deterministic
        }
    } else {
      ExpressionSet()
    }
  }

  /**
   * This method can be overridden by any child class of QueryPlan to specify a set of constraints
   * based on the given operator's constraint propagation logic. These constraints are then
   * canonicalized and filtered automatically to contain only those attributes that appear in the
   * [[outputSet]].
   *
   * See [[Expression#canonicalized]] for more details.
   */
  protected lazy val validConstraints: ExpressionSet = ExpressionSet()
}

trait ConstraintHelper {

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   */
  def inferAdditionalConstraints(constraints: ExpressionSet): ExpressionSet = {
    var inferredConstraints = ExpressionSet()
    // IsNotNull should be constructed by `constructIsNotNullConstraints`.
    val predicates = constraints.filterNot(_.isInstanceOf[IsNotNull])
    predicates.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        // Also remove EqualNullSafe with the same l and r to avoid Once strategy's idempotence
        // is broken. l = r and l <=> r can infer l <=> l and r <=> r which is useless.
        val candidateConstraints = predicates - eq - EqualNullSafe(l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, r, l)
      case eq @ EqualTo(l @ Cast(_: Attribute, _, _, _), r: Attribute) =>
        inferredConstraints ++= replaceConstraints(predicates - eq - EqualNullSafe(l, r), r, l)
      case eq @ EqualTo(l: Attribute, r @ Cast(_: Attribute, _, _, _)) =>
        inferredConstraints ++= replaceConstraints(predicates - eq - EqualNullSafe(l, r), l, r)
      case _ => // No inference
    }

    inferredConstraints -- constraints
  }

  /**
   * Infers additional constraints by substituting known attribute-to-literal bindings into
   * non-equality predicates. For example, given `a = 5` and `b >= a`, infers `b >= 5`.
   *
   * Attribute-to-attribute (and cast-form) [[EqualTo]] and all [[EqualNullSafe]] predicates are
   * excluded from substitution targets.
   * Substituting into attribute-attribute [[EqualTo]] would duplicate work already done by the
   * transitivity case in [[inferAdditionalConstraints]] (e.g. `a = 5` and `b = a` imply `b = 5`,
   * already derived there). Substituting into [[EqualNullSafe]] would produce a structurally
   * distinct form (e.g. `b <=> 5`) that downstream rules (such as subquery-reuse matching)
   * treat differently from the [[EqualTo]] form, leading to duplicate subqueries being generated.
   */
  def inferConstraintsFromLiteralBindings(constraints: ExpressionSet): ExpressionSet = {
    // Collect attr -> literal bindings, guarded by binary-stable collation so that
    // the substitution is semantically safe (non-binary-stable collations may equate
    // strings that are binary-distinct, so substituting the literal would change results).
    val bindings: Map[Attribute, Literal] = constraints.collect {
      case EqualTo(a: Attribute, l: Literal) if isBinaryStable(a.dataType) => a -> l
      case EqualTo(l: Literal, a: Attribute) if isBinaryStable(a.dataType) => a -> l
    }.toMap

    if (bindings.isEmpty) return ExpressionSet()

    val targets = constraints.filterNot {
      // attr=attr EqualTo: already covered by inferAdditionalConstraints (transitivity).
      // cast-equality EqualTo: would produce redundant cast literals derivable from that path.
      // EqualNullSafe: substituting a literal produces a structurally distinct b <=> lit form
      // that subquery-reuse matching treats differently from b = lit, causing duplicate subqueries.
      // IsNotNull: handled separately by constructIsNotNullConstraints.
      case EqualTo(_: Attribute, _: Attribute) => true
      case EqualTo(Cast(_: Attribute, _, _, _), _: Attribute) => true
      case EqualTo(_: Attribute, Cast(_: Attribute, _, _, _)) => true
      case _: EqualNullSafe | _: IsNotNull => true
      case _ => false
    }

    var inferred = ExpressionSet()
    bindings.foreach { case (attr, lit) =>
      inferred ++= replaceConstraints(targets, attr, lit)
    }
    inferred -- constraints
  }

  /**
   * Infers the full additional constraint set for a given set of constraints by combining the
   * equality-based inferences above ([[inferAdditionalConstraints]],
   * [[inferConstraintsFromLiteralBindings]]) with transitive inequality chaining
   * ([[inferInequalityConstraints]]). The two equality-based passes never produce a new literal
   * binding (an `attr = literal` equality), only new inequalities or substitutions into existing
   * predicates, so a single downstream call to [[inferInequalityConstraints]] sees every literal
   * bound they can ever contribute; it does not need to be interleaved with them.
   */
  def inferAllAdditionalConstraints(constraints: ExpressionSet): ExpressionSet = {
    val equalityInferred =
      inferAdditionalConstraints(constraints) ++ inferConstraintsFromLiteralBindings(constraints)
    equalityInferred ++ inferInequalityConstraints(constraints ++ equalityInferred)
  }

  /**
   * Infers the full set of constraints derivable by transitively chaining inequality constraints.
   * For e.g., if an operator has constraints of the form (`a > b`, `b > 5`), this returns an
   * additional constraint of the form `a > 5`; a longer chain such as `a < b && b < c && c < 5`
   * derives both `b < 5` and `a < 5` in this same call.
   *
   * This is computed as a worklist over a directed bound graph rather than a repeated fixed point
   * over the full candidate set: each candidate comparison becomes a directed edge once, and each
   * newly discovered bound is propagated only along its adjacent edges, instead of rescanning the
   * full candidates x bounds product after every newly inferred bound (which costs O(n^3) on a
   * sparse n-element chain such as `x1 < x2 < ... < xn < lit`). A site only ever keeps its single
   * tightest bound per direction, so reaching it via multiple literals (e.g. `b < 5` and `b < 10`
   * both reaching `a` through `a < b`) converges to one inferred predicate instead of one per
   * literal; a relaxation only re-enters the worklist when it strictly tightens that site's bound,
   * which bounds the number of relaxations per site by the number of distinct literals that can
   * reach it and guarantees termination.
   */
  private def inferInequalityConstraints(constraints: ExpressionSet): ExpressionSet = {
    // Admits only comparisons between binary-stable types. This is a conservative guard, not a
    // strict requirement: chaining never substitutes a value the way
    // inferConstraintsFromLiteralBindings does, it only reuses an operand under the same
    // collation-aware comparison, so a non-binary-stable collation could in principle still order
    // consistently with its binary form here. EqualTo is admitted only when at least one side is
    // foldable (a literal bound); every other EqualTo -- attribute-to-attribute (handled by
    // inferAdditionalConstraints's transitivity), as well as any other shape without a foldable
    // side such as `a + 1 = b` -- is excluded, since no inference path below handles a
    // non-foldable equality operand.
    val validComparisons = constraints.toSeq.filter {
      case b: BinaryComparison =>
        b.deterministic && !b.throwable && !b.isInstanceOf[EqualNullSafe] &&
          (!b.isInstanceOf[EqualTo] || b.left.foldable || b.right.foldable) &&
          isBinaryStable(b.left.dataType) && isBinaryStable(b.right.dataType)
      case _ => false
    }

    def hasFoldableOperand(e: Expression): Boolean = e match {
      case b: BinaryComparison => b.left.foldable || b.right.foldable
      case _ => false
    }

    if (!validComparisons.exists(hasFoldableOperand)) {
      return ExpressionSet()
    }

    // Canonicalize comparisons into LessThan / LessThanOrEqual direction. EqualTo is expanded
    // into both directions, unlike GreaterThan/GreaterThanOrEqual, because an equality acts as
    // both an upper and a lower bound and the matching below is direction-sensitive on which
    // side carries the literal. Kept as a Seq, not an ExpressionSet: ExpressionSet canonicalizes
    // EqualTo(l, r) and EqualTo(r, l) to the same entry and would silently keep only one of the
    // two directions just expanded.
    val canonical = validComparisons.flatMap {
      case GreaterThan(l, r) => LessThan(r, l) :: Nil
      case GreaterThanOrEqual(l, r) => LessThanOrEqual(r, l) :: Nil
      case EqualTo(l, r) => EqualTo(l, r) :: EqualTo(r, l) :: Nil
      case other => other :: Nil
    }

    val (initialBounds, candidates) = canonical.partition(hasFoldableOperand)
    if (initialBounds.isEmpty || candidates.isEmpty) return ExpressionSet()

    // A candidate operand is only safe to copy into a newly inferred, single-side filter when
    // doing so cannot raise an error on rows that never evaluated it before: an attribute
    // reference, or a lossless (non-truncating, non-failing) widening cast of one. Arbitrary
    // expressions -- a parsing/narrowing cast, arithmetic that can overflow under ANSI, etc. --
    // are excluded, since the comparison they came from may have been a cross-side join
    // condition that only ever ran on matched rows.
    def isSafeToRelocate(e: Expression): Boolean = e match {
      case _: Attribute => true
      case Cast(a: Attribute, toType, _, _) => UpCastRule.canUpCast(a.dataType, toType)
      case _ => false
    }

    // Bound represents a known literal bound on `site`: `site {<,<=} lit` when `upper`, or
    // `lit {<,<=} site` otherwise.
    case class Bound(upper: Boolean, site: Expression, lit: Expression, strict: Boolean)

    def toBound(e: Expression): Option[Bound] = e match {
      case b: BinaryComparison =>
        val strict = b.isInstanceOf[LessThan]
        if (b.right.foldable) Some(Bound(upper = true, b.left, b.right, strict))
        else if (b.left.foldable) Some(Bound(upper = false, b.right, b.left, strict))
        else None
      case _ => None
    }

    // Index candidate edges by the semantic key of the endpoint a bound would match, so a newly
    // discovered bound only revisits the edges that touch it instead of the full candidate list.
    // An edge `l < r` (or `l <= r`) propagates an upper bound on `r` to `l`, and a lower bound on
    // `l` to `r`.
    val upperEdges = new mutable.HashMap[Expression, mutable.Buffer[(Expression, Boolean)]]
    val lowerEdges = new mutable.HashMap[Expression, mutable.Buffer[(Expression, Boolean)]]
    candidates.foreach {
      case c @ BinaryComparison(l, r) =>
        val strict = c.isInstanceOf[LessThan]
        if (isSafeToRelocate(l)) {
          upperEdges.getOrElseUpdate(r.canonicalized, mutable.Buffer.empty) += (l -> strict)
        }
        if (isSafeToRelocate(r)) {
          lowerEdges.getOrElseUpdate(l.canonicalized, mutable.Buffer.empty) += (r -> strict)
        }
      case _ =>
    }

    if (upperEdges.isEmpty && lowerEdges.isEmpty) return ExpressionSet()

    // Key: (upper, site.canonicalized) -> tightest bound discovered for that site so far, keyed
    // without the literal so that bounding the same site via different literals (e.g. `b < 5` and
    // `b < 10` both reaching `a` through `a < b`) converges to the single tightest predicate
    // instead of emitting one inferred conjunct per literal it was ever bounded by.
    type BoundKey = (Boolean, Expression)
    val bestBound = new mutable.HashMap[BoundKey, Bound]
    val inferredBounds = new mutable.LinkedHashMap[BoundKey, Bound]
    val worklist = new mutable.Queue[Bound]

    // True when `candidate` improves on `current` for the same key: a smaller upper-bound literal
    // (or larger lower-bound one), or an equal literal newly made strict. Compares with the same
    // per-type ordering BinaryComparison.eval itself uses, so tightness agrees with the actual
    // `<`/`<=` runtime semantics (decimal scale, etc.) instead of a parallel notion of "smaller".
    // A null literal is treated as incomparable and never wins, matching the SQL three-valued
    // semantics where a comparison against NULL only ever filters, never tightens.
    def isTighter(upper: Boolean, candidate: Bound, current: Bound): Boolean = {
      val a = candidate.lit.eval()
      val b = current.lit.eval()
      if (a == null || b == null) {
        false
      } else {
        val cmp = TypeUtils.getInterpretedOrdering(candidate.site.dataType).compare(a, b)
        if (cmp != 0) (if (upper) cmp < 0 else cmp > 0) else candidate.strict && !current.strict
      }
    }

    // Relaxes the best known bound for `b`'s (direction, site) key: only a genuine improvement
    // re-enters the worklist, which bounds the number of relaxations per key by the number of
    // distinct literals that can ever reach it and guarantees termination.
    def relax(b: Bound, viaEdge: Boolean): Unit = {
      val key = (b.upper, b.site.canonicalized)
      if (bestBound.get(key).forall(isTighter(b.upper, b, _))) {
        bestBound(key) = b
        worklist += b
        if (viaEdge) inferredBounds(key) = b
      }
    }

    initialBounds.flatMap(toBound(_)).foreach(relax(_, viaEdge = false))

    while (worklist.nonEmpty) {
      val bound = worklist.dequeue()
      val key = (bound.upper, bound.site.canonicalized)
      // Skip propagating a bound that a later, tighter relaxation has already superseded.
      if (bestBound.get(key).contains(bound)) {
        val edges = if (bound.upper) upperEdges else lowerEdges
        edges.get(bound.site.canonicalized).foreach(_.foreach { case (other, edgeStrict) =>
          relax(Bound(bound.upper, other, bound.lit, bound.strict || edgeStrict), viaEdge = true)
        })
      }
    }

    val inferred = inferredBounds.values.map { b =>
      if (b.upper) {
        if (b.strict) LessThan(b.site, b.lit) else LessThanOrEqual(b.site, b.lit)
      } else {
        if (b.strict) GreaterThan(b.site, b.lit) else GreaterThanOrEqual(b.site, b.lit)
      }
    }

    ExpressionSet(inferred.toSeq) -- constraints
  }

  private def replaceConstraints(
      constraints: ExpressionSet,
      source: Expression,
      destination: Expression): ExpressionSet = {
    if (isBinaryStable(source.dataType)) {
      constraints.map(_ transform {
        case e: Expression if e.semanticEquals(source) => destination
      })
    } else {
      constraints.map(_ transform {
        case b: BinaryComparison if sameCollationOperand(b, source) =>
          b.withNewChildren(b.children.map { c =>
            if (c.semanticEquals(source)) destination else c
          })
      })
    }
  }

  private def sameCollationOperand(b: BinaryComparison, source: Expression): Boolean =
    (b.left.semanticEquals(source) || b.right.semanticEquals(source)) &&
      b.left.dataType == source.dataType && b.right.dataType == source.dataType

  /**
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  def constructIsNotNullConstraints(
      constraints: ExpressionSet,
      output: Seq[Attribute]): ExpressionSet = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints = constraints.flatMap(inferIsNotNullConstraints(_))

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull)

    isNotNullConstraints -- constraints
  }

  /**
   * Infer the Attribute-specific IsNotNull constraints from the null intolerant child expressions
   * of constraints.
   */
  private def inferIsNotNullConstraints(constraint: Expression): Seq[Expression] =
    constraint match {
      // When the root is IsNotNull, we can push IsNotNull through the child null intolerant
      // expressions
      case IsNotNull(expr) => scanNullIntolerantAttribute(expr).map(IsNotNull(_))
      // Constraints always return true for all the inputs. That means, null will never be returned.
      // Thus, we can infer `IsNotNull(constraint)`, and also push IsNotNull through the child
      // null intolerant expressions.
      case _ => scanNullIntolerantAttribute(constraint).map(IsNotNull(_))
    }

  @tailrec
  private def isExtractOnly(e: Expression): Boolean = e match {
    case g: GetStructField => isExtractOnly(g.child)
    case g: GetArrayStructFields => isExtractOnly(g.child)
    case _: Attribute => true
    case _ => false
  }


  /**
   * Recursively explores the expressions which are null intolerant and returns all
   * attributes/ExtractValues in these expressions for scalar/nested types respectively.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Expression] = expr match {
    case e: ExtractValue if isExtractOnly(e) => Seq(e)
    case a: Attribute => Seq(a)
    case e if e.nullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }
}

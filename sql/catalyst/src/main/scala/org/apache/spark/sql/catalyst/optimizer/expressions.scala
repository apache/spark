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

import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, Stack}
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{MultiLikeBase, _}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.{AlwaysProcess, TreeNodeTag}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils.isBinaryStable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/*
 * Optimization rules defined in this file should not affect the structure of the logical plan.
 */


/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  // This tag is for avoid repeatedly evaluating expression inside conditional expression
  // which has already failed to evaluate before.
  private[sql] val FAILED_TO_EVALUATE = TreeNodeTag[Unit]("FAILED_TO_EVALUATE")

  private def hasNoSideEffect(e: Expression): Boolean = e match {
    case _: Attribute => true
    case _: Literal => true
    // A cast keeps the eval mode it was analyzed with (for example, in a view), so check it as
    // well. A TRY cast returns null instead of failing, and so do some LEGACY casts. Keep the
    // session check so that this does not start folding casts in ANSI sessions.
    case c: Cast if !conf.ansiEnabled => c.evalMode match {
      case EvalMode.TRY => hasNoSideEffect(c.child)
      case EvalMode.LEGACY if Cast.isNoThrowLegacyCast(c.child.dataType, c.dataType) =>
        hasNoSideEffect(c.child)
      case _ => false
    }
    // Only these two of the expressions marked `NoThrow`, as before the other expressions that
    // are always total were marked with it too, so that this rule folds what it did then.
    // TODO(SPARK-61667): Rely on `Expression.isTotal` instead, to also fold e.g.
    // `size(array(i > 1))`.
    case _: CreateArray | _: CreateNamedStruct if e.deterministic =>
      e.children.forall(hasNoSideEffect)
    case _ => false
  }

  private def tryFold(
      expr: Expression,
      isConditionalBranch: Boolean,
      failed: java.util.Set[Expression]): Expression = {
    try {
      Literal.create(expr.freshCopyIfContainsStatefulExpression().eval(EmptyRow), expr.dataType)
    } catch {
      case NonFatal(_) if isConditionalBranch =>
        // When doing constant folding inside conditional expressions, we should not fail
        // during expression evaluation, as the branch we are evaluating may not be reached at
        // runtime, and we shouldn't fail the query, to match the original behavior.
        if (failed == null) expr.setTagValue(FAILED_TO_EVALUATE, ()) else failed.add(expr)
        expr
    }
  }

  /**
   * Folds the parts of `e` that can be statically evaluated. With `failed`, it adds a part that
   * fails in a conditional branch to it, and skips the parts in it as tagged ones, instead of
   * tagging such a part, which may be shared with a plan, so that a rule can check whether `e`
   * folds without leaving a mark on it.
   */
  private[sql] def constantFolding(
      e: Expression,
      isConditionalBranch: Boolean = false,
      failed: java.util.Set[Expression] = null): Expression = e match {
    case c: ConditionalExpression if !c.foldable =>
      c.mapChildren(constantFolding(_, isConditionalBranch = true, failed))

    // Skip redundant folding of literals. This rule is technically not necessary. Placing this
    // here avoids running the next rule for Literal values, which would create a new Literal
    // object and running eval unnecessarily.
    case l: Literal => l

    // This foldable expression carries planning identity that must survive later optimizer batches.
    case p: PercentileFusionArray => p

    case Size(c: CreateArray, _) if c.children.forall(hasNoSideEffect) =>
      Literal(c.children.length)

    case e if e.containsTag(FAILED_TO_EVALUATE) || (failed != null && failed.contains(e)) => e

    // Fold expressions that are foldable.
    case e if e.foldable => tryFold(e, isConditionalBranch, failed)

    // Don't replace ScalarSubquery if its plan is an aggregate that may suffer from a COUNT bug.
    case s @ ScalarSubquery(_, _, _, _, _, mayHaveCountBug, _)
      if conf.getConf(SQLConf.DECORRELATE_SUBQUERY_PREVENT_CONSTANT_FOLDING_FOR_COUNT_BUG) &&
        mayHaveCountBug.nonEmpty && mayHaveCountBug.get =>
      s

    // Replace ScalarSubquery with null if its maxRows is 0
    case s: ScalarSubquery if s.plan.maxRows.contains(0) =>
      Literal(null, s.dataType)

    case other =>
      val newOther = other.mapChildren(constantFolding(_, isConditionalBranch, failed))
      if (newOther.foldable) {
        tryFold(newOther, isConditionalBranch, failed)
      } else {
        newOther
      }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(AlwaysProcess.fn, ruleId) {
    case q: LogicalPlan => q.mapExpressions(constantFolding(_))
  }
}

/**
 * Substitutes [[Attribute Attributes]] which can be statically evaluated with their corresponding
 * value in conjunctive [[Expression Expressions]]
 * e.g.
 * {{{
 *   SELECT * FROM table WHERE i = 5 AND j = i + 3
 *   ==>  SELECT * FROM table WHERE i = 5 AND j = 8
 * }}}
 *
 * Approach used:
 * - Populate a mapping of attribute => constant value by looking at all the equals predicates,
 *   except, under a `NOT`, the ones over a nullable attribute and any `EqualTo` with a NULL literal
 * - Using this mapping, replace occurrence of the attributes with the corresponding constant values
 *   in the AND node.
 */
object ConstantPropagation extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val childColumns = new ChildColumns(plan)
    // The checks fold a conjunct with the foldable columns of the child in only if
    // `FoldablePropagation`, which puts them into the condition, is not excluded.
    val propagatesFoldables = !excludedRules(FoldablePropagation.ruleName)
    plan.transformUpWithPruning(
      _.containsAllPatterns(LITERAL, FILTER, BINARY_COMPARISON), ruleId) {
      case f: Filter =>
        val (newCondition, _) = traverse(f.condition, replaceChildren = true, nullIsFalse = true,
          new FailableColumns(failableColumnsOf(f.child, childColumns),
            if (propagatesFoldables) foldableColumnsOf(f.child, childColumns)
            else AttributeMap.empty))
        if (newCondition.isDefined) {
          f.copy(condition = newCondition.get)
        } else {
          f
        }
    }
  }

  /**
   * Traverse a condition as a tree and replace attributes with constant values.
   * - On matching [[And]], recursively traverse each children and get propagated mappings.
   *   If the current node is not child of another [[And]], replace all occurrences of the
   *   attributes with the corresponding constant values.
   * - If a child of [[And]] is [[EqualTo]] or [[EqualNullSafe]], propagate the mapping
   *   of attribute => constant, except, under a [[Not]], for one over a nullable attribute and
   *   for an [[EqualTo]] with a NULL literal.
   * - On matching [[Or]] or [[Not]], recursively traverse each children, propagate empty mapping.
   * - Otherwise, stop traversal and propagate empty mapping.
   * @param condition condition to be traversed
   * @param replaceChildren whether to replace attributes with constant values in children
   * @param nullIsFalse whether a boolean expression result can be considered to false e.g. in the
   *                    case of `WHERE e`, null result of expression `e` means the same as if it
   *                    resulted false
   * @param columns the columns of the filter's child that can fail
   * @return A tuple including:
   *         1. Option[Expression]: optional changed condition after traversal
   *         2. AttributeMap: propagated mapping of attribute => constant
   */
  private def traverse(
      condition: Expression,
      replaceChildren: Boolean,
      nullIsFalse: Boolean,
      columns: FailableColumns): (Option[Expression], AttributeMap[(Literal, BinaryComparison)]) =
    condition match {
      case _ if !condition.containsAllPatterns(LITERAL, BINARY_COMPARISON) =>
        (None, AttributeMap.empty)
      case e @ EqualTo(left: AttributeReference, right: Literal)
        if safeToReplace(e, left, nullIsFalse) =>
        (None, AttributeMap(Map(left -> (right, e))))
      case e @ EqualTo(left: Literal, right: AttributeReference)
        if safeToReplace(e, right, nullIsFalse) =>
        (None, AttributeMap(Map(right -> (left, e))))
      case e @ EqualNullSafe(left: AttributeReference, right: Literal)
        if safeToReplace(e, left, nullIsFalse) =>
        (None, AttributeMap(Map(left -> (right, e))))
      case e @ EqualNullSafe(left: Literal, right: AttributeReference)
        if safeToReplace(e, right, nullIsFalse) =>
        (None, AttributeMap(Map(right -> (left, e))))
      case a: And =>
        val (newLeft, equalityPredicatesLeft) =
          traverse(a.left, replaceChildren = false, nullIsFalse, columns)
        val (newRight, equalityPredicatesRight) =
          traverse(a.right, replaceChildren = false, nullIsFalse, columns)
        val equalityPredicates = equalityPredicatesLeft ++ equalityPredicatesRight
        val newSelf = if (equalityPredicates.nonEmpty && replaceChildren) {
          val unreplaced = And(newLeft.getOrElse(a.left), newRight.getOrElse(a.right))
          val (movable, failable) = equalityPredicates.toSeq.partition { case (attr, _) =>
            columns.canEvaluateUnconditionally(attr)
          }
          if (failable.isEmpty) {
            Some(moveEqualitiesFirst(unreplaced, replaceConstants(unreplaced, equalityPredicates),
              equalityPredicates, columns))
          } else {
            Some(propagateInPlace(
              unreplaced, AttributeMap(failable), AttributeMap(movable), nullIsFalse, columns))
          }
        } else {
          if (newLeft.isDefined || newRight.isDefined) {
            Some(And(newLeft.getOrElse(a.left), newRight.getOrElse(a.right)))
          } else {
            None
          }
        }
        (newSelf, equalityPredicates)
      case o: Or =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newLeft, _) = traverse(o.left, replaceChildren = true, nullIsFalse, columns)
        val (newRight, _) = traverse(o.right, replaceChildren = true, nullIsFalse, columns)
        val newSelf = if (newLeft.isDefined || newRight.isDefined) {
          Some(Or(left = newLeft.getOrElse(o.left), right = newRight.getOrElse((o.right))))
        } else {
          None
        }
        (newSelf, AttributeMap.empty)
      case n: Not =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newChild, _) = traverse(n.child, replaceChildren = true, nullIsFalse = false, columns)
        (newChild.map(Not), AttributeMap.empty)
      case _ => (None, AttributeMap.empty)
    }

  // We need to take into account if an attribute is nullable and the context of the conjunctive
  // expression. E.g. `SELECT * FROM t WHERE NOT(c = 1 AND c + 1 = 1)` where attribute `c` can be
  // substituted into `1 + 1 = 1` if 'c' isn't nullable. If 'c' is nullable then the enclosing
  // NOT prevents us to do the substitution as NOT flips the context (`nullIsFalse`) of what a
  // null result of the enclosed expression means. So under a NOT, an `EqualTo` is propagated only
  // if it is never NULL: not over a nullable attribute, nor with a NULL literal, which is NULL on
  // every row even if 'c' isn't nullable. Where a NULL counts as FALSE, an AND with such an
  // `EqualTo` is never TRUE whatever the attribute is replaced with, so propagating it there is
  // safe. An `EqualNullSafe` is never NULL, but, as before, under a NOT it is propagated only over
  // an attribute that is not nullable.
  private def safeToReplace(e: BinaryComparison, ar: AttributeReference, nullIsFalse: Boolean) =
    nullIsFalse || !(ar.nullable || e.nullable)

  /**
   * Propagates the equalities of `condition`, an AND, when some of them, `failable`, are over a
   * column of the filter's child that can fail (see [[FailableColumns]]), e.g. `i = 1` with `i`
   * the alias `CAST(s AS INT)`. Such an equality can fail, so it is not moved ahead of the
   * conjuncts written before it, which would compute the column on rows that they filter out.
   * Instead, its constant replaces the column in the other comparisons, where they are written,
   * as before SPARK-60983 (see [[replaceConstantsInPlace]]). The other equalities, `movable`, are
   * propagated and moved as since SPARK-60983, by [[replaceConstants]] and [[moveEqualitiesFirst]].
   *
   * Before SPARK-60983, the rule put the constants of all of the equalities into every
   * comparison, also those of the equalities whose constant a later iteration folds, e.g.
   * `CAST(7 AS BIGINT)`, unless it is NULL, and [[FoldablePropagation]], unless it is excluded,
   * puts the foldable columns of the child into the condition too. So the conjuncts that the
   * rewrite changes are also folded with all of them in (see [[foldChanged]]):
   *  - where one of them then becomes FALSE, or NULL where that means FALSE, the condition is
   *    FALSE, as before SPARK-60983, e.g. with `CASE WHEN 7 > 10 THEN 10 / x ELSE 0 END > 1` for
   *    `CASE WHEN id > 10 THEN 10 / x ELSE 0 END > 1 AND i = 1 AND id = 7`, as [[replaceConstants]]
   *    does not put `id = 7` into a `CASE WHEN` that reads another column;
   *  - otherwise, where one of them fails to fold with all of the constants in, outside of a
   *    conditional branch, which fails the query while it is optimized, the equalities are
   *    handled as since SPARK-60983, with no column taken as one that can fail (see
   *    [[FailableColumns.none]]), e.g. with `id > 10 / 0` for `i = 0 AND id > 10 / i`, or with
   *    `10 / (7 - 6 - 1)` for `10 / (id - 6 - i) > 0 AND i = 1 AND id = 7`;
   *  - where one of them fails to fold only as rewritten, as [[replaceConstants]] does not put
   *    the constant of `movable` into an operand that decides whether one that can fail runs, it
   *    is replaced with what it folds to with all of the constants in, as before SPARK-60983,
   *    e.g. with TRUE for `x > 0 AND (id > 0 OR 10 / i > 1) AND i = 0 AND id = 7`, or `x > 5` for
   *    `((id > 10 AND 10 / i > 1) OR x > 5) AND i = 0 AND id = 7`. That is its value on the rows
   *    where the equalities hold, and the AND is not TRUE on the others. If such a replacement
   *    can fail, e.g. `10 / x > 5` in place of `x > 5`, the equalities over the other columns,
   *    also those whose constant is not folded yet, but for one over a column that can fail, go
   *    first, so that it runs only on the rows where those hold.
   *
   * The equality over a column that can fail stays in the condition, so the rule checks such
   * conjuncts again each time it runs on the filter, e.g. for `id = 7 AND (id < 3 OR 10 / x > 1)
   * AND i = 1`, where [[replaceConstants]] keeps `id` in the operand before the division, it
   * folds `7 < 3 OR 10 / x > 1` to `10 / x > 1` each time, only to tell whether that is FALSE or
   * fails to fold.
   */
  private def propagateInPlace(
      condition: Expression,
      failable: AttributeMap[(Literal, BinaryComparison)],
      movable: AttributeMap[(Literal, BinaryComparison)],
      nullIsFalse: Boolean,
      columns: FailableColumns): Expression = {
    val equalityPredicates = AttributeMap(failable.toSeq ++ movable.toSeq)
    // `replaceConstants` goes first, on the condition as written, so that it reaches and folds
    // what it does for a column that can not fail. `inPlace` has only the constants of
    // `failable` in, which `moveEqualitiesFirst` compares the rewritten condition with; it is
    // needed only if `movable` is not empty.
    lazy val inPlace = replaceConstantsInPlace(
      replaceConstants(condition, AttributeMap.empty, failable), failable)
    val rewritten =
      replaceConstantsInPlace(replaceConstants(condition, movable, failable), failable)
    val unfolded = unfoldedEqualities(condition, equalityPredicates, nullIsFalse)
    val allEqualities = AttributeMap(equalityPredicates.toSeq ++ unfolded)
    val all = replaceConstantsInPlace(
      replaceConstants(condition, AttributeMap.empty, allEqualities), allEqualities)
    val folded = splitConjunctivePredicates(condition)
      .zip(splitConjunctivePredicates(rewritten))
      .zip(splitConjunctivePredicates(all))
      .map { case ((before, after), withAll) =>
        val afterFolded = foldChanged(before, after, nullIsFalse, columns)
        val withAllFolded = if (withAll.fastEquals(after)) {
          afterFolded
        } else {
          foldChanged(before, withAll, nullIsFalse, columns)
        }
        (after, afterFolded, withAllFolded)
      }
    if (folded.exists { case (_, a, w) => a == FoldsToFalse || w == FoldsToFalse }) {
      FalseLiteral
    } else if (folded.exists {
        case (_, _, FailsToFold) => true
        // A replacement with an untagged part that failed in a conditional branch fails once
        // `ConstantFolding` folds it in the plan, if another rule took the part out of the branch.
        case (_, FailsToFold, FoldsTo(_, untagged)) => untagged
        case _ => false
      }) {
      moveEqualitiesFirst(condition, replaceConstants(condition, equalityPredicates),
        equalityPredicates, FailableColumns.none)
    } else {
      // A conjunct that fails to fold as rewritten is replaced with what it folds to with all of
      // the constants in, which does not fail to fold.
      val replacements = folded.collect { case (_, FailsToFold, FoldsTo(e, _)) => e }
      val replaced = if (replacements.isEmpty) {
        rewritten
      } else {
        folded.map {
          case (_, FailsToFold, FoldsTo(e, _)) => e
          case (after, _, _) => after
        }.reduce(And)
      }
      if (replacements.forall(columns.canEvaluateUnconditionally)) {
        if (movable.isEmpty) replaced else moveEqualitiesFirst(inPlace, replaced, movable, columns)
      } else {
        // Such a replacement can fail on a row where an equality over a column that can not fail
        // does not hold, also one whose constant is not folded yet, so those go first. One over a
        // column that can fail stays, as its constant goes in place in the next iteration.
        val predicates = (movable.values.map(_._2) ++ unfolded.collect {
          case (a, (_, b)) if columns.canEvaluateUnconditionally(a) => b
        }).toSet[Expression]
        val (equalities, others) =
          splitConjunctivePredicates(replaced).partition(predicates.contains)
        (equalities ++ others).reduce(And)
      }
    }
  }

  /**
   * The equalities of the conjuncts of `condition` between an attribute and a foldable expression
   * that is not a literal yet, e.g. `id = CAST(7 AS BIGINT)` for a BIGINT `id`, with the literal
   * that it folds to, other than those over the attributes of `equalityPredicates`.
   * [[ConstantFolding]] makes such a one a literal for the next iteration. One whose constant is
   * NULL is left out: [[NullPropagation]] makes it NULL, or `IS NULL` for `<=>`, before it is
   * propagated.
   */
  private def unfoldedEqualities(
      condition: Expression,
      equalityPredicates: AttributeMap[(Literal, BinaryComparison)],
      nullIsFalse: Boolean): Seq[(Attribute, (Literal, BinaryComparison))] = {
    def equality(b: BinaryComparison, a: AttributeReference, e: Expression) =
      if (!e.isInstanceOf[Literal] && e.foldable && !equalityPredicates.contains(a)) {
        fold(e).filter { l =>
          // `safeToReplace` gets the comparison with the literal, as for the other equalities.
          val withLiteral = b.withNewChildren(b.children.map(c => if (c eq e) l else c))
          l.value != null &&
            safeToReplace(withLiteral.asInstanceOf[BinaryComparison], a, nullIsFalse)
        }.map(l => (a: Attribute) -> (l, b))
      } else {
        None
      }
    splitConjunctivePredicates(condition).flatMap {
      case b @ EqualTo(a: AttributeReference, e) => equality(b, a, e)
      case b @ EqualTo(e, a: AttributeReference) => equality(b, a, e)
      case b @ EqualNullSafe(a: AttributeReference, e) => equality(b, a, e)
      case b @ EqualNullSafe(e, a: AttributeReference) => equality(b, a, e)
      case _ => None
    }
  }

  /** What a conjunct folds to, see [[foldChanged]]. */
  private sealed trait Folded
  /** It is the conjunct as written. */
  private case object NotChecked extends Folded
  /** It is FALSE, or NULL where that means FALSE. */
  private case object FoldsToFalse extends Folded
  /** It fails to fold, outside of a conditional branch. */
  private case object FailsToFold extends Folded
  /**
   * It folds to `folded`, which can hold a part that failed in a conditional branch without its
   * tag if `untagged`, see [[simplify]].
   */
  private case class FoldsTo(folded: Expression, untagged: Boolean = false) extends Folded

  /**
   * What `after`, the conjunct `before` with constants in, folds to with the foldable columns of
   * the filter's child in, if either of them changed it. One that
   * [[ExprUtils.canEvaluateUnconditionally]] accepts is not folded, as it can not fail, and the
   * constants make it FALSE once they are in the condition, in this or a later iteration.
   */
  private def foldChanged(
      before: Expression,
      after: Expression,
      nullIsFalse: Boolean,
      columns: FailableColumns): Folded = {
    val withFoldables = columns.replaceFoldables(after)
    if (withFoldables.fastEquals(before)) {
      NotChecked
    } else if (ExprUtils.canEvaluateUnconditionally(withFoldables)) {
      FoldsTo(withFoldables)
    } else {
      simplify(withFoldables) match {
        case None => FailsToFold
        case Some((FalseLiteral, _)) => FoldsToFalse
        case Some((Literal(null, BooleanType), _)) if nullIsFalse => FoldsToFalse
        case Some((folded, untagged)) => FoldsTo(folded, untagged)
      }
    }
  }

  /**
   * Replaces the attributes of `equalityPredicates` with their constants in the comparisons of
   * `condition` other than those equalities, wherever they are, as before SPARK-60983. A
   * comparison that this rewrites no longer reads the column, so the conditions read it in no
   * more places than written, but a rewritten conjunct can fail on a row where the equality does
   * not hold, e.g. `10 / (id - 1)` for `10 / (id - i) > 0 AND i = 1` on a row where `id` is 1 and
   * `i` is 2, a conjunct after one that the constant folds away can run on such a row, e.g.
   * `10 / x > 0` for `i > 0 AND 10 / x > 0 AND i = 1`, and a rewritten conjunct can pass a row on
   * to another such equality that the written one stops, e.g. `j < 5` for
   * `j < 5 AND i = 2 AND j = 1`, as before SPARK-60983.
   *
   * As in [[replaceConstants]], an attribute whose type is not binary stable is replaced only in
   * a comparison with an attribute of the same type (see [[isSameCollationAttrRefComparison]]),
   * one outside of a comparison, e.g. in `i IN (1, 2)`, is kept, and one in the correlation of a
   * subquery in a comparison is replaced. Unlike before SPARK-60983, an attribute that
   * [[hasSignedZero]] marks is kept under an operator that can tell -0.0 from 0.0, which are
   * equal, i.e. one that [[keepsEqualValues]] rejects, e.g. in `CAST(x * y AS STRING)` for
   * `x = 0.0`, as [[replaceConstants]] keeps it there. [[replaceConstants]], run first, folds such
   * an operator where it reaches it and all of its attributes get a constant, as it does for a
   * column that can not fail, e.g. `CAST(x AS STRING)` (see its `foldOnly`).
   */
  private def replaceConstantsInPlace(
      condition: Expression,
      equalityPredicates: AttributeMap[(Literal, BinaryComparison)]): Expression = {
    val constantsMap = AttributeMap(equalityPredicates.map { case (attr, (lit, _)) => attr -> lit })
    val predicates = equalityPredicates.values.map(_._2).toSet
    def replaceInComparison(b: BinaryComparison): Expression = {
      val canReplace = replaceableIn(b, constantsMap)
      val signed = AttributeSet(b.references.filter(a => canReplace(a) &&
        hasSignedZero(constantsMap(a))))
      // Replaces the attributes, but keeps those of `signed` under an operator that can tell
      // -0.0 from 0.0.
      def replace(e: Expression, keepsEqual: Boolean): Expression = e match {
        case a: AttributeReference if canReplace(a) && (keepsEqual || !signed.contains(a)) =>
          constantsMap(a)
        case _ => e.mapChildren(replace(_, keepsEqual && keepsEqualValues(e)))
      }
      replace(b, keepsEqual = true)
    }
    condition.transformWithPruning(_.containsPattern(BINARY_COMPARISON)) {
      case b: BinaryComparison if !predicates.contains(b) => replaceInComparison(b)
    }
  }

  /**
   * Whether `l` may equal a value that an operator can tell from it: a zero of a FLOAT or a
   * DOUBLE, which equals the zero of the other sign, or any value of a type with a FLOAT or a
   * DOUBLE in it, as this does not look into the value.
   */
  private def hasSignedZero(l: Literal): Boolean = (l.value, l.dataType) match {
    case (null, _) => false
    case (d: Double, DoubleType) => d == 0.0
    case (f: Float, FloatType) => f == 0.0f
    case (_, dt) => dt.existsRecursively(t => t == FloatType || t == DoubleType)
  }

  /**
   * Whether `e` gives equal results for equal children, if it does not fail, so that a child can
   * be replaced with a value equal to it: an operator that [[ExprUtils.isTotalOperator]] accepts,
   * or binary arithmetic, which can fail in ANSI mode, but whose results, or failures, are
   * equal for -0.0 and 0.0, e.g. a division by either zero fails, although they may differ in the
   * sign of a zero, which only an operator above it that this rejects can tell apart.
   */
  private def keepsEqualValues(e: Expression): Boolean =
    ExprUtils.isTotalOperator(e) || e.isInstanceOf[BinaryArithmetic]

  /**
   * Simplifies `e` with [[ConstantFolding]] and the other rules of the operator optimization
   * batch that rewrite the condition of a filter (see [[SIMPLIFY_RULES_AFTER_FOLDING]]), in the
   * order of the batch, until it no longer changes, at most [[MAX_SIMPLIFY_ITERATIONS]] times,
   * or returns None if it fails to fold. Like [[ConstantFolding]], it does not fold again a part
   * that failed in a conditional branch, also once another rule rewrites it or takes it out of
   * the branch, or one that an earlier [[ConstantFolding]] tagged so. It tags the parts of a copy
   * of `e`, which may be shared with a plan, so that the other rules copy the tag to what they
   * rewrite a part into, as in the batch. If `e` can not be copied, it keeps the parts that failed
   * in a set of its own instead, which misses a part once another rule rewrites it, and leaves
   * such a part untagged in what it returns, which it then marks so. It runs only the rules that
   * `spark.sql.optimizer.excludedRules` does not exclude, and does not fold if that excludes
   * [[ConstantFolding]].
   */
  private def simplify(e: Expression): Option[(Expression, Boolean)] = {
    val excluded = excludedRules
    if (excluded(ConstantFolding.ruleName)) return Some((e, false))
    def included(rules: Seq[Rule[LogicalPlan]]) = rules.filterNot(r => excluded(r.ruleName))
    val before = included(SIMPLIFY_RULES_BEFORE_FOLDING)
    val after = included(SIMPLIFY_RULES_AFTER_FOLDING)
    val copy = copyWithTags(e)
    val failed = if (copy.isDefined) {
      null
    } else {
      java.util.Collections.newSetFromMap(
        new java.util.IdentityHashMap[Expression, java.lang.Boolean]())
    }
    def foldOnce(e: Expression): Expression = ConstantFolding.constantFolding(e, failed = failed)
    def rewrite(e: Expression, rules: Seq[Rule[LogicalPlan]]): Expression =
      rules.foldLeft(Filter(e, LocalRelation()): LogicalPlan)((plan, rule) => rule(plan))
        .asInstanceOf[Filter].condition
    def simplifyOnce(e: Expression): Expression = foldOnce(rewrite(e, after ++ before))
    try {
      var current = foldOnce(rewrite(copy.getOrElse(e), before))
      var next = simplifyOnce(current)
      var iterations = 1
      while (!next.fastEquals(current) && iterations < MAX_SIMPLIFY_ITERATIONS) {
        current = next
        next = simplifyOnce(current)
        iterations += 1
      }
      Some((next, failed != null && !failed.isEmpty))
    } catch {
      case NonFatal(_) => None
    }
  }

  /**
   * The rules that `spark.sql.optimizer.excludedRules` excludes. Unlike `Optimizer.batches`, this
   * does not leave out the non-excludable rules, none of which this rule checks.
   */
  private def excludedRules: Set[String] =
    conf.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq).toSet

  /** The most times that [[simplify]] simplifies an expression. */
  private val MAX_SIMPLIFY_ITERATIONS = 10

  /**
   * The rules of the operator optimization batch that rewrite the condition of a filter, other
   * than [[ConstantFolding]], which [[simplify]] runs, in the order of the batch: those between
   * this rule and [[ConstantFolding]], and those after [[ConstantFolding]], up to this rule in the
   * next iteration. `ReplaceNullWithFalseInPredicate` is left out, as NULL means FALSE only where
   * the caller says so, and so are those that need the plan below the filter, such as
   * `RewriteSizeOfArrayStruct`, or only replace a subquery, such as `RewriteNonCorrelatedExists`,
   * and the extension rules.
   */
  private val SIMPLIFY_RULES_BEFORE_FOLDING: Seq[Rule[LogicalPlan]] = Seq(OptimizeIn, OptimizeRand)
  private val SIMPLIFY_RULES_AFTER_FOLDING: Seq[Rule[LogicalPlan]] = Seq(
    ReorderAssociativeOperator,
    LikeSimplification,
    BooleanSimplification,
    SimplifyConditionals,
    PushFoldableIntoBranches,
    SimplifyBinaryComparison,
    SimplifyCasts,
    SimplifyCaseConversionExpressions,
    SimplifyDateTimeConversions,
    UnwrapCastInBinaryComparison,
    OptimizeUpdateFields,
    SimplifyExtractValueOps,
    OptimizeCsvJsonExprs,
    CombineConcats,
    NullPropagation,
    NullDownPropagation)

  /**
   * A copy of `e`, with the tags of its parts, as `clone` makes it, or None if a part
   * can not be copied, or its copy is the part itself, e.g. for an object. A part that `e` holds
   * in more than one place gets a copy for each.
   */
  private def copyWithTags(e: Expression): Option[Expression] = {
    def fresh(from: Expression, to: Expression): Boolean = !(to eq from) &&
      from.children.zip(to.children).forall { case (f, t) => fresh(f, t) }
    try Some(e.clone()).filter(fresh(e, _)) catch { case NonFatal(_) => None }
  }

  /**
   * Whether an attribute in `b` can be replaced with its constant of `constantsMap`. Binary-stable
   * `AttributeReference`s can always be replaced; others only as
   * [[isSameCollationAttrRefComparison]] allows.
   */
  private def replaceableIn(
      b: BinaryComparison,
      constantsMap: AttributeMap[Literal]): Attribute => Boolean = {
    lazy val collationSafeReplacement = isSameCollationAttrRefComparison(b)
    a => constantsMap.contains(a) && (isBinaryStable(a.dataType) || collationSafeReplacement)
  }

  /**
   * Replaces attributes in the comparisons of `condition` with their constant values.
   *
   * The constant is the value of an attribute only on the rows that pass the whole condition.
   * The rewritten condition also runs on other rows, e.g. a row with `y = 1` for
   * `10 / (id - y) > 0 AND y = 0`, and a part of it that becomes constant is folded with no row
   * at all, e.g. `10 / y` for `y = 0 AND id > 10 / y`. So that this raises no error the original
   * condition cannot raise, an expression that can fail keeps its attributes, unless all of them
   * are replaced and the result evaluates without an error. For the same reason, an operand of an
   * AND, OR, COALESCE or IN in a conjunct is rewritten only if no operand after it can fail, as it
   * decides whether they run, e.g. `y < 3` in `(y < 3 OR 10 / x > 1) AND y = 5`. The conjuncts of
   * `condition` itself are rewritten regardless, and [[moveEqualitiesFirst]] orders them. A column
   * of the projection below that can fail (see [[FailableColumns]]) counts as one that can not
   * fail here, and [[moveEqualitiesFirst]] can then move the equalities ahead of a conjunct over
   * it. The constants of `foldOnly`, which [[replaceConstantsInPlace]] puts in elsewhere
   * afterwards, go only into such a whole expression that folds.
   */
  private def replaceConstants(
      condition: Expression,
      equalityPredicates: AttributeMap[(Literal, BinaryComparison)],
      foldOnly: AttributeMap[(Literal, BinaryComparison)] = AttributeMap.empty): Expression = {
    def constants(equalities: AttributeMap[(Literal, BinaryComparison)]) =
      AttributeMap(equalities.map { case (attr, (lit, _)) => attr -> lit })
    val constantsMap = constants(equalityPredicates)
    val foldConstants = constants(AttributeMap(foldOnly.toSeq ++ equalityPredicates.toSeq))
    val predicates = (equalityPredicates.values ++ foldOnly.values).map(_._2).toSet
    def replaceInComparison(b: BinaryComparison): Expression = {
      val canReplace = replaceableIn(b, constantsMap)
      val canFold = replaceableIn(b, foldConstants)
      def replace(e: Expression): Expression = e match {
        case a: AttributeReference if canReplace(a) => constantsMap(a)
        case _: And | _: Or | _: Coalesce | _: In => replaceOperands(e, replace)
        case _ if ExprUtils.isTotalOperator(e) => e.mapChildren(replace)
        case _ if e.references.nonEmpty && e.references.forall(canFold) =>
          fold(e.transform { case a: AttributeReference if canFold(a) => foldConstants(a) })
            .getOrElse(e)
        case _ => e
      }
      b.mapChildren(replace)
    }
    def replaceInCondition(e: Expression): Expression = e match {
      case b: BinaryComparison if predicates.contains(b) => b
      case b: BinaryComparison => replaceInComparison(b)
      case _: And | _: Or | _: Coalesce | _: In => replaceOperands(e, replaceInCondition)
      case _ if ExprUtils.isTotalOperator(e) => e.mapChildren(replaceInCondition)
      // The children of these include the comparisons of a pulled up correlated condition.
      case _: InSubquery | _: ListQuery | _: Exists => e.mapChildren(replaceInCondition)
      case _ => e
    }
    def replaceInConjuncts(e: Expression): Expression = e match {
      case a: And => a.mapChildren(replaceInConjuncts)
      case _ => replaceInCondition(e)
    }
    replaceInConjuncts(condition)
  }

  /**
   * Rewrites the operands of `e`, which short-circuits, with `f`. An operand decides whether the
   * ones after it run, so it is rewritten only if none of those, as rewritten, can fail.
   */
  private def replaceOperands(e: Expression, f: Expression => Expression): Expression = {
    val (newChildren, _) = e.children.foldRight((List.empty[Expression], false)) {
      case (child, (laterChildren, laterCanFail)) =>
        // TODO(SPARK-61861): An operand that its constant would make `e` end is kept, so the filter
        // keeps the operands after it, e.g. `id > 0` in
        // `(id > 0 OR (CAST(s AS INT) >= 1 AND CAST(s AS INT) <= 100)) AND id = 7`, where the
        // subexpression elimination of `FilterExec` computes the cast ahead of the OR.
        val newChild = if (laterCanFail) child else f(child)
        (newChild :: laterChildren,
          laterCanFail || !ExprUtils.canEvaluateUnconditionally(newChild))
    }
    e.withNewChildren(newChildren)
  }

  /** Returns the literal that `e` folds to, or None if it is not constant or fails to fold. */
  private def fold(e: Expression): Option[Literal] = {
    if (e.foldable) {
      try {
        Some(ConstantFolding.constantFolding(e)).collect { case l: Literal => l }
      } catch {
        case NonFatal(_) => None
      }
    } else {
      None
    }
  }

  /**
   * Moves `equalityPredicates` ahead of the other conjuncts of `condition`, the rewritten
   * `original`, if one that can fail still refers to a propagated attribute. Such a conjunct also
   * runs on rows where an equality does not hold, with the value of the row, while a guard over
   * the same attribute may be folded away, e.g. `n / d > 1` for `d <> 0 AND n / d > 1 AND d = 5`.
   * The same holds for a conjunct that can fail after one that is rewritten, as that one decides
   * whether it runs, e.g. `10 / x > 0` for `y > 0 AND 10 / x > 0 AND y = 1`. An equality between
   * an attribute and a literal can fail only as the attribute does, so the caller passes only
   * equalities over columns that `columns` takes as ones that can not fail.
   *
   * A conjunct over a column of the projection below that `columns` takes as one that can fail
   * can fail too, e.g. `i > 0`, with `i` the alias `CAST(s AS INT)`, in ANSI mode. The first
   * rewritten conjunct counts too, not only the ones after it, if it reads such a column, as
   * [[replaceConstants]] rewrites its operands as if the column could not fail, e.g. `id < 3` in
   * `(id < 3 OR i > 0) AND id = 7`. `keepsFailable` checks the attributes a conjunct reads
   * against those of `equalityPredicates`, not those that the expressions of its columns read,
   * which these equalities do not replace.
   */
  private def moveEqualitiesFirst(
      original: Expression,
      condition: Expression,
      equalityPredicates: AttributeMap[(Literal, BinaryComparison)],
      columns: FailableColumns): Expression = {
    val predicates = equalityPredicates.values.map(_._2).toSet[Expression]
    val propagated = AttributeSet(equalityPredicates.keys)
    val conjuncts = splitConjunctivePredicates(condition)
    val (equalities, others) = conjuncts.partition(predicates.contains)
    val reordered = equalities ++ others
    val keepsFailable = others.exists { c =>
      c.references.intersect(propagated).nonEmpty && !columns.canEvaluateUnconditionally(c)
    }
    val firstRewritten = splitConjunctivePredicates(original).zip(conjuncts)
      .indexWhere { case (before, after) => !before.fastEquals(after) }
    val unmasksFailable = firstRewritten >= 0 && (columns.reads(conjuncts(firstRewritten)) ||
      conjuncts.drop(firstRewritten + 1).exists(!columns.canEvaluateUnconditionally(_)))
    if ((keepsFailable || unmasksFailable) && reordered != conjuncts) {
      // TODO(SPARK-61438): An equality that is NULL on a row does not stop it from reaching the
      // conjuncts after it, e.g. those that its constant rewrote, in an AND under an OR.
      reordered.reduce(And)
    } else {
      condition
    }
  }

  /**
   * What one application of the rule finds for each plan below a filter, as the filters above it
   * look at it too: the columns that can fail, and the foldable ones, each only once asked for.
   */
  private class ChildColumns(plan: LogicalPlan) {
    val failable = new java.util.IdentityHashMap[LogicalPlan, AttributeSet]()
    val foldable = new java.util.IdentityHashMap[LogicalPlan, AttributeMap[Alias]]()
    /** The definitions of the CTEs that are not inlined, by their ids. */
    lazy val cteDefs: Map[Long, CTERelationDef] = plan.collectWithSubqueries {
      case w: WithCTE => w.cteDefs
    }.flatten.map(d => d.id -> d).toMap
  }

  // Not `computeIfAbsent`: `compute` adds the values of the plans below to the same map, which
  // the mapping function of `Map.computeIfAbsent` should not do.
  private def cached[T](cache: java.util.IdentityHashMap[LogicalPlan, T], plan: LogicalPlan)(
      compute: => T): T = {
    val value = cache.get(plan)
    if (value != null) {
      value
    } else {
      val computed = compute
      cache.put(plan, computed)
      computed
    }
  }

  /**
   * The columns of `plan` that can fail, i.e. those that a projection at its top, or below the
   * filters, samples, limits, offsets, sorts, joins, lateral joins, unions, CTEs that are not
   * inlined and projections there, or, for a reference to a CTE there, in its definition, computes
   * with a deterministic expression that [[ExprUtils.canEvaluateUnconditionally]] rejects or that
   * reads such a column, or passes such a column through, the result columns of an aggregation
   * with grouping keys there whose expression, with each aggregate function taken as its value, is
   * deterministic and rejected by it, and those that a serializer from objects there computes with
   * such an expression.
   * Under whole-stage codegen, the projection computes a deterministic column for a row only when
   * a conjunct of the filter above first reads it, unless its subexpression elimination computes
   * it up front, and `PushPredicateThroughNonJoin` pushes a conjunct below the projection with the
   * expression of the column substituted. So a conjunct over such a column can fail as that
   * expression does, and so can one over such a result column, which an aggregation with grouping
   * keys computes the same way. A non-deterministic column is computed on every row the projection
   * gets, and no filter is pushed through a projection with one, so it does not count; nor does a
   * result column of a global aggregation, which computes it for its one row up front.
   * A limit, an offset, a union in whole-stage codegen and the streamed side of a broadcast hash or
   * nested loop join pass the columns on as they are, in the same stage, and so does a sort that
   * the physical plan removes as its input is sorted already, and a `WithCTE`, which
   * `ReplaceCTERefWithRepartition` removes. A lateral join counts as the join that
   * `RewriteLateralSubquery` turns it into after this rule runs. A reference to a CTE reads the
   * columns its definition computes, where `PushdownPredicatesAndPruneColumnsForCTEDef` copies the
   * conditions over it to, as their OR, which `BooleanSimplification` can reorder when there are
   * several. Otherwise, e.g. below the shuffle of a limit or an offset over more than one
   * partition, on the build side of a join, below a sort or a union outside whole-stage codegen, or
   * for a reference to a CTE where no condition is copied, the columns are computed for every row
   * anyway, but they still count:
   * the constant of an equality over them then goes into the other conjuncts in place, as before
   * SPARK-60983, which can fail on a row where the equality does not hold, where moving the
   * equality first would not. Other operators are not looked through for the columns that can
   * fail.
   */
  private def failableColumnsOf(plan: LogicalPlan, cache: ChildColumns): AttributeSet =
    cached(cache.failable, plan) {
      plan match {
        case Project(projectList, child) =>
          val below = failableColumnsOf(child, cache)
          AttributeSet(projectList.collect {
            case a: Alias if a.child.deterministic &&
                (!ExprUtils.canEvaluateUnconditionally(a.child) ||
                  a.child.references.exists(below.contains)) => a.toAttribute
            case a: Attribute if below.contains(a) => a
          })
        case Filter(_, child) => failableColumnsOf(child, cache)
        case s: Sample => failableColumnsOf(s.child, cache)
        case l: GlobalLimit => failableColumnsOf(l.child, cache)
        case l: LocalLimit => failableColumnsOf(l.child, cache)
        case o: Offset => failableColumnsOf(o.child, cache)
        case s: Sort => failableColumnsOf(s.child, cache)
        case j: Join => failableColumnsOf(j.left, cache) ++ failableColumnsOf(j.right, cache)
        case l: LateralJoin =>
          failableColumnsOf(l.left, cache) ++ failableColumnsOf(l.right.plan, cache)
        case w: WithCTE => failableColumnsOf(w.plan, cache)
        case r: CTERelationRef if !r.recursive =>
          cache.cteDefs.get(r.cteId).map { d =>
            val failable = failableColumnsOf(d.child, cache)
            AttributeSet(r.output.zip(d.output).collect {
              case (a, b) if failable.contains(b) => a
            })
          }.getOrElse(AttributeSet.empty)
        case s: SerializeFromObject =>
          AttributeSet(s.serializer.collect {
            case a: Alias if a.child.deterministic &&
                !ExprUtils.canEvaluateUnconditionally(a.child) => a.toAttribute
          })
        case u: Union =>
          val children = u.children.map(c => (c.output, failableColumnsOf(c, cache)))
          AttributeSet(u.output.zipWithIndex.collect {
            case (a, n) if children.exists { case (output, failable) =>
              failable.contains(output(n))
            } => a
          })
        case agg: Aggregate if agg.groupingExpressions.nonEmpty =>
          AttributeSet(agg.aggregateExpressions.flatMap {
            case a: Alias =>
              val result = a.child.transform { case e: AggregateExpression => e.resultAttribute }
              Some(a.toAttribute).filter(_ =>
                result.deterministic && !ExprUtils.canEvaluateUnconditionally(result))
            case _ => None
          })
        case _ => AttributeSet.empty
      }
    }

  /**
   * The foldable columns of `plan`, with their aliases, as [[FoldablePropagation]] finds them and
   * replaces them in the conditions above, also those that it finds only in a later application.
   */
  private def foldableColumnsOf(plan: LogicalPlan, cache: ChildColumns): AttributeMap[Alias] =
    cached(cache.foldable, plan) {
      // The plan with the foldable columns replaced is not used.
      FoldablePropagation.propagateFoldablesFromChildren(plan,
        plan.children.map(foldableColumnsOf(_, cache)), withoutAliases = true).map(_._2)
        .getOrElse(AttributeMap.empty)
    }

  /** The columns of the child of a filter that can fail, and the foldable ones. */
  private class FailableColumns(
      failableColumns: => AttributeSet,
      foldableColumns: => AttributeMap[Alias]) {
    private lazy val failable: AttributeSet = failableColumns
    private lazy val foldable: AttributeMap[Alias] = foldableColumns

    /**
     * [[ExprUtils.canEvaluateUnconditionally]] of `e`, which must also not read a column that can
     * fail. This gives the result of checking `e` with the expressions of the deterministic
     * columns substituted, and the non-deterministic ones kept as attributes, as the check
     * accepts an expression only if it accepts each of its children, by their types, which a
     * column shares with its expression. It does not build that expression, which doubles with
     * every projection that reads a column twice.
     */
    def canEvaluateUnconditionally(e: Expression): Boolean =
      ExprUtils.canEvaluateUnconditionally(e) && !reads(e)

    /** Whether `e` reads a column that can fail. */
    def reads(e: Expression): Boolean = failable.nonEmpty && e.references.exists(failable.contains)

    /** `e` with the foldable columns replaced with their expressions. */
    def replaceFoldables(e: Expression): Expression =
      ConstantPropagation.replaceFoldables(e, foldable)
  }

  private def replaceFoldables(e: Expression, foldable: AttributeMap[Alias]): Expression =
    if (foldable.isEmpty) {
      e
    } else {
      e.transformUp { case a: AttributeReference if foldable.contains(a) => foldable(a).child }
    }

  private object FailableColumns {
    /** No column counts as one that can fail, as before SPARK-61437. */
    val none = new FailableColumns(AttributeSet.empty, AttributeMap.empty)
  }

  /**
   * Binary-stable `AttributeReference`s can always be replaced safely. Non-binary-stable
   * `AttributeReference`s (i.e., those with a non-`UTF8_BINARY` `StringType`) are only replaced
   * when both sides of the comparison are `AttributeReference`s (or `CollationKey`-wrapped
   * `AttributeReference`s) with the same `StringType`, preventing substitution inside
   * expressions that change the effective collation (e.g., a `Cast`). For example, given a
   * column `c STRING COLLATE UTF8_LCASE`:
   *
   *   `c = 'hello' AND c = 'HELLO' COLLATE UNICODE`
   *
   * `c` is added to `constantsMap`. In the right-hand comparison, `c` is wrapped with a
   * `Cast` to `UNICODE`, so we don't have an `AttributeReference` vs. `AttributeReference`
   * comparison and `c` is not replaced inside the `Cast`, preserving correctness.
   */
  private def isSameCollationAttrRefComparison(b: BinaryComparison): Boolean = {
    (b.left, b.right) match {
      case (AttributeReference(_, st1: StringType, _, _),
      AttributeReference(_, st2: StringType, _, _)) =>
        st1 == st2
      case (CollationKey(AttributeReference(_, st1: StringType, _, _)),
      CollationKey(AttributeReference(_, st2: StringType, _, _))) =>
        st1 == st2
      case _ => false
    }
  }
}

/**
 * Reorder associative integral-type operators and fold all constants into one.
 */
object ReorderAssociativeOperator extends Rule[LogicalPlan] {
  private def flattenAdd(
    expression: Expression,
    groupSet: ExpressionSet): Seq[Expression] = expression match {
    case expr @ Add(l, r, _) if !groupSet.contains(expr) =>
      flattenAdd(l, groupSet) ++ flattenAdd(r, groupSet)
    case other => other :: Nil
  }

  private def flattenMultiply(
    expression: Expression,
    groupSet: ExpressionSet): Seq[Expression] = expression match {
    case expr @ Multiply(l, r, _) if !groupSet.contains(expr) =>
      flattenMultiply(l, groupSet) ++ flattenMultiply(r, groupSet)
    case other => other :: Nil
  }

  private def collectGroupingExpressions(plan: LogicalPlan): ExpressionSet = plan match {
    case Aggregate(groupingExpressions, aggregateExpressions, child, _) =>
      ExpressionSet.apply(groupingExpressions)
    case _ => ExpressionSet(Seq.empty)
  }

  private def isSameInteger(expr: Expression, value: Int): Boolean = expr match {
    case l: Literal => l.value == value
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(BINARY_ARITHMETIC), ruleId) {
    case q: LogicalPlan =>
      // We have to respect aggregate expressions which exists in grouping expressions when plan
      // is an Aggregate operator, otherwise the optimized expression could not be derived from
      // grouping expressions.
      // TODO: do not reorder consecutive `Add`s or `Multiply`s with different `failOnError` flags
      val groupingExpressionSet = collectGroupingExpressions(q)
      q.transformExpressionsDownWithPruning(_.containsPattern(BINARY_ARITHMETIC)) {
      case a @ Add(_, _, f) if a.deterministic && a.dataType.isInstanceOf[IntegralType] =>
        val (literals, others) = flattenAdd(a, groupingExpressionSet)
          .partition(_.isInstanceOf[Literal])
        if (literals.nonEmpty) {
          val literalExpr = literals.reduce((x, y) => Add(x, y, f))
          if (others.isEmpty) {
            literalExpr
          } else if (isSameInteger(literalExpr, 0)) {
            others.reduce((x, y) => Add(x, y, f))
          } else {
            Add(others.reduce((x, y) => Add(x, y, f)), literalExpr, f)
          }
        } else {
          a
        }
      case m @ Multiply(_, _, f) if m.deterministic && m.dataType.isInstanceOf[IntegralType] =>
        val (literals, others) = flattenMultiply(m, groupingExpressionSet)
          .partition(_.isInstanceOf[Literal])
        if (literals.nonEmpty) {
          val literalExpr = literals.reduce((x, y) => Multiply(x, y, f))
          if (others.isEmpty || (isSameInteger(literalExpr, 0) && !m.nullable)) {
            literalExpr
          } else if (isSameInteger(literalExpr, 1)) {
            others.reduce((x, y) => Multiply(x, y, f))
          } else {
            Multiply(others.reduce((x, y) => Multiply(x, y, f)), literalExpr, f)
          }
        } else {
          m
        }
    }
  }
}


/**
 * Optimize IN predicates:
 * 1. Converts the predicate to false when the list is empty and
 *    the value is not nullable.
 * 2. Removes literal repetitions.
 * 3. Replaces [[In (value, seq[Literal])]] with optimized version
 *    [[InSet (value, HashSet[Literal])]] which is much faster.
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(IN), ruleId) {
    case q: LogicalPlan => q.transformExpressionsDownWithPruning(_.containsPattern(IN), ruleId) {
      case In(v, list) if list.isEmpty =>
        // IN (empty list) is always false under current behavior.
        // Under legacy behavior it's null if the left side is null, otherwise false (SPARK-44550).
        if (!SQLConf.get.legacyNullInEmptyBehavior) {
          FalseLiteral
        } else {
          If(IsNotNull(v), FalseLiteral, Literal(null, BooleanType))
        }
      case expr @ In(v, list) if expr.inSetConvertible =>
        val newList = ExpressionSet(list).toSeq
        if (newList.length == 1
          // TODO: `EqualTo` for structural types are not working. Until SPARK-24443 is addressed,
          // TODO: we exclude them in this rule.
          && !v.isInstanceOf[CreateNamedStruct]
          && !newList.head.isInstanceOf[CreateNamedStruct]) {
          EqualTo(v, newList.head)
        } else if (newList.length > conf.optimizerInSetConversionThreshold) {
          val hSet = newList.map(e => e.eval(EmptyRow))
          InSet(v, HashSet() ++ hSet)
        } else if (newList.length < list.length) {
          expr.copy(list = newList)
        } else { // newList.length == list.length && newList.length > 1
          expr
        }
    }
  }
}


/**
 * Simplifies boolean expressions:
 * 1. Simplifies expressions whose answer can be determined without evaluating both sides.
 * 2. Eliminates / extracts common factors.
 * 3. Merge same expressions
 * 4. Removes `Not` operator.
 * 5. Simplifies contained integral ranges.
 */
object BooleanSimplification extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(AND, OR, NOT), ruleId) {
    case q: LogicalPlan => q.transformExpressionsUpWithPruning(
      _.containsAnyPattern(AND, OR, NOT), ruleId) {
      actualExprTransformer
    }
  }

  val actualExprTransformer: PartialFunction[Expression, Expression] = {
    case TrueLiteral And e => e
    case e And TrueLiteral => e
    case FalseLiteral Or e => e
    case e Or FalseLiteral => e

    case FalseLiteral And _ => FalseLiteral
    case _ And FalseLiteral => FalseLiteral
    case TrueLiteral Or _ => TrueLiteral
    case _ Or TrueLiteral => TrueLiteral

    case a And b if Not(a).semanticEquals(b) =>
      If(IsNull(a), Literal.create(null, a.dataType), FalseLiteral)
    case a And b if a.semanticEquals(Not(b)) =>
      If(IsNull(b), Literal.create(null, b.dataType), FalseLiteral)

    case a Or b if Not(a).semanticEquals(b) =>
      If(IsNull(a), Literal.create(null, a.dataType), TrueLiteral)
    case a Or b if a.semanticEquals(Not(b)) =>
      If(IsNull(b), Literal.create(null, b.dataType), TrueLiteral)

    case a And b if a.semanticEquals(b) => a
    case a Or b if a.semanticEquals(b) => a

    case a And b if integralRangeImplies(a, b) => a
    case a And b if integralRangeImplies(b, a) => b
    case a Or b if integralRangeImplies(a, b) => b
    case a Or b if integralRangeImplies(b, a) => a

    // The following optimizations are applicable only when the operands are not nullable,
    // since the three-value logic of AND and OR are different in NULL handling.
    // See the chart:
    // +---------+---------+---------+---------+
    // | operand | operand |   OR    |   AND   |
    // +---------+---------+---------+---------+
    // | TRUE    | TRUE    | TRUE    | TRUE    |
    // | TRUE    | FALSE   | TRUE    | FALSE   |
    // | FALSE   | FALSE   | FALSE   | FALSE   |
    // | UNKNOWN | TRUE    | TRUE    | UNKNOWN |
    // | UNKNOWN | FALSE   | UNKNOWN | FALSE   |
    // | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
    // +---------+---------+---------+---------+

    // (NULL And (NULL Or FALSE)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
    case a And (b Or c) if !a.nullable && Not(a).semanticEquals(b) => And(a, c)
    // (NULL And (FALSE Or NULL)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
    case a And (b Or c) if !a.nullable && Not(a).semanticEquals(c) => And(a, b)
    // ((NULL Or FALSE) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
    // And(b, c) evaluates b also on the rows where a is true. If b can fail, Not(a) is evaluated
    // first. It is used instead of c, which can have the operands of a in another order.
    case (a Or b) And c if !c.nullable && a.semanticEquals(Not(c)) =>
      if (ExprUtils.canEvaluateUnconditionally(b)) And(b, c) else And(Not(a), b)
    // ((FALSE Or NULL) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
    case (a Or b) And c if !c.nullable && b.semanticEquals(Not(c)) => And(a, c)

    // (NULL Or (NULL And TRUE)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
    case a Or (b And c) if !a.nullable && Not(a).semanticEquals(b) => Or(a, c)
    // (NULL Or (TRUE And NULL)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
    case a Or (b And c) if !a.nullable && Not(a).semanticEquals(c) => Or(a, b)
    // ((NULL And TRUE) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
    // Or(b, c) evaluates b also on the rows where a is false. If b can fail, Not(a) is evaluated
    // first. It is used instead of c, which can have the operands of a in another order.
    case (a And b) Or c if !c.nullable && a.semanticEquals(Not(c)) =>
      if (ExprUtils.canEvaluateUnconditionally(b)) Or(b, c) else Or(Not(a), b)
    // ((TRUE And NULL) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
    case (a And b) Or c if !c.nullable && b.semanticEquals(Not(c)) => Or(a, c)

    // Common factor elimination for conjunction
    case and @ (left And right) =>
      // 1. Split left and right to get the disjunctive predicates,
      //    i.e. lhs = (a || b), rhs = (a || c)
      // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a). A throwable
      //    one is left out where the formula evaluates it on more rows, see `commonFactors`.
      // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
      // 4. If common is non-empty, apply the formula to get the optimized predicate:
      //    common || (ldiff && rdiff)
      // 5. Else, split left and right to get the conjunctive predicates.
      //    for example lhs = (a && b), rhs = (a && c) => all = (a, b, a, c), distinct = (a, b, c)
      //    optimized predicate: (a && b && c)
      val lhs = splitDisjunctivePredicates(left)
      val rhs = splitDisjunctivePredicates(right)
      val common = commonFactors(lhs, rhs)
      if (common.nonEmpty) {
        val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
        val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
        if (ldiff.isEmpty || rdiff.isEmpty) {
          // (a || b || c || ...) && (a || b) => (a || b)
          common.reduce(Or)
        } else {
          // (a || b || c || ...) && (a || b || d || ...) =>
          // a || b || ((c || ...) && (d || ...))
          (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
        }
      } else {
        // No common factor to extract from disjunctive predicates, reduce one from conjunction
        val all = splitConjunctivePredicates(left) ++ splitConjunctivePredicates(right)
        val distinct = ExpressionSet(all)
        if (all.size == distinct.size) {
          // No duplicates, return the original predicate
          and
        } else {
          // (a && b) && a && (a && c) => a && b && c
          buildBalancedPredicate(distinct.toSeq, And)
        }
      }

    // Common factor elimination for disjunction
    case or @ (left Or right) =>
      // 1. Split left and right to get the conjunctive predicates,
      //    i.e.  lhs = (a && b), rhs = (a && c)
      // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a). A throwable
      //    one is left out where the formula evaluates it on more rows, see `commonFactors`.
      // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
      // 4. If common is non-empty, apply the formula to get the optimized predicate:
      //    common && (ldiff || rdiff)
      // 5. Else, split left and right to get the conjunctive predicates.
      // for example lhs = (a || b), rhs = (a || c) => all = (a, b, a, c), distinct = (a, b, c)
      // optimized predicate: (a || b || c)
      val lhs = splitConjunctivePredicates(left)
      val rhs = splitConjunctivePredicates(right)
      val common = commonFactors(lhs, rhs)
      if (common.nonEmpty) {
        val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
        val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
        if (ldiff.isEmpty || rdiff.isEmpty) {
          // (a && b) || (a && b && c && ...) => a && b
          common.reduce(And)
        } else {
          // (a && b && c && ...) || (a && b && d && ...) =>
          // a && b && ((c && ...) || (d && ...))
          (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
        }
      } else {
        // No common factor to extract from conjunctive predicates, reduce one from disjunction
        val all = splitDisjunctivePredicates(left) ++ splitDisjunctivePredicates(right)
        val distinct = ExpressionSet(all)
        if (all.size == distinct.size) {
          // No duplicates, return the original predicate
          or
        } else {
          // (a || b) || a || (a || c) => a || b || c
          buildBalancedPredicate(distinct.toSeq, Or)
        }
      }

    case not: Not => simplifyNot(not)
  }

  /**
   * Returns the operands of `lhs` that are also in `rhs` and are to be evaluated ahead of the other
   * operands, in their order in `lhs`. A throwable one is returned only if it is the same in `lhs`
   * and `rhs`, and if, in `lhs` or in `rhs`, only the ones returned before it come before it.
   * Otherwise, evaluating it ahead would also run it on rows where an operand before it decides the
   * result of both `lhs` and `rhs`, or would run its own operands in an order that
   * `semanticEquals` ignores. As in PushPredicateThroughJoin, one that can fail without being
   * throwable, such as a division in ANSI mode, is returned anyway: it can be a join key, which a
   * hash join evaluates on every row.
   */
  private def commonFactors(lhs: Seq[Expression], rhs: Seq[Expression]): Seq[Expression] = {
    lhs.filter(e => rhs.exists(e.semanticEquals)).foldLeft(Seq.empty[Expression]) { (common, e) =>
      def onlyCommonBefore(operands: Seq[Expression]): Boolean =
        operands.takeWhile(!_.semanticEquals(e)).forall(o => common.exists(o.semanticEquals))
      if (!e.throwable ||
          (rhs.contains(e) && (onlyCommonBefore(lhs) || onlyCommonBefore(rhs)))) {
        common :+ e
      } else {
        common
      }
    }
  }

  /**
   * Simplify Not expressions. This method is extracted to allow recursive calls for
   * Not(a Or b) and Not(a And b) cases, which avoids calling the entire actualExprTransformer
   * and only focuses on Not simplification.
   */
  private def simplifyNot(not: Not): Expression = not match {
    case Not(TrueLiteral) => FalseLiteral
    case Not(FalseLiteral) => TrueLiteral

    case Not(a GreaterThan b) => LessThanOrEqual(a, b)
    case Not(a GreaterThanOrEqual b) => LessThan(a, b)

    case Not(a LessThan b) => GreaterThanOrEqual(a, b)
    case Not(a LessThanOrEqual b) => GreaterThan(a, b)

    // De Morgan's Laws can create new Not expressions that need further simplification.
    case Not(a Or b) =>
      And(simplifyNot(Not(a)), simplifyNot(Not(b)))
    case Not(a And b) =>
      Or(simplifyNot(Not(a)), simplifyNot(Not(b)))

    case Not(Not(e)) => e

    case Not(IsNull(e)) => IsNotNull(e)
    case Not(IsNotNull(e)) => IsNull(e)

    case _ => not
  }

  private case class IntegralRange(
      attribute: AttributeReference,
      bound: Long,
      isLowerBound: Boolean,
      inclusive: Boolean)

  private object IntegralComparison {
    def unapply(expression: Expression): Option[IntegralRange] = {
      def range(
          attribute: AttributeReference,
          literal: Literal,
          isLowerBound: Boolean,
          inclusive: Boolean): Option[IntegralRange] = {
        if (attribute.dataType.isInstanceOf[IntegralType] &&
            literal.dataType == attribute.dataType && literal.value != null) {
          Some(IntegralRange(attribute, literal.value.asInstanceOf[Number].longValue(),
            isLowerBound, inclusive))
        } else {
          None
        }
      }

      // ConstantFolding precedes BooleanSimplification in the operator optimization batches,
      // so foldable bounds such as 2 + 2 can be matched as literals.
      expression match {
        case GreaterThan(a: AttributeReference, l: Literal) => range(a, l, true, false)
        case GreaterThanOrEqual(a: AttributeReference, l: Literal) => range(a, l, true, true)
        case LessThan(a: AttributeReference, l: Literal) => range(a, l, false, false)
        case LessThanOrEqual(a: AttributeReference, l: Literal) => range(a, l, false, true)
        case GreaterThan(l: Literal, a: AttributeReference) => range(a, l, false, false)
        case GreaterThanOrEqual(l: Literal, a: AttributeReference) => range(a, l, false, true)
        case LessThan(l: Literal, a: AttributeReference) => range(a, l, true, false)
        case LessThanOrEqual(l: Literal, a: AttributeReference) => range(a, l, true, true)
        case _ => None
      }
    }
  }

  // For comparisons on the same attribute, containment also preserves UNKNOWN: both
  // comparisons return NULL together. Keep arbitrary expressions out of these rules so
  // removing a comparison cannot suppress an exception or a nondeterministic evaluation.
  private def integralRangeImplies(left: Expression, right: Expression): Boolean = {
    (left, right) match {
      case (IntegralComparison(l), IntegralComparison(r))
          if l.attribute.semanticEquals(r.attribute) && l.isLowerBound == r.isLowerBound =>
        val order = java.lang.Long.compare(l.bound, r.bound)
        if (order == 0) {
          !l.inclusive || r.inclusive
        } else if (l.isLowerBound) {
          order > 0
        } else {
          order < 0
        }

      case (And(l @ IntegralComparison(_), r @ IntegralComparison(_)),
          target @ IntegralComparison(_)) =>
        integralRangeImplies(l, target) || integralRangeImplies(r, target)

      case (source @ IntegralComparison(_),
          Or(l @ IntegralComparison(_), r @ IntegralComparison(_))) =>
        integralRangeImplies(source, l) || integralRangeImplies(source, r)

      case _ => false
    }
  }
}


/**
 * Simplifies binary comparisons with semantically-equal expressions:
 * 1) Replace '<=>' with 'true' literal.
 * 2) Replace '=', '<=', and '>=' with 'true' literal if both operands are non-nullable.
 * 3) Replace '<' and '>' with 'false' literal if both operands are non-nullable.
 * 4) Unwrap '=', '<=>' if one side is a boolean literal
 */
object SimplifyBinaryComparison
  extends Rule[LogicalPlan] with PredicateHelper with ConstraintHelper {

  private def canSimplifyComparison(
      left: Expression,
      right: Expression,
      notNullExpressions: => ExpressionSet): Boolean = {
    if (left.semanticEquals(right)) {
      (!left.nullable && !right.nullable) || notNullExpressions.contains(left)
    } else {
      false
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(BINARY_COMPARISON), ruleId) {
    case l: LogicalPlan =>
      lazy val notNullExpressions = ExpressionSet(l match {
        case Filter(fc, _) =>
          splitConjunctivePredicates(fc).collect {
            case i: IsNotNull => i.child
          }
        case _ => Seq.empty
      })

      l.transformExpressionsUpWithPruning(_.containsPattern(BINARY_COMPARISON)) {
        // True with equality
        case a EqualNullSafe b if a.semanticEquals(b) => TrueLiteral
        case a EqualTo b if canSimplifyComparison(a, b, notNullExpressions) => TrueLiteral
        case a GreaterThanOrEqual b if canSimplifyComparison(a, b, notNullExpressions) =>
          TrueLiteral
        case a LessThanOrEqual b if canSimplifyComparison(a, b, notNullExpressions) => TrueLiteral

        // False with inequality
        case a GreaterThan b if canSimplifyComparison(a, b, notNullExpressions) => FalseLiteral
        case a LessThan b if canSimplifyComparison(a, b, notNullExpressions) => FalseLiteral

        // Optimize equalities when one side is Literal in order to help pushing down the filters
        case a EqualTo TrueLiteral => a
        case TrueLiteral EqualTo b => b
        case a EqualTo FalseLiteral => Not(a)
        case FalseLiteral EqualTo b => Not(b)
        case a EqualNullSafe TrueLiteral if !a.nullable => a
        case TrueLiteral EqualNullSafe b if !b.nullable => b
        case a EqualNullSafe FalseLiteral if !a.nullable => Not(a)
        case FalseLiteral EqualNullSafe b if !b.nullable => Not(b)
      }
  }
}


/**
 * Simplifies conditional expressions (if / case).
 */
object SimplifyConditionals extends Rule[LogicalPlan] {
  private def falseOrNullLiteral(e: Expression): Boolean = e match {
    case FalseLiteral => true
    case Literal(null, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(IF, CASE_WHEN, EVAL_ONLY_WHEN), ruleId) {
    case q: LogicalPlan => q transformExpressionsUp {
      case If(TrueLiteral, trueValue, _) => trueValue
      case If(FalseLiteral, _, falseValue) => falseValue
      case If(Literal(null, _), _, falseValue) => falseValue
      case If(cond, TrueLiteral, FalseLiteral) =>
        if (cond.nullable) EqualNullSafe(cond, TrueLiteral) else cond
      case If(cond, FalseLiteral, TrueLiteral) =>
        if (cond.nullable) Not(EqualNullSafe(cond, TrueLiteral)) else Not(cond)
      case If(cond, trueValue, falseValue)
        if cond.deterministic && trueValue.semanticEquals(falseValue) => trueValue
      // Drop a UDF evaluation marker (see `EvalOnlyWhen`) once the optimizer has removed every
      // Python and external UDF from its child, so the plan matches the one without the marker.
      // The marker's condition equals this branch's condition, so `If(c, EvalOnlyWhen(c, e), f)`
      // is `If(c, e, f)`.
      case If(cond, EvalOnlyWhen(markerCond, child), falseValue)
        if cond.deterministic && cond.semanticEquals(markerCond) &&
          !EvalOnlyWhen.hasLiftedUDF(child) =>
        If(cond, child, falseValue)
      // A marker whose condition folded to a constant: it is `child` when always true, and null
      // otherwise (the branch is never taken).
      case EvalOnlyWhen(TrueLiteral, child) => child
      case EvalOnlyWhen(FalseLiteral | Literal(null, _), child) =>
        Literal.create(null, child.dataType)
      case If(cond, l @ Literal(null, _), FalseLiteral) if !cond.nullable => And(cond, l)
      case If(cond, l @ Literal(null, _), TrueLiteral) if !cond.nullable => Or(Not(cond), l)
      case If(cond, FalseLiteral, l @ Literal(null, _)) if !cond.nullable => And(Not(cond), l)
      case If(cond, TrueLiteral, l @ Literal(null, _)) if !cond.nullable => Or(cond, l)

      case CaseWhen(Seq((cond, TrueLiteral)), Some(FalseLiteral)) =>
        if (cond.nullable) EqualNullSafe(cond, TrueLiteral) else cond
      case CaseWhen(Seq((cond, FalseLiteral)), Some(TrueLiteral)) =>
        if (cond.nullable) Not(EqualNullSafe(cond, TrueLiteral)) else Not(cond)

      case e @ CaseWhen(branches, elseValue) if branches.exists(x => falseOrNullLiteral(x._1)) =>
        // If there are branches that are always false, remove them.
        // If there are no more branches left, just use the else value.
        // Note that these two are handled together here in a single case statement because
        // otherwise we cannot determine the data type for the elseValue if it is None (i.e. null).
        val newBranches = branches.filter(x => !falseOrNullLiteral(x._1))
        if (newBranches.isEmpty) {
          elseValue.getOrElse(Literal.create(null, e.dataType))
        } else {
          e.copy(branches = newBranches)
        }

      case CaseWhen(branches, _) if branches.headOption.map(_._1).contains(TrueLiteral) =>
        // If the first branch is a true literal, remove the entire CaseWhen and use the value
        // from that. Note that CaseWhen.branches should never be empty, and as a result the
        // headOption (rather than head) added above is just an extra (and unnecessary) safeguard.
        branches.head._2

      case CaseWhen(branches, _) if branches.exists(_._1 == TrueLiteral) =>
        // a branch with a true condition eliminates all following branches,
        // these branches can be pruned away
        val (h, t) = branches.span(_._1 != TrueLiteral)
        CaseWhen( h :+ t.head, None)

      case e @ CaseWhen(branches, elseOpt)
          if branches.forall(_._2.semanticEquals(elseOpt.getOrElse(Literal(null, e.dataType)))) =>
        val elseValue = elseOpt.getOrElse(Literal(null, e.dataType))
        // For non-deterministic conditions with side effect, we can not remove it, or change
        // the ordering. As a result, we try to remove the deterministic conditions from the tail.
        var hitNonDeterministicCond = false
        var i = branches.length
        while (i > 0 && !hitNonDeterministicCond) {
          hitNonDeterministicCond = !branches(i - 1)._1.deterministic
          if (!hitNonDeterministicCond) {
            i -= 1
          }
        }
        if (i == 0) {
          elseValue
        } else {
          e.copy(
            branches = branches.take(i).map(branch => (branch._1, elseValue)),
            elseValue = elseOpt.filterNot(_.semanticEquals(Literal(null, e.dataType))))
        }

      case e @ CaseWhen(_, elseOpt)
          if elseOpt.exists(_.semanticEquals(Literal(null, e.dataType))) =>
        e.copy(elseValue = None)
    }
  }
}


/**
 * Push the foldable expression into (if / case) branches.
 */
object PushFoldableIntoBranches extends Rule[LogicalPlan] {

  // To be conservative here: it's only a guaranteed win if all but at most only one branch
  // end up being not foldable.
  private def atMostOneUnfoldable(exprs: Seq[Expression]): Boolean = {
    exprs.count(!_.foldable) < 2
  }

  // Not all UnaryExpression can be pushed into (if / case) branches, e.g. Alias.
  private def supportedUnaryExpression(e: UnaryExpression): Boolean = e match {
    case _: IsNull | _: IsNotNull => true
    case _: UnaryMathExpression | _: Abs | _: Bin | _: Factorial | _: Hex => true
    case _: String2StringExpression | _: Ascii | _: Base64 | _: BitLength | _: Chr | _: Length =>
      true
    case _: Cast => true
    case _: TryEval => true
    case _: GetDateField | _: LastDay => true
    case _: ExtractIntervalPart[_] => true
    case _: ArraySetLike => true
    case _: ExtractValue => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CASE_WHEN, IF), ruleId) {
    case q: LogicalPlan => q.transformExpressionsUpWithPruning(
      _.containsAnyPattern(CASE_WHEN, IF), ruleId) {
      case u @ UnaryExpression(i @ If(_, trueValue, falseValue))
          if supportedUnaryExpression(u) && atMostOneUnfoldable(Seq(trueValue, falseValue)) =>
        i.copy(
          trueValue = u.withNewChildren(Array(trueValue).toImmutableArraySeq),
          falseValue = u.withNewChildren(Array(falseValue).toImmutableArraySeq))

      case u @ UnaryExpression(c @ CaseWhen(branches, elseValue))
          if supportedUnaryExpression(u) && atMostOneUnfoldable(branches.map(_._2) ++ elseValue) =>
        c.copy(
          branches.map(e => e.copy(_2 = u.withNewChildren(Array(e._2).toImmutableArraySeq))),
          Some(u.withNewChildren(
            Array(elseValue.getOrElse(Literal(null, c.dataType))).toImmutableArraySeq)))

      case SupportedBinaryExpr(b, i @ If(_, trueValue, falseValue), right)
          if right.foldable && atMostOneUnfoldable(Seq(trueValue, falseValue)) =>
        i.copy(
          trueValue = b.withNewChildren(Array(trueValue, right).toImmutableArraySeq),
          falseValue = b.withNewChildren(Array(falseValue, right).toImmutableArraySeq))

      case SupportedBinaryExpr(b, left, i @ If(_, trueValue, falseValue))
          if left.foldable && atMostOneUnfoldable(Seq(trueValue, falseValue)) =>
        i.copy(
          trueValue = b.withNewChildren(Array(left, trueValue).toImmutableArraySeq),
          falseValue = b.withNewChildren(Array(left, falseValue).toImmutableArraySeq))

      case SupportedBinaryExpr(b, c @ CaseWhen(branches, elseValue), right)
          if right.foldable && atMostOneUnfoldable(branches.map(_._2) ++ elseValue) =>
        c.copy(
          branches.map(e => e.copy(_2 = b.withNewChildren(Array(e._2, right).toImmutableArraySeq))),
          Some(b.withNewChildren(
            Array(elseValue.getOrElse(Literal(null, c.dataType)), right).toImmutableArraySeq)))

      case SupportedBinaryExpr(b, left, c @ CaseWhen(branches, elseValue))
          if left.foldable && atMostOneUnfoldable(branches.map(_._2) ++ elseValue) =>
        c.copy(
          branches.map(e => e.copy(_2 = b.withNewChildren(Array(left, e._2).toImmutableArraySeq))),
          Some(b.withNewChildren(
            Array(left, elseValue.getOrElse(Literal(null, c.dataType))).toImmutableArraySeq)))
    }
  }
}

object SupportedBinaryExpr {
  def unapply(expr: Expression): Option[(Expression, Expression, Expression)] = expr match {
    case _: BinaryComparison | _: StringPredicate | _: StringRegexExpression =>
      Some(expr, expr.children.head, expr.children.last)
    case _: BinaryArithmetic => Some(expr, expr.children.head, expr.children.last)
    case _: BinaryMathExpression => Some(expr, expr.children.head, expr.children.last)
    case _: AddMonths | _: DateAdd | _: DateAddInterval | _: DateDiff | _: DateSub |
         _: DateAddYMInterval | _: TimestampAddYMInterval | _: TimestampAddInterval =>
      Some(expr, expr.children.head, expr.children.last)
    case _: FindInSet | _: RoundBase => Some(expr, expr.children.head, expr.children.last)
    case BinaryPredicate(expr) =>
      Some(expr, expr.arguments.head, expr.arguments.last)
    case _ => None
  }
}

/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] with PredicateHelper {
  // These regexes classify a pattern that has no escape character (`%`/`_` are the only
  // wildcards). Patterns that do contain the escape character are decoded by `decodeLikePattern`
  // first. Consecutive wildcard characters are equivalent to a single wildcard character.
  private val startsWith = "([^_%]+)%+".r
  private val endsWith = "%+([^_%]+)".r
  private val startsAndEndsWith = "([^_%]+)%+([^_%]+)".r
  private val contains = "%+([^_%]+)%+".r
  private val equalTo = "([^_%]*)".r

  private def startsWithExpr(input: Expression, prefix: String): Expression =
    StartsWith(input, Literal.create(prefix, input.dataType))

  private def endsWithExpr(input: Expression, postfix: String): Expression =
    EndsWith(input, Literal.create(postfix, input.dataType))

  private def containsExpr(input: Expression, infix: String): Expression =
    Contains(input, Literal.create(infix, input.dataType))

  private def equalToExpr(input: Expression, str: String): Expression =
    EqualTo(input, Literal.create(str, input.dataType))

  // 'a%a' is basically 'a%' && '%a', but a length guard is required to stop 'a' matching 'a%a'.
  // When the collation matches raw bytes (supportsBinaryEquality), StartsWith/EndsWith pin the
  // literal bytes of the prefix and suffix, so a byte-length guard (OctetLength, O(1) via the
  // stored numBytes) accepts exactly the same inputs as the code-point guard and is cheaper.
  // Otherwise the anchors are collation-aware (LIKE reaches this only for UTF8_LCASE) and can
  // match a code point whose UTF-8 length differs from the pattern's -- a single multibyte code
  // point could then satisfy both anchors and clear the byte guard -- so the code-point (Length)
  // guard must be kept for correctness.
  // The replacement reads the input three times, so it is made only for a deterministic input;
  // a nondeterministic one could give each read a different value.
  private def startsAndEndsWithExpr(
      input: Expression, prefix: String, postfix: String): Expression = {
    val lengthGuard = input.dataType match {
      case st: StringType if st.supportsBinaryEquality =>
        GreaterThanOrEqual(OctetLength(input),
          Literal.create(UTF8String.fromString(prefix).numBytes
            + UTF8String.fromString(postfix).numBytes))
      case _ =>
        GreaterThanOrEqual(Length(input),
          Literal.create(prefix.codePointCount(0, prefix.length)
            + postfix.codePointCount(0, postfix.length)))
    }
    And(lengthGuard, And(startsWithExpr(input, prefix), endsWithExpr(input, postfix)))
  }

  // A decoded LIKE pattern token: a maximal run of literal characters, or a `%` wildcard
  // (consecutive `%` collapse into one).
  private sealed trait LikeToken
  private case class LikeLiteral(str: String) extends LikeToken
  private case object LikeWildcard extends LikeToken

  // Decodes a `pattern` that contains the escape character into literal/wildcard tokens, or None
  // if it cannot be simplified: an invalid escape (the escape char not followed by `%`, `_`, or
  // the escape char) or a trailing escape char -- both make LIKE throw at runtime, so the rule
  // must leave them alone -- or an unescaped `_` wildcard, which this rule does not handle.
  private def decodeLikePattern(pattern: String, escapeChar: Char): Option[Seq[LikeToken]] = {
    val tokens = ArrayBuffer.empty[LikeToken]
    val literal = new StringBuilder
    def flushLiteral(): Unit = if (literal.length > 0) {
      tokens += LikeLiteral(literal.toString)
      literal.setLength(0)
    }
    var i = 0
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == escapeChar) {
        if (i + 1 >= pattern.length) {
          return None // trailing escape character
        }
        val next = pattern.charAt(i + 1)
        if (next == '%' || next == '_' || next == escapeChar) {
          literal.append(next)
          i += 2
        } else {
          return None // invalid escape sequence
        }
      } else if (c == '%') {
        flushLiteral()
        if (tokens.isEmpty || tokens.last != LikeWildcard) {
          tokens += LikeWildcard
        }
        i += 1
      } else if (c == '_') {
        return None // unescaped single-character wildcard, not handled here
      } else {
        literal.append(c)
        i += 1
      }
    }
    flushLiteral()
    Some(tokens.toSeq)
  }

  private def simplifyLike(
      input: Expression, pattern: String, escapeChar: Char = '\\'): Option[Expression] = {
    if (!pattern.contains(escapeChar)) {
      // Fast path: no escape character, so `%`/`_` are the only wildcards.
      pattern match {
        case startsWith(prefix) => Some(startsWithExpr(input, prefix))
        case endsWith(postfix) => Some(endsWithExpr(input, postfix))
        case startsAndEndsWith(prefix, postfix) if input.deterministic =>
          Some(startsAndEndsWithExpr(input, prefix, postfix))
        case contains(infix) => Some(containsExpr(input, infix))
        case equalTo(str) => Some(equalToExpr(input, str))
        case _ => None
      }
    } else if (escapeChar == '%' || escapeChar == '_') {
      // Pathological: the escape character is itself a wildcard character, so `%`/`_` no longer
      // act as wildcards and the shape detection does not apply. Skip, as before.
      None
    } else {
      // The pattern uses escape sequences: decode them, then classify the decoded shape. The
      // decoded literals are exact, so the resulting predicate accepts the same rows as the LIKE.
      decodeLikePattern(pattern, escapeChar).flatMap {
        case Seq(LikeLiteral(str)) => Some(equalToExpr(input, str))
        case Seq(LikeLiteral(prefix), LikeWildcard) => Some(startsWithExpr(input, prefix))
        case Seq(LikeWildcard, LikeLiteral(postfix)) => Some(endsWithExpr(input, postfix))
        case Seq(LikeLiteral(prefix), LikeWildcard, LikeLiteral(postfix)) if input.deterministic =>
          Some(startsAndEndsWithExpr(input, prefix, postfix))
        case Seq(LikeWildcard, LikeLiteral(infix), LikeWildcard) =>
          Some(containsExpr(input, infix))
        case _ => None
      }
    }
  }

  private def simplifyMultiLike(
      child: Expression, patterns: Seq[UTF8String], multi: MultiLikeBase): Expression = {
    val (remainPatternMap, replacementMap) =
      patterns.map { p =>
        p -> Option(p).flatMap(p => simplifyLike(child, p.toString))
      }.partition(_._2.isEmpty)
    val remainPatterns = remainPatternMap.map(_._1)
    val replacements = replacementMap.map(_._2.get)
    if (replacements.isEmpty) {
      multi
    } else {
      multi match {
        case l: LikeAll =>
          val and = buildBalancedPredicate(replacements, And)
          if (remainPatterns.nonEmpty) And(and, l.copy(patterns = remainPatterns)) else and
        case l: NotLikeAll =>
          val and = buildBalancedPredicate(replacements.map(Not(_)), And)
          if (remainPatterns.nonEmpty) And(and, l.copy(patterns = remainPatterns)) else and
        case l: LikeAny =>
          val or = buildBalancedPredicate(replacements, Or)
          if (remainPatterns.nonEmpty) Or(or, l.copy(patterns = remainPatterns)) else or
        case l: NotLikeAny =>
          val or = buildBalancedPredicate(replacements.map(Not(_)), Or)
          if (remainPatterns.nonEmpty) Or(or, l.copy(patterns = remainPatterns)) else or
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(LIKE_FAMLIY), ruleId) {
    case l @ Like(input, Literal(pattern, _: StringType), escapeChar) =>
      if (pattern == null) {
        // If pattern is null, return null value directly, since "col like null" == null.
        Literal(null, BooleanType)
      } else {
        simplifyLike(input, pattern.toString, escapeChar).getOrElse(l)
      }
    case l @ LikeAll(child, patterns) if CollapseProject.isCheap(child) =>
      simplifyMultiLike(child, patterns, l)
    case l @ NotLikeAll(child, patterns) if CollapseProject.isCheap(child) =>
      simplifyMultiLike(child, patterns, l)
    case l @ LikeAny(child, patterns) if CollapseProject.isCheap(child) =>
      simplifyMultiLike(child, patterns, l)
    case l @ NotLikeAny(child, patterns) if CollapseProject.isCheap(child) =>
      simplifyMultiLike(child, patterns, l)
  }
}


/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  private def isNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    t => t.containsAnyPattern(NULL_CHECK, NULL_LITERAL, COUNT, COALESCE)
      || t.containsAllPatterns(WINDOW_EXPRESSION, CAST, LITERAL), ruleId) {
    case q: LogicalPlan => q.transformExpressionsUpWithPruning(
      t => t.containsAnyPattern(NULL_CHECK, NULL_LITERAL, COUNT, COALESCE)
        || t.containsAllPatterns(WINDOW_EXPRESSION, CAST, LITERAL), ruleId) {
      case e @ WindowExpression(Cast(Literal(0L, _), _, _, _), _) =>
        Cast(Literal(0L), e.dataType, Option(conf.sessionLocalTimeZone))
      case e @ AggregateExpression(Count(exprs), _, _, _, _) if exprs.forall(isNullLiteral) =>
        Cast(Literal(0L), e.dataType, Option(conf.sessionLocalTimeZone))
      case ae @ AggregateExpression(Count(exprs), _, false, _, _) if !exprs.exists(_.nullable) =>
        // This rule should be only triggered when isDistinct field is false.
        ae.copy(aggregateFunction = Count(Literal(1)))

      case IsNull(c) if !c.nullable => Literal.create(false, BooleanType)
      case IsNotNull(c) if !c.nullable => Literal.create(true, BooleanType)

      case EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case EqualNullSafe(l, Literal(null, _)) => IsNull(l)

      case AssertNotNull(c, _) if !c.nullable => c

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filterNot(isNullLiteral)
        if (newChildren.isEmpty) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          val nonNullableIndex = newChildren.indexWhere(e => !e.nullable)
          if (nonNullableIndex > -1) {
            Coalesce(newChildren.take(nonNullableIndex + 1))
          } else {
            Coalesce(newChildren)
          }
        }

      // If the list is empty, transform the In expression to false literal.
      case In(_, list)
        if list.isEmpty && !SQLConf.get.legacyNullInEmptyBehavior =>
        Literal.create(false, BooleanType)
      // If the value expression is NULL (and the list is non-empty), then transform the
      // In expression to null literal.
      // If the legacy flag is set, then it becomes null even if the list is empty (which is
      // incorrect legacy behavior)
      case In(Literal(null, _), list)
        if list.nonEmpty || SQLConf.get.legacyNullInEmptyBehavior
      => Literal.create(null, BooleanType)
      case InSubquery(Seq(Literal(null, _)), _)
        if SQLConf.get.legacyNullInEmptyBehavior =>
        Literal.create(null, BooleanType)
      case InSubquery(Seq(Literal(null, _)), ListQuery(sub, _, _, _, conditions, _))
        if !SQLConf.get.legacyNullInEmptyBehavior
        && conditions.isEmpty =>
        If(Exists(sub), Literal(null, BooleanType), FalseLiteral)

      // Non-leaf NullIntolerant expressions will return null, if at least one of its children is
      // a null literal.
      case e if e.nullIntolerant && e.children.exists(isNullLiteral) =>
        Literal.create(null, e.dataType)
    }
  }
}


/**
 * Unwrap the input of IsNull/IsNotNull if the input is NullIntolerant
 * E.g. IsNull(Not(null)) == IsNull(null)
 */
object NullDownPropagation extends Rule[LogicalPlan] {
  // Return true iff the expression returns non-null result for all non-null inputs.
  // Not all `NullIntolerant` can be propagated. E.g. `Cast` is `NullIntolerant`; however,
  // cast('Infinity' as integer) is null. Hence, `Cast` is not supported `NullIntolerant`.
  // `ExtractValue` is also not supported. E.g. the planner may resolve column `a` to `a#123`,
  // then IsNull(a#123) cannot be optimized.
  // Applying to `EqualTo` is too disruptive for [SPARK-32290] optimization, not supported for now.
  // If e has multiple children, the deterministic check is required because optimizing
  // IsNull(a > b) to Or(IsNull(a), IsNull(b)), for example, may cause skipping the evaluation of b
  private def supportedNullIntolerant(e: Expression): Boolean = (e match {
    case _: Not => true
    case _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual
      if e.deterministic => true
    case _ => false
  }) && e.children.nonEmpty

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(NULL_CHECK), ruleId) {
    case q: LogicalPlan => q.transformExpressionsDownWithPruning(
      _.containsPattern(NULL_CHECK), ruleId) {
      case IsNull(e) if e.nullIntolerant && supportedNullIntolerant(e) =>
        e.children.map(IsNull(_): Expression).reduceLeft(Or)
      case IsNotNull(e) if e.nullIntolerant && supportedNullIntolerant(e) =>
        e.children.map(IsNotNull(_): Expression).reduceLeft(And)
    }
  }
}


/**
 * Replace attributes with aliases of the original foldable expressions if possible.
 * Other optimizations will take advantage of the propagated foldable expressions. For example,
 * this rule can optimize
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY x, y, 3
 * }}}
 * to
 * {{{
 *   SELECT 1.0 x, 'abc' y, Now() z ORDER BY 1.0, 'abc', Now()
 * }}}
 * and other rules can further optimize it and remove the ORDER BY operator.
 */
object FoldablePropagation extends Rule[LogicalPlan] with AliasHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    CleanupAliases(propagateFoldables(plan)._1)
  }

  private def propagateFoldables(plan: LogicalPlan): (LogicalPlan, AttributeMap[Alias]) = {
    val (newChildren, foldableMaps) = plan.children.map(propagateFoldables).unzip
    val newPlan = plan.withNewChildren(newChildren)
    propagateFoldablesFromChildren(newPlan, foldableMaps, withoutAliases = false)
      .getOrElse((newPlan, AttributeMap.empty))
  }

  /**
   * Replaces in `plan` the foldable columns of its children, given in `childMaps` in the order of
   * `plan.children`, and returns it with the foldable columns that it passes on, or None if it
   * does not propagate them, in which case `childMaps` is not evaluated.
   *
   * `ConstantPropagation` also uses this to find the foldable columns of the child of a filter,
   * with `withoutAliases`. Then a column is foldable also if its expression is foldable once the
   * aliases below its top one are removed, as `CleanupAliases` does, e.g. one that renames a
   * foldable column or is computed from one, which this rule finds only in its next application,
   * and its alias has those aliases removed.
   */
  private[optimizer] def propagateFoldablesFromChildren(
      plan: LogicalPlan,
      childMaps: => Seq[AttributeMap[Alias]],
      withoutAliases: Boolean): Option[(LogicalPlan, AttributeMap[Alias])] = {
    plan match {
      case p: Project =>
        val newProject = replaceFoldable(p, childMaps.head)
        Some((newProject, collectFoldables(newProject.projectList, withoutAliases)))

      // FoldablePropagation rule can produce incorrect optimized plan for streaming queries.
      // This is because the optimizer can replace the grouping expressions, or join column
      // with a literal value if the grouping key is constant for the micro-batch. However,
      // as Streaming queries also read from the StateStore, this optimization also
      // overwrites any keys read from State Store. We need to disable this optimization
      // until we can make optimizer aware of Streaming state store. The State Store nodes
      // are currently added in the Physical plan.
      case a: Aggregate if !a.isStreaming =>
        val newAggregate = replaceFoldable(a, childMaps.head)
        Some((newAggregate, collectFoldables(newAggregate.aggregateExpressions, withoutAliases)))

      // We can not replace the attributes in `Expand.output`. If there are other non-leaf
      // operators that have the `output` field, we should put them here too.
      case e: Expand =>
        val foldableMap = childMaps.head
        val newExpand = if (foldableMap.isEmpty) {
          e
        } else {
          val newProjections = e.projections.map(_.map(_.transform {
            case a: AttributeReference if foldableMap.contains(a) => foldableMap(a)
          }))
          if (newProjections == e.projections) {
            e
          } else {
            e.copy(projections = newProjections)
          }
        }
        Some((newExpand, foldableMap))

      case u: UnaryNode if canPropagateFoldables(u) =>
        val foldableMap = childMaps.head
        Some((replaceFoldable(u, foldableMap), foldableMap))

      // Join derives the output attributes from its child while they are actually not the
      // same attributes. For example, the output of outer join is not always picked from its
      // children, but can also be null. We should exclude these miss-derived attributes when
      // propagating the foldable expressions.
      // TODO(cloud-fan): It seems more reasonable to use new attributes as the output attributes
      // of outer join.
      // FoldablePropagation rule can produce incorrect optimized plan for streaming queries.
      // This is because the optimizer can replace the grouping expressions, or join column
      // with a literal value if the grouping key is constant for the micro-batch. However,
      // as Streaming queries also read from the StateStore, this optimization also
      // overwrites any keys read from State Store. We need to disable this optimization
      // until we can make optimizer aware of Streaming state store. The State Store nodes
      // are currently added in the Physical plan.
      case j: Join if !j.left.isStreaming || !j.right.isStreaming =>
        val foldableMap = AttributeMap(
          childMaps.foldLeft(Iterable.empty[(Attribute, Alias)])(_ ++ _.baseMap.values))
        val newJoin = replaceFoldable(j, foldableMap)
        val missDerivedAttrsSet: AttributeSet = AttributeSet(newJoin.joinType match {
          case _: InnerLike | LeftExistence(_) => Nil
          case LeftOuter | LeftSingle => newJoin.right.output
          case RightOuter => newJoin.left.output
          case FullOuter => newJoin.left.output ++ newJoin.right.output
          case _ => Nil
        })
        val newFoldableMap = AttributeMap(foldableMap.baseMap.values.filterNot {
          case (attr, _) => missDerivedAttrsSet.contains(attr)
        })
        Some((newJoin, newFoldableMap))

      // For other plans, they are not safe to apply foldable propagation, and they should not
      // propagate foldable expressions from children.
      case _ => None
    }
  }

  private def replaceFoldable(plan: LogicalPlan, foldableMap: AttributeMap[Alias]): plan.type = {
    if (foldableMap.isEmpty) {
      plan
    } else {
      plan transformExpressions {
        case a: AttributeReference if foldableMap.contains(a) => foldableMap(a).withName(a.name)
      }
    }
  }

  private def collectFoldables(expressions: Seq[NamedExpression], withoutAliases: Boolean) = {
    AttributeMap(expressions.collect {
      case a: Alias if a.child.foldable => (a.toAttribute, a)
      case a: Alias if withoutAliases && trimAliases(a.child).foldable =>
        (a.toAttribute, trimNonTopLevelAliases(a))
    })
  }

  /**
   * List of all [[UnaryNode]]s which allow foldable propagation.
   */
  private def canPropagateFoldables(u: UnaryNode): Boolean = u match {
    // Handling `Project` is moved to `propagateFoldablesFromChildren`.
    case _: Filter => true
    case _: SubqueryAlias => true
    // Handling `Aggregate` is moved to `propagateFoldablesFromChildren`.
    case _: Window => true
    case _: Sample => true
    case _: GlobalLimit => true
    case _: LocalLimit => true
    case _: Offset => true
    case _: Generate => true
    case _: Distinct => true
    case _: AppendColumns => true
    case _: AppendColumnsWithObject => true
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: RebalancePartitions => true
    case _: Sort => true
    case _: TypedFilter => true
    case _ => false
  }
}


/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(CAST), ruleId) {
    // Annotated STRING (flag off) is not unconstrained STRING. Dropping CAST(... AS STRING)
    // would hide the type change. First-class CHAR/VARCHAR already fail e.dataType == StringType.
    case c @ Cast(e: NamedExpression, StringType, _, _)
      if e.dataType == StringType && e.metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY) => c
    case Cast(e, dataType, _, _) if e.dataType == dataType => e
    case c @ Cast(Cast(e, dt1: NumericType, _, _), dt2: NumericType, _, _)
        if isWiderCast(e.dataType, dt1) && isWiderCast(dt1, dt2) =>
      c.copy(child = e)
    case c @ Cast(e, dataType, _, _) => (e.dataType, dataType) match {
      case (ArrayType(from, false), ArrayType(to, true)) if from == to => e
      case (MapType(fromKey, fromValue, false), MapType(toKey, toValue, true))
        if fromKey == toKey && fromValue == toValue => e
      case (from: StructType, to: StructType)
        if from.asNullable == to.asNullable &&
          DataType.equalsIgnoreCompatibleNullability(from, to) => e
      case _ => c
      }
    case IsNotNull(Cast(e, dataType: NumericType, _, _)) if isWiderCast(e.dataType, dataType) =>
      IsNotNull(e)
  }

  // Returns whether the from DataType can be safely casted to the to DataType without losing
  // any precision or range.
  private def isWiderCast(from: DataType, to: NumericType): Boolean =
    from.isInstanceOf[NumericType] && Cast.canUpCast(from, to)
}


/**
 * Removes redundant same-case conversion expressions (e.g. UPPER(UPPER(x)) or LOWER(LOWER(x)))
 * that are unnecessary because the case conversion operation is idempotent.
 *
 * Note: Cross-case conversions (UPPER(LOWER(x)) or LOWER(UPPER(x))) are NOT simplified
 * because they are not semantics-preserving for some Unicode characters
 * (e.g. U+0131 LATIN SMALL LETTER DOTLESS I, where LOWER(UPPER(U+0131)) yields 'i'
 * while LOWER(U+0131) leaves the character unchanged).
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(UPPER_OR_LOWER), ruleId) {
    case q: LogicalPlan => q.transformExpressionsUpWithPruning(
      _.containsPattern(UPPER_OR_LOWER), ruleId) {
      case Upper(Upper(child)) => Upper(child)
      case Lower(Lower(child)) => Lower(child)
    }
  }
}

/**
 * Removes date and time related functions that are unnecessary.
 */
object SimplifyDateTimeConversions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
      _.containsPattern(DATETIME), ruleId) {
    case q: LogicalPlan => q.transformExpressionsUpWithPruning(
        _.containsPattern(DATETIME), ruleId) {
      // Remove a string to timestamp conversions followed by a timestamp to string conversions if
      // original string is in the same format.
      case DateFormatClass(
          GetTimestamp(
            e @ DateFormatClass(child, pattern, timeZoneId),
            pattern2,
            TimestampType,
            _,
            timeZoneId2,
            _),
          pattern3,
          timeZoneId3)
          if pattern.semanticEquals(pattern2) && pattern.semanticEquals(pattern3)
            && timeZoneId == timeZoneId2 && timeZoneId == timeZoneId3
            && !child.dataType.isInstanceOf[TimeType]
            && !child.dataType.isInstanceOf[AnyTimestampNanoType] =>
        e

      // Remove a timestamp to string conversion followed by a string to timestamp conversions if
      // original timestamp is built with the same format.
      case GetTimestamp(
          DateFormatClass(
            e @ GetTimestamp(_, pattern, TimestampType, _, timeZoneId, _),
            pattern2,
            timeZoneId2),
          pattern3,
          TimestampType,
          _,
          timeZoneId3,
          _)
          if pattern.semanticEquals(pattern2) && pattern.semanticEquals(pattern3)
            && timeZoneId == timeZoneId2 && timeZoneId == timeZoneId3 =>
        e
    }
  }
}

/**
 * Combine nested [[Concat]] expressions.
 */
object CombineConcats extends Rule[LogicalPlan] {

  private def flattenConcats(concat: Concat): Concat = {
    val stack = Stack[Expression](concat)
    val flattened = ArrayBuffer.empty[Expression]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Concat(children) =>
          stack.pushAll(children.reverse)
        // If `spark.sql.function.concatBinaryAsString` is false, nested `Concat` exprs possibly
        // have `Concat`s with binary output. Since `TypeCoercion` casts them into strings,
        // we need to handle the case to combine all nested `Concat`s.
        case c @ Cast(Concat(children), StringType, _, _) =>
          val newChildren = children.map { e => c.copy(child = e) }
          stack.pushAll(newChildren.reverse)
        case child =>
          flattened += child
      }
    }
    Concat(flattened.toSeq)
  }

  private def hasNestedConcats(concat: Concat): Boolean = concat.children.exists {
    case c: Concat => true
    case c @ Cast(Concat(children), StringType, _, _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(CONCAT), ruleId) {
    case concat: Concat if hasNestedConcats(concat) =>
      flattenConcats(concat)
  }
}

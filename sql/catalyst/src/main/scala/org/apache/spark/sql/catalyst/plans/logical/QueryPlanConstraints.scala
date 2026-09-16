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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern.WITH_EXPRESSION
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils.isBinaryStable


trait QueryPlanConstraints extends ConstraintHelper { self: LogicalPlan =>

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = {
    if (conf.constraintPropagationEnabled) {
      val base = asConstraints(validConstraints.toSeq)
      base
        .union(inferAdditionalConstraints(base))
        .union(inferConstraintsFromLiteralBindings(base))
        .union(constructIsNotNullConstraints(base, output))
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

  /**
   * The form `predicates` take as constraints. `RewriteWithExpression` leaves a `With` where it
   * cannot pre-evaluate the definition, in a join condition or a conditional branch, and everything
   * downstream reads constraints as plain, individually true predicates: `IsNotNull` inference
   * walks them, one is substituted into another, and `InferFiltersFromConstraints` plants them as
   * filters to be pushed down and translated for a data source. A `With` is opaque to the first and
   * the last, and a conjunction hidden inside one is never matched against the conjuncts an
   * operator already carries. So a `With` is read as the expression it stands for, where no single
   * constraint is left holding two copies of a definition.
   */
  protected def asConstraints(predicates: Seq[Expression]): ExpressionSet =
    ExpressionSet(predicates.flatMap(splitConjunctsReadingWiths))

  /**
   * `predicate`'s top-level conjuncts, seeing through a `With` that is one of them. Splitting the
   * conjunction it hides is what keeps a definition from being duplicated within a conjunct:
   * `BETWEEN` builds `ref >= lower AND ref <= upper`, so the split leaves each conjunct holding a
   * single reference. Two conjuncts then each hold one copy, which is what the condition itself
   * held before `RewriteWithExpression` learned to keep the `With`.
   */
  private def splitConjunctsReadingWiths(predicate: Expression): Seq[Expression] = predicate match {
    case And(left, right) =>
      splitConjunctsReadingWiths(left) ++ splitConjunctsReadingWiths(right)
    case w: With if splitConjuncts(w.child).forall(readsEachDefinitionOnce(w, _)) =>
      splitConjunctsReadingWiths(With.inlineDefinitions(w))
    case other => Seq(readFreeWiths(other))
  }

  /** `condition`'s top-level conjuncts. This is `PredicateHelper.splitConjunctivePredicates`, kept
   * local rather than mixed in: `ConstraintHelper` is inherited by every `LogicalPlan`, so taking
   * the whole of `PredicateHelper` for one method would add a dozen members to all of them.
   */
  private def splitConjuncts(condition: Expression): Seq[Expression] = condition match {
    case And(left, right) => splitConjuncts(left) ++ splitConjuncts(right)
    case other => other :: Nil
  }

  /**
   * Whether `e` reads each of `w`'s definitions at most once, so that reading `w` as the expression
   * it stands for puts no second copy of a definition in one constraint. A constraint can be
   * planted as a filter, where a second copy is work the memoized form did not do; where the copies
   * would be real the `With` stays, one constraint, opaque as it was to this code before.
   */
  private def readsEachDefinitionOnce(w: With, e: Expression): Boolean = {
    val ids = w.defs.map(_.id).toSet
    val reads = e.collect { case ref: CommonExpressionRef if ids.contains(ref.id) => ref.id }
    reads.distinct.length == reads.length
  }

  /**
   * `e` with every `With` below the top of a constraint read as the expression it stands for, where
   * that duplicates nothing. `RewriteWithExpression` inlines a `With` this cheap itself, but a plan
   * is asked for its constraints while the rules that would do so are still running.
   */
  private def readFreeWiths(e: Expression): Expression =
    e.transformUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case w: With if readsEachDefinitionOnce(w, w.child) => With.inlineDefinitions(w)
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

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

import org.apache.spark.sql.catalyst.expressions._


trait QueryPlanConstraints extends ConstraintHelper { self: LogicalPlan =>

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = {
    if (conf.constraintPropagationEnabled) {
      ExpressionSet(
        validConstraints
          .union(inferAdditionalConstraints(validConstraints))
          .union(constructIsNotNullConstraints(validConstraints, output))
          .filter { c =>
            c.references.nonEmpty && c.references.subsetOf(outputSet) && c.deterministic
          }
      )
    } else {
      ExpressionSet(Set.empty)
    }
  }

  /**
   * This method can be overridden by any child class of QueryPlan to specify a set of constraints
   * based on the given operator's constraint propagation logic. These constraints are then
   * canonicalized and filtered automatically to contain only those attributes that appear in the
   * [[outputSet]].
   *
   * See [[Canonicalize]] for more details.
   */
  protected def validConstraints: Set[Expression] = Set.empty
}

trait ConstraintHelper {

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   */
  def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        val candidateConstraints = constraints - eq
        inferredConstraints ++= replaceConstraints(candidateConstraints, l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, r, l)
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  private def replaceConstraints(
      constraints: Set[Expression],
      source: Expression,
      destination: Attribute): Set[Expression] = constraints.map(_ transform {
    case e: Expression if e.semanticEquals(source) => destination
  })

  /**
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  def constructIsNotNullConstraints(
      constraints: Set[Expression],
      output: Seq[Attribute]): Set[Expression] = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints: Set[Expression] = constraints.flatMap(inferIsNotNullConstraints)

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull).toSet

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
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }

  def normalizeAndReduceWithConstraints(expression: Expression): Expression =
    reduceWithConstraints(normalize(expression))._1

  private def normalize(expression: Expression) = expression transform {
    case GreaterThan(x, y) => LessThan(y, x)
    case GreaterThanOrEqual(x, y) => LessThanOrEqual(y, x)
  }

  /**
   * Traverse a condition as a tree and simplify expressions with constraints.
   * - This functions assumes that the plan has been normalized using [[normalize()]]
   * - On matching [[And]], recursively traverse both children, simplify child expressions with
   *   propagated constraints from sibling and propagate up union of constraints.
   * - If a child of [[And]] is [[LessThan]], [[LessThanOrEqual]], [[EqualTo]], [[EqualNullSafe]],
   *   propagate the constraint.
   * - On matching [[Or]] or [[Not]], recursively traverse each children, propagate no constraints.
   * - Otherwise, stop traversal and propagate no constraints.
   * @param expression expression to be traversed
   * @return A tuple including:
   *         1. Expression: optionally changed expression after traversal
   *         2. Seq[Expression]: propagated constraints
   */
  private def reduceWithConstraints(expression: Expression): (Expression, Seq[Expression]) =
    expression match {
      case e @ (_: LessThan | _: LessThanOrEqual | _: EqualTo | _: EqualNullSafe)
        if e.deterministic => (e, Seq(e))
      case a @ And(left, right) =>
        val (newLeft, leftConstraints) = reduceWithConstraints(left)
        val simplifiedRight = reduceWithConstraints(right, leftConstraints)
        val (simplifiedNewRight, rightConstraints) = reduceWithConstraints(simplifiedRight)
        val simplifiedNewLeft = reduceWithConstraints(newLeft, rightConstraints)
        val newAnd = if ((simplifiedNewLeft fastEquals left) &&
          (simplifiedNewRight fastEquals right)) {
          a
        } else {
          And(simplifiedNewLeft, simplifiedNewRight)
        }
        (newAnd, leftConstraints ++ rightConstraints)
      case o @ Or(left, right) =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newLeft, _) = reduceWithConstraints(left)
        val (newRight, _) = reduceWithConstraints(right)
        val newOr = if ((newLeft fastEquals left) && (newRight fastEquals right)) {
          o
        } else {
          Or(newLeft, newRight)
        }

        (newOr, Seq.empty)
      case n @ Not(child) =>
        // Ignore the EqualityPredicates from children since they are only propagated through And.
        val (newChild, _) = reduceWithConstraints(child)
        val newNot = if (newChild fastEquals child) {
          n
        } else {
          Not(newChild)
        }
        (newNot, Seq.empty)
      case _ => (expression, Seq.empty)
    }

  private def reduceWithConstraints(expression: Expression, constraints: Seq[Expression]) =
    constraints.foldLeft(expression)((e, constraint) => reduceWithConstraint(e, constraint))

  private def planEqual(x: Expression, y: Expression) =
    !x.foldable && !y.foldable && x.canonicalized == y.canonicalized

  private def valueEqual(x: Expression, y: Expression) =
    x.foldable && y.foldable && EqualTo(x, y).eval(EmptyRow).asInstanceOf[Boolean]

  private def valueLessThan(x: Expression, y: Expression) =
    x.foldable && y.foldable && LessThan(x, y).eval(EmptyRow).asInstanceOf[Boolean]

  private def valueLessThanOrEqual(x: Expression, y: Expression) =
    x.foldable && y.foldable && LessThanOrEqual(x, y).eval(EmptyRow).asInstanceOf[Boolean]

  private def reduceWithConstraint(expression: Expression, constraint: Expression): Expression =
    constraint match {
      case a LessThan b => expression transformUp {
        case c LessThan d if planEqual(b, d) && (planEqual(a, c) || valueLessThanOrEqual(c, a)) =>
          Literal.TrueLiteral
        case c LessThan d if planEqual(b, c) && (planEqual(a, d) || valueLessThanOrEqual(d, a)) =>
          Literal.FalseLiteral
        case c LessThan d if planEqual(a, c) && (planEqual(b, d) || valueLessThanOrEqual(b, d)) =>
          Literal.TrueLiteral
        case c LessThan d if planEqual(a, d) && (planEqual(b, c) || valueLessThanOrEqual(b, c)) =>
          Literal.FalseLiteral

        case c LessThanOrEqual d
          if planEqual(b, d) && (planEqual(a, c) || valueLessThanOrEqual(c, a)) =>
          Literal.TrueLiteral
        case c LessThanOrEqual d
          if planEqual(b, c) && (planEqual(a, d) || valueLessThanOrEqual(d, a)) =>
          Literal.FalseLiteral
        case c LessThanOrEqual d
          if planEqual(a, c) && (planEqual(b, d) || valueLessThanOrEqual(b, d)) =>
          Literal.TrueLiteral
        case c LessThanOrEqual d
          if planEqual(a, d) && (planEqual(b, c) || valueLessThanOrEqual(b, c)) =>
          Literal.FalseLiteral

        case c EqualTo d if planEqual(b, d) && planEqual(a, c) => Literal.FalseLiteral
        case c EqualTo d if planEqual(b, c) && planEqual(a, d) => Literal.FalseLiteral
        case c EqualTo d if planEqual(a, c) && planEqual(b, d) => Literal.FalseLiteral
        case c EqualTo d if planEqual(a, d) && planEqual(b, c) => Literal.FalseLiteral

        case c EqualNullSafe d if planEqual(b, d) =>
          if (planEqual(a, c)) Literal.FalseLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(b, c) =>
          if (planEqual(a, d)) Literal.FalseLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, c) =>
          if (planEqual(b, d)) Literal.FalseLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, d) =>
          if (planEqual(b, c)) Literal.FalseLiteral else EqualTo(c, d)
      }
      case a LessThanOrEqual b => expression transformUp {
        case c LessThan d if planEqual(b, d) && valueLessThan(c, a) =>
          Literal.TrueLiteral
        case c LessThan d if planEqual(b, c) && (planEqual(a, d) || valueLessThanOrEqual(d, a)) =>
          Literal.FalseLiteral
        case c LessThan d if planEqual(a, c) && valueLessThan(b, d) =>
          Literal.TrueLiteral
        case c LessThan d if planEqual(a, d) && (planEqual(b, c) || valueLessThanOrEqual(b, c)) =>
          Literal.FalseLiteral

        case c LessThanOrEqual d
          if planEqual(b, d) && (planEqual(a, c) || valueLessThanOrEqual(c, a)) =>
          Literal.TrueLiteral
        case c LessThanOrEqual d if planEqual(b, c) && valueLessThan(d, a) =>
          Literal.FalseLiteral
        case c LessThanOrEqual d if planEqual(b, c) && (planEqual(a, d) || valueEqual(a, d)) =>
          EqualTo(c, d)
        case c LessThanOrEqual d
          if planEqual(a, c) && (planEqual(b, d) || valueLessThanOrEqual(b, d)) =>
          Literal.TrueLiteral
        case c LessThanOrEqual d if planEqual(a, d) && valueLessThan(b, c) =>
          Literal.FalseLiteral
        case c LessThanOrEqual d if planEqual(a, d) && (planEqual(b, c) || valueEqual(b, c)) =>
          EqualTo(c, d)

        case c EqualNullSafe d if planEqual(b, d) => EqualTo(c, d)
        case c EqualNullSafe d if planEqual(b, c) => EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, c) => EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, d) => EqualTo(c, d)
      }
      case a EqualTo b => expression transformUp {
        case c LessThan d if planEqual(b, d) && planEqual(a, c) => Literal.FalseLiteral
        case c LessThan d if planEqual(b, c) && planEqual(a, d) => Literal.FalseLiteral
        case c LessThan d if planEqual(a, d) && planEqual(b, c) => Literal.FalseLiteral
        case c LessThan d if planEqual(a, c) && planEqual(b, d) => Literal.FalseLiteral

        case c LessThanOrEqual d if planEqual(b, d) && planEqual(a, c) => Literal.TrueLiteral
        case c LessThanOrEqual d if planEqual(b, c) && planEqual(a, d) => Literal.TrueLiteral
        case c LessThanOrEqual d if planEqual(a, d) && planEqual(b, c) => Literal.TrueLiteral
        case c LessThanOrEqual d if planEqual(a, c) && planEqual(b, d) => Literal.TrueLiteral

        case c EqualTo d if planEqual(b, d) && planEqual(a, c) => Literal.TrueLiteral
        case c EqualTo d if planEqual(b, c) && planEqual(a, d) => Literal.TrueLiteral
        case c EqualTo d if planEqual(a, d) && planEqual(b, c) => Literal.TrueLiteral
        case c EqualTo d if planEqual(a, c) && planEqual(b, d) => Literal.TrueLiteral

        case c EqualNullSafe d if planEqual(b, d) =>
          if (planEqual(a, c)) Literal.TrueLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(b, c) =>
          if (planEqual(a, d)) Literal.TrueLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, d) =>
          if (planEqual(b, c)) Literal.TrueLiteral else EqualTo(c, d)
        case c EqualNullSafe d if planEqual(a, c) =>
          if (planEqual(b, d)) Literal.TrueLiteral else EqualTo(c, d)
      }
      case a EqualNullSafe b => expression transformUp {
        case c LessThan d if planEqual(b, d) && planEqual(a, c) => Literal.FalseLiteral
        case c LessThan d if planEqual(b, c) && planEqual(d, a) => Literal.FalseLiteral
        case c LessThan d if planEqual(a, d) && planEqual(b, c) => Literal.FalseLiteral
        case c LessThan d if planEqual(a, c) && planEqual(d, b) => Literal.FalseLiteral

        case c LessThanOrEqual d if planEqual(b, d) && planEqual(a, c) => EqualTo(c, d)
        case c LessThanOrEqual d if planEqual(b, c) && planEqual(a, d) => EqualTo(c, d)
        case c LessThanOrEqual d if planEqual(a, d) && planEqual(b, c) => EqualTo(c, d)
        case c LessThanOrEqual d if planEqual(a, c) && planEqual(b, d) => EqualTo(c, d)

        case c EqualNullSafe d if planEqual(b, d) && planEqual(a, c) => Literal.TrueLiteral
        case c EqualNullSafe d if planEqual(b, c) && planEqual(a, d) => Literal.TrueLiteral
        case c EqualNullSafe d if planEqual(a, d) && planEqual(b, c) => Literal.TrueLiteral
        case c EqualNullSafe d if planEqual(a, c) && planEqual(b, d) => Literal.TrueLiteral
      }
      case _ => expression
    }
}

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


trait QueryPlanConstraints { self: LogicalPlan =>

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
          .union(constructIsNotNullConstraints(validConstraints))
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

  /**
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
  private def constructIsNotNullConstraints(constraints: Set[Expression]): Set[Expression] = {
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

  // Collect aliases from expressions of the whole tree rooted by the current QueryPlan node, so
  // we may avoid producing recursive constraints.
  private lazy val aliasMap: AttributeMap[Expression] = AttributeMap(
    expressions.collect {
      case a: Alias => (a.toAttribute, a.child)
    } ++ children.flatMap(_.asInstanceOf[QueryPlanConstraints].aliasMap))
    // Note: the explicit cast is necessary, since Scala compiler fails to infer the type.

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   *
   * [SPARK-17733] We explicitly prevent producing recursive constraints of the form `a = f(a, b)`
   * as they are often useless and can lead to a non-converging set of constraints.
   */
  private def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    val constraintClasses = generateEquivalentConstraintClasses(constraints)

    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        val candidateConstraints = constraints - eq
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(l) &&
            !isRecursiveDeduction(r, constraintClasses) => r
        })
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(r) &&
            !isRecursiveDeduction(l, constraintClasses) => l
        })
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  /**
   * Generate a sequence of expression sets from constraints, where each set stores an equivalence
   * class of expressions. For example, Set(`a = b`, `b = c`, `e = f`) will generate the following
   * expression sets: (Set(a, b, c), Set(e, f)). This will be used to search all expressions equal
   * to an selected attribute.
   */
  private def generateEquivalentConstraintClasses(
      constraints: Set[Expression]): Seq[Set[Expression]] = {
    var constraintClasses = Seq.empty[Set[Expression]]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        // Transform [[Alias]] to its child.
        val left = aliasMap.getOrElse(l, l)
        val right = aliasMap.getOrElse(r, r)
        // Get the expression set for an equivalence constraint class.
        val leftConstraintClass = getConstraintClass(left, constraintClasses)
        val rightConstraintClass = getConstraintClass(right, constraintClasses)
        if (leftConstraintClass.nonEmpty && rightConstraintClass.nonEmpty) {
          // Combine the two sets.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: rightConstraintClass :: Nil) :+
            (leftConstraintClass ++ rightConstraintClass)
        } else if (leftConstraintClass.nonEmpty) { // && rightConstraintClass.isEmpty
          // Update equivalence class of `left` expression.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: Nil) :+ (leftConstraintClass + right)
        } else if (rightConstraintClass.nonEmpty) { // && leftConstraintClass.isEmpty
          // Update equivalence class of `right` expression.
          constraintClasses = constraintClasses
            .diff(rightConstraintClass :: Nil) :+ (rightConstraintClass + left)
        } else { // leftConstraintClass.isEmpty && rightConstraintClass.isEmpty
          // Create new equivalence constraint class since neither expression presents
          // in any classes.
          constraintClasses = constraintClasses :+ Set(left, right)
        }
      case _ => // Skip
    }

    constraintClasses
  }

  /**
   * Get all expressions equivalent to the selected expression.
   */
  private def getConstraintClass(
      expr: Expression,
      constraintClasses: Seq[Set[Expression]]): Set[Expression] =
    constraintClasses.find(_.contains(expr)).getOrElse(Set.empty[Expression])

  /**
   * Check whether replace by an [[Attribute]] will cause a recursive deduction. Generally it
   * has the form like: `a -> f(a, b)`, where `a` and `b` are expressions and `f` is a function.
   * Here we first get all expressions equal to `attr` and then check whether at least one of them
   * is a child of the referenced expression.
   */
  private def isRecursiveDeduction(
      attr: Attribute,
      constraintClasses: Seq[Set[Expression]]): Boolean = {
    val expr = aliasMap.getOrElse(attr, attr)
    getConstraintClass(expr, constraintClasses).exists { e =>
      expr.children.exists(_.semanticEquals(e))
    }
  }
}

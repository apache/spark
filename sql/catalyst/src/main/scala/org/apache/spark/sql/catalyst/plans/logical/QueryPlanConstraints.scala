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
      validConstraints
        .union(inferAdditionalConstraints(validConstraints))
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
   * See [[Canonicalize]] for more details.
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
        val candidateConstraints = predicates - eq
        inferredConstraints ++= replaceConstraints(candidateConstraints, l, r)
        inferredConstraints ++= replaceConstraints(candidateConstraints, r, l)
      case eq @ EqualTo(l @ Cast(_: Attribute, _, _), r: Attribute) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, r, l)
      case eq @ EqualTo(l: Attribute, r @ Cast(_: Attribute, _, _)) =>
        inferredConstraints ++= replaceConstraints(predicates - eq, l, r)
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  private def replaceConstraints(
      constraints: ExpressionSet,
      source: Expression,
      destination: Expression): ExpressionSet = constraints.map(_ transform {
    case e: Expression if e.semanticEquals(source) => destination
  })

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
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }
}

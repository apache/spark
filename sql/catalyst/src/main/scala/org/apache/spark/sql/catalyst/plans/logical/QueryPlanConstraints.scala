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
   * See [[Expression#canonicalized]] for more details.
   */
  protected lazy val validConstraints: ExpressionSet = ExpressionSet()
}

trait ConstraintHelper {

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   *
   * This method performs two main types of inference:
   *  1. Attribute-to-attribute: For example, if an operator has constraints
   *     of the form (`a = 5`, `a = b`), this returns an additional constraint of the form `b = 5`.
   *  2. Constant propagation: If the constraints contain both an equality to a constant and a
   *     complex expression, such as `a = 5` and `b = a + 3`, it will infer `b = 5 + 3`
   *     by substituting the constant into the expression.
   *
   * @param constraints The set of input constraints
   * @return A new set of inferred constraints
   */
  def inferAdditionalConstraints(constraints: ExpressionSet): ExpressionSet = {
    var inferredConstraints = ExpressionSet()
    // IsNotNull should be constructed by `constructIsNotNullConstraints`.
    val predicates = constraints.filterNot(_.isInstanceOf[IsNotNull])

    // Step 1: Infer attribute-to-attribute equalities
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

    // Step 2: Infer by constant substitution (e.g., a = 5, b = a + 3 => b = 5 + 3)
    val equalityPredicates = predicates.toSeq.flatMap {
      case e @ EqualTo(left: AttributeReference, right: Literal) => Some(((left, right), e))
      case e @ EqualTo(left: Literal, right: AttributeReference) => Some(((right, left), e))
      case _ => None
    }
    if (equalityPredicates.nonEmpty) {
      val constantsMap = AttributeMap(equalityPredicates.map(_._1))
      val predicateSet = equalityPredicates.map(_._2).toSet
      def replaceConstantsInExpression(expression: Expression) = expression transform {
        case a: AttributeReference =>
          constantsMap.get(a) match {
            case Some(literal) => literal
            case None => a
          }
      }
      predicates.foreach { cond =>
        val replaced = cond transform {
          // attribute equality is handled above, no need to replace
          case e @ EqualTo(_: Attribute, _: Attribute) => e
          case e @ EqualTo(_: Cast, _: Attribute) => e
          case e @ EqualTo(_: Attribute, _: Cast) => e

          case e @ EqualTo(_, _) if !predicateSet.contains(e) => replaceConstantsInExpression(e)
        }
        // Avoid inferring tautologies like 1 = 1
        val isTautology = replaced match {
          case EqualTo(left: Expression, right: Expression) if left.foldable && right.foldable =>
            left.eval() == right.eval()
          case _ => false
        }
        if (!constraints.contains(replaced) && !isTautology) {
          inferredConstraints += replaced
        }
      }
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

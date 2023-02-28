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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf

trait QueryPlanConstraints extends ConstraintHelper { self: LogicalPlan =>

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = {
    if (conf.constraintPropagationEnabled) {
      inferConstraints(validConstraints)
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

trait ConstraintHelper extends SQLConfHelper {

  lazy val constraintInferenceLimit = conf.getConf(SQLConf.CONSTRAINT_INFERENCE_LIMIT)

  /**
   * Infers an additional set of constraints from a given set of equality constraints and returns
   * them with the original constraint set.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   */
  def inferConstraints(constraints: ExpressionSet): ExpressionSet = {
    // IsNotNull should be constructed by `constructIsNotNullConstraints`.
    val (notNullConstraints, predicates) = constraints.partition(_.isInstanceOf[IsNotNull])

    val equivalenceMap = mutable.Map.empty[Expression, mutable.ArrayBuffer[Expression]]
    predicates.foreach {
      case EqualTo(l: Attribute, r: Attribute) =>
        equivalenceMap.getOrElseUpdate(l.canonicalized, mutable.ArrayBuffer.empty) += r
        equivalenceMap.getOrElseUpdate(r.canonicalized, mutable.ArrayBuffer.empty) += l
      case EqualTo(l @ Cast(_: Attribute, _, _, _), r: Attribute) =>
        equivalenceMap.getOrElseUpdate(r.canonicalized, mutable.ArrayBuffer.empty) += l
      case EqualTo(l: Attribute, r @ Cast(_: Attribute, _, _, _)) =>
        equivalenceMap.getOrElseUpdate(l.canonicalized, mutable.ArrayBuffer.empty) += r
      case _ => // No inference
    }

    def inferConstraints(expr: Expression) = {
      // The current constraint inference doesn't do a full-blown inference which means that when
      // - a constraint contain an attribute multiple times (E.g. `c + c > 1`)
      // - and we have multiple equivalences for that attribute  (E.g. `c = a`, `c = b`)
      // then we return only `a + a > 1` and `b + b > 1` besides the original constraint, but
      // doesn't return `a + b > 1`.
      val currentMapping = mutable.Map.empty[Expression, Seq[Expression]]
      expr.multiTransformDown {
        case e: Expression if equivalenceMap.contains(e.canonicalized) =>
          // When we encounter an attribute for the first time in the tree, set up a cache to track
          // the current equivalence and return that cached equivalence when we encounter the same
          // expression at other places.
          currentMapping.getOrElse(e.canonicalized, {
            // Always return the original expression too
            val alternatives = e +: equivalenceMap(e.canonicalized).toSeq

            // When iterate through the alternatives for the first encounter we also update the
            // cache.
            alternatives.toStream.map { a =>
              currentMapping += e.canonicalized -> Seq(a)
              a
            }.append {
              currentMapping -= e.canonicalized
              Seq.empty
            }
          })
      }.filter {
        case EqualTo(e1, e2) => e1.canonicalized != e2.canonicalized
        case _ => true
      }
    }

    val inferredConstraints = predicates.toStream.flatMap(inferConstraints)

    notNullConstraints ++
      constraintInferenceLimit.map(l => inferredConstraints.take(l)).getOrElse(inferredConstraints)
  }

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
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }
}

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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._

object Optimizer extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("ConstantFolding", FixedPoint(100),
      NullPropagation,
      ConstantFolding,
      BooleanSimplification,
      SimplifyFilters,
      SimplifyCasts) ::
    Batch("Filter Pushdown", FixedPoint(100),
      CombineFilters,
      PushPredicateThroughProject,
      PushPredicateThroughJoin,
      ColumnPruning) :: Nil
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Project <- Join
 *  - Collapse adjacent projections, performing alias substitution.
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of off references required either above or to evaluate the condition.
      val allReferences: Set[Attribute] =
        projectList.flatMap(_.references).toSet ++ condition.map(_.references).getOrElse(Set.empty)

      /** Applies a projection only when the child is producing unnecessary attributes */
      def prunedChild(c: LogicalPlan) =
        if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
          Project(allReferences.filter(c.outputSet.contains).toSeq, c)
        } else {
          c
        }

      Project(projectList, Join(prunedChild(left), prunedChild(right), joinType, condition))

    // Combine adjacent Projects.
    case Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = projectList2.collect {
        case a @ Alias(e, _) => (a.toAttribute: Expression, a)
      }.toMap

      // Substitute any attributes that are produced by the child projection, so that we safely
      // eliminate it.
      // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
      // TODO: Fix TransformBase to avoid the cast below.
      val substitutedProjection = projectList1.map(_.transform {
        case a if aliasMap.contains(a) => aliasMap(a)
      }).asInstanceOf[Seq[NamedExpression]]

      Project(substitutedProjection, child)

    // Eliminate no-op Projects
    case Project(projectList, child) if(child.output == projectList) => child
  }
}

/**
 * Replaces [[catalyst.expressions.Expression Expressions]] that can be statically evaluated with
 * equivalent [[catalyst.expressions.Literal Literal]] values. This rule is more specific with 
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ Count(Literal(null, _)) => Literal(0, e.dataType)
      case e @ Sum(Literal(c, _)) if c == 0 => Literal(0, e.dataType)
      case e @ Average(Literal(c, _)) if c == 0 => Literal(0.0, e.dataType)
      case e @ IsNull(c) if c.nullable == false => Literal(false, BooleanType)
      case e @ IsNotNull(c) if c.nullable == false => Literal(true, BooleanType)
      case e @ GetItem(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ GetItem(_, Literal(null, _)) => Literal(null, e.dataType)
      case e @ GetField(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ Coalesce(children) => {
        val newChildren = children.filter(c => c match {
          case Literal(null, _) => false
          case _ => true
        })
        if (newChildren.length == 0) {
          Literal(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren(0)
        } else {
          Coalesce(newChildren)
        }
      }
      case e @ If(Literal(v, _), trueValue, falseValue) => if (v == true) trueValue else falseValue
      case e @ In(Literal(v, _), list) if (list.exists(c => c match {
          case Literal(candidate, _) if candidate == v => true
          case _ => false
        })) => Literal(true, BooleanType)
      case e: UnaryMinus => e.child match {
        case Literal(null, _) => Literal(null, e.dataType)
        case _ => e
      }
      case e: Cast => e.child match {
        case Literal(null, _) => Literal(null, e.dataType)
        case _ => e
      }
      case e: Not => e.child match {
        case Literal(null, _) => Literal(null, e.dataType)
        case _ => e
      }
      // Put exceptional cases above if any
      case e: BinaryArithmetic => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: BinaryComparison => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
    }
  }
}

/**
 * Replaces [[catalyst.expressions.Expression Expressions]] that can be statically evaluated with
 * equivalent [[catalyst.expressions.Literal Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals.
      case l: Literal => l
      case e if e.foldable => Literal(e.eval(null), e.dataType)
    }
  }
}

/**
 * Simplifies boolean expressions where the answer can be determined without evaluating both sides.
 * Note that this rule can eliminate expressions that might otherwise have been evaluated and thus
 * is only safe when evaluations of expressions does not result in side effects.
 */
object BooleanSimplification extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => and
        }

      case or @ Or(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), _) => Literal(true)
          case (_, Literal(true, BooleanType)) => Literal(true)
          case (Literal(false, BooleanType), r) => r
          case (l, Literal(false, BooleanType)) => l
          case (_, _) => or
        }
    }
  }
}

/**
 * Combines two adjacent [[catalyst.plans.logical.Filter Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(Literal(true, BooleanType), child) =>
      child
    case Filter(Literal(null, _), child) =>
      LocalRelation(child.output)
    case Filter(Literal(false, BooleanType), child) =>
      LocalRelation(child.output)
  }
}

/**
 * Pushes [[catalyst.plans.logical.Filter Filter]] operators through
 * [[catalyst.plans.logical.Project Project]] operators, in-lining any
 * [[catalyst.expressions.Alias Aliases]] that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a @ Alias(c, _) =>
        (a.toAttribute: Attribute) -> c
      }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Pushes down [[catalyst.plans.logical.Filter Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[catalyst.plans.logical.Filter Filter]] conditions are moved into the `condition` of the
 * [[catalyst.plans.logical.Join Join]].
 * And also Pushes down the join filter, where the `condition` can be evaluated using only the 
 * attributes of the left or right side of sub query when applicable. 
 * 
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  // split the condition expression into 3 parts, 
  // (canEvaluateInLeftSide, canEvaluateInRightSide, haveToEvaluateWithBothSide) 
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) = 
        rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) = 
        split(splitConjunctivePredicates(filterCondition), left, right)

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) = 
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case Inner =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case FullOuter => f
      }
  }
}

/**
 * Removes [[catalyst.expressions.Cast Casts]] that are unnecessary because the input is already
 * the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}

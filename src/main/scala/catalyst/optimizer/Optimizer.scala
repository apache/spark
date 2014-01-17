package catalyst
package optimizer

import catalyst.expressions._
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types.BooleanType
import catalyst.plans.Inner

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries) ::
    Batch("ConstantFolding", Once,
      ConstantFolding,
      BooleanSimplification,
      SimplifyCasts) ::
    Batch("Filter Pushdown", Once,
      EliminateSubqueries,
      CombineFilters,
      PushPredicateThroughProject,
      PushPredicateThroughInnerJoin) :: Nil
}

/**
 * Removes [[catalyst.plans.logical.Subquery Subquery]] operators from the plan.  Subqueries are
 * only required to provide scoping information for attributes and can be removed once analysis is
 * complete.
 */
object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
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
      case e if e.foldable => Literal(Evaluate(e, Nil), e.dataType)
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
      case and @ And(left, right) => {
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => and
        }
      }
      case or @ Or(left, right) => {
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
}

/**
 * Combines two adjacent [[catalyst.plans.logical.Filter Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff@Filter(fc, nf@Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
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
    case filter@Filter(condition, project@Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a@Alias(c, _) => a.toAttribute -> c }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  //
  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Pushes down [[catalyst.plans.logical.Filter Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of an inner join.  Other
 * [[catalyst.plans.logical.Filter Filter]] conditions are moved into the `condition` of the
 * [[catalyst.plans.logical.Join Join]].
 */
object PushPredicateThroughInnerJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(filterCondition, Join(left, right, Inner, joinCondition)) =>
      val allConditions =
        splitConjunctivePredicates(filterCondition) ++
          joinCondition.map(splitConjunctivePredicates).getOrElse(Nil)

      // Split the predicates into those that can be evaluated on the left, right, and those that
      // must be evaluated after the join.
      val (rightConditions, leftOrJoinConditions) =
        allConditions.partition(_.references subsetOf right.outputSet)
      val (leftConditions, joinConditions) =
        leftOrJoinConditions.partition(_.references subsetOf left.outputSet)

      // Build the new left and right side, optionally with the pushed down filters.
      val newLeft = leftConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
      val newRight = rightConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
      Join(newLeft, newRight, Inner, joinConditions.reduceLeftOption(And))
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
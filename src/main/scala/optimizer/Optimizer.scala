package catalyst
package optimizer

import catalyst.expressions._
import catalyst.expressions.Alias
import catalyst.plans.Inner
import catalyst.plans.logical._
import catalyst.plans.logical.Filter
import catalyst.plans.logical.Project
import catalyst.plans.logical.Subquery
import catalyst.rules._
import catalyst.types.{NullType, DataType}

object Optimize extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Subqueries", Once,
      EliminateSubqueries,
      CombineFilters,
      PredicatePushDownThoughProject,
      PushPredicateThroughInnerJoin) :: Nil
}

object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Combines two filter operators into one
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff@Filter(fc, nf@Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Pushes predicate through project, also inlines project's aliases in filter
 */
object PredicatePushDownThoughProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter@Filter(condition, project@Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a@Alias(c, _) => a.toAttribute -> c }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  // Assuming the expression cost is minimal, we can replace condition with the alias's child to push down predicate
  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Pushes down predicates that can be evaluated using only the attributes of the left or right side
 * of a join.  Other predicates are left as a single filter on top of the join.
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
      val newJoin = Join(newLeft, newRight, Inner, None)
      joinConditions.reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
  }
}
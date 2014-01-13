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
      BooleanSimplification) ::
    Batch("Filter Pushdown", Once,
      EliminateSubqueries,
      CombineFilters,
      PredicatePushDownThoughProject,
      PushPredicateThroughInnerJoin) :: Nil
}

/**
 * Removes subqueries from the plan.  Subqueries are only required to provide scoping information
 * for attributes and can be removed once analysis is complete.
 */
object EliminateSubqueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Replaces expressions that can be statically evaluated with equivalent [[expressions.Literal]]
 * values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
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
      Join(newLeft, newRight, Inner, joinConditions.reduceLeftOption(And))
  }
}
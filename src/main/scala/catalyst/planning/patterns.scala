package catalyst
package planning

import scala.annotation.tailrec

import expressions._
import plans.logical._

/**
 * A pattern that matches any number of filter operations on top of another relational operator.
 * Adjacent filter operators are collected and their conditions are broken up and returned as a
 * sequence of conjunctive predicates.
 *
 * @return A tuple containing a sequence of conjunctive predicates that should be used to filter the
 *         output and a relational operator.
 */
object FilteredOperation {
  type ReturnType = (Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) =>
      collectFilters(filters ++ splitConjunctivePredicates(condition), child)
    case other => (filters, other)
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = condition match {
    case And(cond1, cond2) => splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
    case other => other :: Nil
  }
}
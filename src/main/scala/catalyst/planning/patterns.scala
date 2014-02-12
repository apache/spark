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
object FilteredOperation extends PredicateHelper {
  type ReturnType = (Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) =>
      collectFilters(filters ++ splitConjunctivePredicates(condition), child)
    case other => (filters, other)
  }
}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.  [[Alias Aliases]] are in-lined/substituted if necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] =
    Some(collectProjectsAndFilters(None, Nil, plan, Map.empty))

  /**
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1 WHERE c1 > 10) t2
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1 WHERE c1 > 10) t2
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  def collectProjectsAndFilters(
      topFields: Option[Seq[Expression]],
      filters: Seq[Expression],
      plan: LogicalPlan,
      aliases: Map[Attribute, Expression]): ReturnType = {
    plan match {
      case Project(fields, child) =>
        val moreAliases = aliases ++ collectAliases(fields)
        val updatedTopFields = topFields.map(_.map(substitute(moreAliases))).getOrElse(fields)
        collectProjectsAndFilters(Some(updatedTopFields), filters, child, moreAliases)

      case Filter(condition, child) =>
        val moreFilters = filters ++ splitConjunctivePredicates(condition)
        collectProjectsAndFilters(topFields, moreFilters.map(substitute(aliases)), child, aliases)

      case other =>
        (topFields.getOrElse(other.output).asInstanceOf[Seq[NamedExpression]], filters, other)
    }
  }

  def collectAliases(fields: Seq[Expression]) = fields.collect {
    case a @ Alias(child, _) => a.toAttribute.asInstanceOf[Attribute] -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = expr.transform {
    case a @ Alias(ref: AttributeReference, name) =>
      aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

    case a: AttributeReference =>
      aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}

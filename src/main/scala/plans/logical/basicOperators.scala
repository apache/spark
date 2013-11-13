package catalyst
package plans
package logical

import expressions._

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)
  def references = projectList.flatMap(_.references).toSet
}

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = condition.references
}

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  def references = condition.map(_.references).getOrElse(Set.empty)
  def output = left.output ++ right.output
}
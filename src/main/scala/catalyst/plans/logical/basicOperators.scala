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

case class Union(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  // TODO: These aren't really the same attributes as nullability etc might change.
  def output = left.output
  def references = Set.empty
}

case class Join(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  def references = condition.map(_.references).getOrElse(Set.empty)
  def output = left.output ++ right.output
}

case class InsertIntoTable(table: BaseRelation, partition: Map[String, String], child: LogicalPlan)
  extends LogicalPlan {
  // The table being inserted into is a child for the purposes of transformations.
  def children = table :: child :: Nil
  def references = Set.empty
  def output = child.output
}

case class InsertIntoCreatedTable(tableName: String, child: LogicalPlan) extends UnaryNode {
  def references = Set.empty
  def output = child.output
}

case class Sort(order: Seq[SortOrder], child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = order.flatMap(_.references).toSet
}

case class Aggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan)
  extends UnaryNode {

  def output = aggregateExpressions.map(_.toAttribute)
  def references = child.references
}

case class StopAfter(limit: Expression, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = limit.references
}

case class Subquery(alias: String, child: LogicalPlan) extends UnaryNode {
  def output = child.output.map(_.withQualifiers(alias :: Nil))
  def references = Set.empty
}

case class Sample(percentage: Double, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = Set.empty
}

case class Distinct(child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = child.outputSet
}

case object NoRelation extends LeafNode {
  def output = Nil
}

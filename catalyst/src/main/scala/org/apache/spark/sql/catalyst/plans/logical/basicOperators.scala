package org.apache.spark.sql
package catalyst
package plans
package logical

import expressions._

case class Project(projectList: Seq[NamedExpression], child: LogicalPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)
  def references = projectList.flatMap(_.references).toSet
}

/**
 * Applies a [[catalyst.expressions.Generator Generator]] to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a `flatMap` in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * @param join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * @param outer when true, each input row will be output at least once, even if the output of the
 *              given `generator` is empty. `outer` has no effect when `join` is false.
 * @param alias when set, this string is applied to the schema of the output of the transformation
 *              as a qualifier.
 */
case class Generate(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    alias: Option[String],
    child: LogicalPlan)
  extends UnaryNode {

  protected def generatorOutput =
    alias
      .map(a => generator.output.map(_.withQualifiers(a :: Nil)))
      .getOrElse(generator.output)

  def output =
    if (join) child.output ++ generatorOutput else generatorOutput

  def references =
    if (join) child.outputSet else generator.references
}

case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  def output = child.output
  def references = condition.references
}

case class Union(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
  // TODO: These aren't really the same attributes as nullability etc might change.
  def output = left.output

  override lazy val resolved =
    childrenResolved &&
    !left.output.zip(right.output).exists { case (l,r) => l.dataType != r.dataType }

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

case class InsertIntoTable(
    table: BaseRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan)
  extends LogicalPlan {
  // The table being inserted into is a child for the purposes of transformations.
  def children = table :: child :: Nil
  def references = Set.empty
  def output = child.output

  override lazy val resolved = childrenResolved && child.output.zip(table.output).forall {
    case (childAttr, tableAttr) => childAttr.dataType == tableAttr.dataType
  }
}

case class InsertIntoCreatedTable(
    databaseName: Option[String],
    tableName: String,
    child: LogicalPlan) extends UnaryNode {
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

case class Sample(fraction: Double, withReplacement: Boolean, seed: Int, child: LogicalPlan)
    extends UnaryNode {

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

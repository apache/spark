package catalyst
package execution

import catalyst.errors._
import catalyst.expressions._

case class Project(projectList: Seq[NamedExpression], child: SharkPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)

  def execute() = child.execute().map { row =>
    buildRow(projectList.map(Evaluate(_, Vector(row))))
  }
}

case class Filter(condition: Expression, child: SharkPlan) extends UnaryNode {
  def output = child.output
  def execute() = child.execute().filter { row =>
    Evaluate(condition, Vector(row)).asInstanceOf[Boolean]
  }
}

case class Sample(fraction: Double, withReplacement: Boolean, seed: Int, child: SharkPlan)
    extends UnaryNode {

  def output = child.output

  // TODO: How to pick seed?
  def execute() = child.execute().sample(withReplacement, fraction, seed)
}

case class Union(children: Seq[SharkPlan])(@transient sc: SharkContext) extends SharkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  def output = children.head.output
  def execute() = sc.union(children.map(_.execute()))

  override def otherCopyArgs = sc :: Nil
}

case class StopAfter(limit: Int, child: SharkPlan)(@transient sc: SharkContext) extends UnaryNode {
  override def otherCopyArgs = sc :: Nil

  def output = child.output

  override def executeCollect() = child.execute().take(limit)

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  def execute() = sc.makeRDD(executeCollect(), 1)
}

case class SortPartitions(sortOrder: Seq[SortOrder], child: SharkPlan) extends UnaryNode {
  @transient
  lazy val ordering = new RowOrdering(sortOrder)

  def execute() = attachTree(this, "sort") {
    child.execute()
      .mapPartitions(
        iterator => iterator.toArray.sorted(ordering).iterator,
        preservesPartitioning = true)
  }

  def output = child.output
}

// TODO: Rename: SchemaRDD
case class LocalRelation(output: Seq[Attribute], data: Seq[IndexedSeq[Any]])
                        (@transient sc: SharkContext) extends LeafNode {

  // Since LocalRelation is used for unit tests, set the defaultParallelism to 2
  // to make sure we can cover bugs appearing in a distributed environment.
  def execute() = sc.makeRDD(data.map(buildRow), 2)
}


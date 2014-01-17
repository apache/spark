package catalyst
package execution

import org.apache.spark.rdd.RDD

import plans.QueryPlan

abstract class SharkPlan extends QueryPlan[SharkPlan] with Logging {
  self: Product =>

  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Distribution = UnknownDistribution
  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildPartitioning: Seq[Distribution] = Seq.fill(children.size)(UnknownDistribution)

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[Row]

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[Row] = execute().collect()

  protected def buildRow(values: Seq[Any]): Row = new catalyst.expressions.GenericRow(values)
}

trait LeafNode extends SharkPlan with trees.LeafNode[SharkPlan] {
  self: Product =>
}

trait UnaryNode extends SharkPlan with trees.UnaryNode[SharkPlan] {
  self: Product =>
}

trait BinaryNode extends SharkPlan with trees.BinaryNode[SharkPlan] {
  self: Product =>
}

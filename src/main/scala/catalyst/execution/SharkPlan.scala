package catalyst
package execution

import catalyst.plans.QueryPlan
import org.apache.spark.rdd.RDD

abstract class SharkPlan extends QueryPlan[SharkPlan] with Logging {
  self: Product =>

  /**
   * Runs this query returning the result as an RDD.
   */
  def execute(): RDD[Row]

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

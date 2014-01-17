package catalyst
package execution

import org.apache.spark.rdd.RDD

import catalyst.plans.QueryPlan
import scala.reflect.ClassTag

abstract class SharkPlan extends QueryPlan[SharkPlan] with Logging {
  self: Product =>

  def requiredPartitioningSchemes: Seq[Partitioned]
  def outputPartitioningScheme: Partitioned

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

  def requiredPartitioningScheme: Partitioned = NotSpecified()
  def requiredPartitioningSchemes: Seq[Partitioned] = Seq(requiredPartitioningScheme)
  // TODO: We should get the output data properties of a leaf node from metadata.
  def outputPartitioningScheme: Partitioned = NotSpecified()
}

trait UnaryNode extends SharkPlan with trees.UnaryNode[SharkPlan] {
  self: Product =>

  def requiredPartitioningScheme: Partitioned = NotSpecified()
  def requiredPartitioningSchemes: Seq[Partitioned] = Seq(requiredPartitioningScheme)

  def outputPartitioningScheme: Partitioned = child.outputPartitioningScheme
}

trait BinaryNode extends SharkPlan with trees.BinaryNode[SharkPlan] {
  self: Product =>

  def leftRequiredPartitioningScheme: Partitioned = NotSpecified()
  def rightRequiredPartitioningScheme: Partitioned = NotSpecified()
  def requiredPartitioningSchemes: Seq[Partitioned] =
    Seq(leftRequiredPartitioningScheme, rightRequiredPartitioningScheme)

  def outputPartitioningScheme: Partitioned = NotSpecified()
}

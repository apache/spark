package org.apache.spark.sql
package execution

import org.apache.spark.rdd.RDD

import catalyst.plans.QueryPlan
import catalyst.plans.physical._
import catalyst.trees

abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging {
  self: Product =>

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!
  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

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

trait LeafNode extends SparkPlan with trees.LeafNode[SparkPlan] {
  self: Product =>
}

trait UnaryNode extends SparkPlan with trees.UnaryNode[SparkPlan] {
  self: Product =>
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryNode extends SparkPlan with trees.BinaryNode[SparkPlan] {
  self: Product =>
}

package catalyst
package execution

import org.apache.spark.rdd.RDD

import catalyst.plans.QueryPlan
import scala.reflect.ClassTag

abstract class SharkPlan extends QueryPlan[SharkPlan] with Logging {
  self: Product =>

  def requiredDataProperties: Seq[DataProperty]
  def outputDataProperty: DataProperty

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

  def requiredDataProperty: DataProperty = NotSpecifiedProperty()
  def requiredDataProperties: Seq[DataProperty] = Seq(requiredDataProperty)
  // TODO: We should get the output data properties of a leaf node from metadata.
  def outputDataProperty: DataProperty = NotSpecifiedProperty()
}

trait UnaryNode extends SharkPlan with trees.UnaryNode[SharkPlan] {
  self: Product =>

  def requiredDataProperty: DataProperty = NotSpecifiedProperty()
  def requiredDataProperties: Seq[DataProperty] = Seq(requiredDataProperty)

  def outputDataProperty: DataProperty = child.outputDataProperty
}

trait BinaryNode extends SharkPlan with trees.BinaryNode[SharkPlan] {
  self: Product =>

  def leftRequiredDataProperty: DataProperty = NotSpecifiedProperty()
  def rightRequiredDataProperty: DataProperty = NotSpecifiedProperty()
  def requiredDataProperties: Seq[DataProperty] =
    Seq(leftRequiredDataProperty, rightRequiredDataProperty)

  def outputDataProperty: DataProperty = NotSpecifiedProperty()
}

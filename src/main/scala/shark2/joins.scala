package catalyst
package shark2

import expressions._
import org.apache.spark.rdd.RDD

/* Implicits */
import org.apache.spark.SparkContext._

case class SparkEquiInnerJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SharkPlan,
    right: SharkPlan) extends BinaryNode {

  def output = left.output ++ right.output

  def execute() = {
    val leftWithKeys = generateKeys(leftKeys, left.execute())
    val rightWithKeys = generateKeys(rightKeys, right.execute())
    // Do the join.
    val joined = leftWithKeys.join(rightWithKeys)
    // Drop join keys and merge input tuples.
    joined.map { case (_, (leftTuple, rightTuple)) => leftTuple ++ rightTuple }
  }

  /**
   * Turns row into (joinKeys, row). Filters any rows where the any of the join keys is null, ensuring three-valued
   * logic for the equi-join conditions.
   */
  protected def generateKeys(keys: Seq[Expression], rdd: RDD[IndexedSeq[Any]]) =
    rdd.map {
      case row => (leftKeys.map(Evaluate(_, Vector(row))), row)
    }.filter {
      case (key: Seq[_], _) => !key.map(_ == null).reduceLeft(_ || _)
    }
}

case class CartesianProduct(left: SharkPlan, right: SharkPlan) extends BinaryNode {
  def output = left.output ++ right.output

  def execute() = left.execute().cartesian(right.execute()).map {
    case (l: IndexedSeq[Any], r: IndexedSeq[Any]) => l ++ r
  }
}
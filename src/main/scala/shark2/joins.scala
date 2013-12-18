package catalyst
package shark2

import org.apache.spark.rdd.RDD

import errors._
import expressions._

/* Implicits */
import org.apache.spark.SparkContext._

case class SparkEquiInnerJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SharkPlan,
    right: SharkPlan) extends BinaryNode {

  def output = left.output ++ right.output

  def execute() = attachTree(this, "execute") {
    val leftWithKeys = left.execute .map { row =>
      val joinKeys = leftKeys.map(Evaluate(_, Vector(row)))
      logger.debug(s"Generated left join keys ($leftKeys) => ($joinKeys) given row $row")
      (joinKeys, row)
    }

    val rightWithKeys = right.execute().map { row =>
      val joinKeys = rightKeys.map(Evaluate(_, Vector(Nil, row)))
      logger.debug(s"Generated right join keys ($rightKeys) => ($joinKeys) given row $row")
      (joinKeys, row)
    }

    // Do the join.
    val joined = filterNulls(leftWithKeys).join(filterNulls(rightWithKeys))
    // Drop join keys and merge input tuples.
    joined.map { case (_, (leftTuple, rightTuple)) => leftTuple ++ rightTuple }
  }

  /**
   * Filters any rows where the any of the join keys is null, ensuring three-valued
   * logic for the equi-join conditions.
   */
  protected def filterNulls(rdd: RDD[(Seq[Any], IndexedSeq[Any])]) =
    rdd.filter {
      case (key: Seq[_], _) => !key.map(_ == null).reduceLeft(_ || _)
    }
}

case class CartesianProduct(left: SharkPlan, right: SharkPlan) extends BinaryNode {
  def output = left.output ++ right.output

  def execute() = left.execute().cartesian(right.execute()).map {
    case (l: IndexedSeq[Any], r: IndexedSeq[Any]) => l ++ r
  }
}
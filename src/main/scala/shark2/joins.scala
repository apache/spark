package catalyst
package shark2

import org.apache.spark.rdd.RDD

import errors._
import expressions._
import plans._
import shark.SharkContext
import org.apache.spark.util.collection.BitSet
import scala.collection
import scala.collection.mutable

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

case class BroadcastNestedLoopJoin(streamed: SharkPlan, broadcast: SharkPlan, joinType: JoinType, condition: Option[Expression])
                                  (@transient sc: SharkContext) extends BinaryNode {
  override  def otherCopyArgs = sc :: Nil
  def output = left.output ++ right.output

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  def execute() = {
    val broadcastedRelation = sc.broadcast(broadcast.execute().collect().toIndexedSeq)

    val streamedPlusMatches = streamed.execute().map { streamedRow =>
      var i = 0
      val matchedRows = new mutable.ArrayBuffer[IndexedSeq[Any]]
      val includedBroadcastTuples =  new scala.collection.mutable.BitSet(broadcastedRelation.value.size)

      while(i < broadcastedRelation.value.size) {
        // TODO: One bitset per partition instead of per row.
        val broadcastedRow = broadcastedRelation.value(i)
        val includeRow = condition match {
          case None => true
          case Some(c) => Evaluate(c, Vector(streamedRow, broadcastedRow)).asInstanceOf[Boolean]
        }
        if(includeRow) {
          matchedRows += (streamedRow ++ broadcastedRow)
          includedBroadcastTuples += i
        }
        i += 1
      }
      val outputRows = if(matchedRows.size > 0)
        matchedRows
      else if(joinType == LeftOuter || joinType == FullOuter)
        Vector(streamedRow ++ Array.fill(right.output.size)(null))
      else
        Vector()
      (outputRows, includedBroadcastTuples)
    }

    val allIncludedBroadcastTupes = streamedPlusMatches.map(_._2).reduce(_ ++ _)
    val rightOuterMatches: Seq[IndexedSeq[Any]] =
      if(joinType == RightOuter || joinType == FullOuter)
        broadcastedRelation.value.zipWithIndex.filter {
          case (row, i) => !allIncludedBroadcastTupes.contains(i)
        }.map {
          case (row, _) => Vector.fill(left.output.size)(null) ++ row
        }
      else
        Vector()

    sc.union(streamedPlusMatches.flatMap(_._1), sc.makeRDD(rightOuterMatches))
  }
}
package catalyst
package execution

import catalyst.rules.Rule
import catalyst.errors._
import catalyst.expressions._
import catalyst.plans.physical._
import catalyst.types._

import org.apache.spark.{RangePartitioner, HashPartitioner}
import org.apache.spark.rdd.ShuffledRDD

case class Exchange(newPartitioning: Partitioning, child: SharkPlan)
    extends UnaryNode {

  override def outputPartitioning = newPartitioning
  def output = child.output

  def execute() = attachTree(this , "execute") {
    newPartitioning match {
      case HashPartitioning(expressions, width) =>
        // TODO: Eliminate redundant expressions in grouping key and value.
        val rdd = child.execute().map { row =>
          (buildRow(expressions.toSeq.map(Evaluate(_, Vector(row)))), row)
        }
        val part = new HashPartitioner(width)
        val shuffled = new ShuffledRDD[Row, Row, (Row, Row)](rdd, part)

        shuffled.map(_._2)

      case RangePartitioning(sortingExpressions, width) =>
        val directions = sortingExpressions.map(_.direction).toIndexedSeq
        val dataTypes = sortingExpressions.map(_.dataType).toIndexedSeq

        // TODO: MOVE THIS!
        class SortKey(val keyValues: IndexedSeq[Any])
          extends Ordered[SortKey]
          with Serializable {
          def compare(other: SortKey): Int = {
            var i = 0
            while (i < keyValues.size) {
              val left = keyValues(i)
              val right = other.keyValues(i)
              val curDirection = directions(i)
              val curDataType = dataTypes(i)

              logger.debug(s"Comparing $left, $right as $curDataType order $curDirection")
              // TODO: Use numeric here too?
              val comparison =
                if (left == null && right == null) {
                  0
                } else if (left == null) {
                  if (curDirection == Ascending) -1 else 1
                } else if (right == null) {
                  if (curDirection == Ascending) 1 else -1
                } else if (curDataType == IntegerType) {
                  if (curDirection == Ascending) {
                    left.asInstanceOf[Int] compare right.asInstanceOf[Int]
                  } else {
                    right.asInstanceOf[Int] compare left.asInstanceOf[Int]
                  }
                } else if (curDataType == DoubleType) {
                  if (curDirection == Ascending) {
                    left.asInstanceOf[Double] compare right.asInstanceOf[Double]
                  } else {
                    right.asInstanceOf[Double] compare left.asInstanceOf[Double]
                  }
                } else if (curDataType == LongType) {
                  if (curDirection == Ascending) {
                    left.asInstanceOf[Long] compare right.asInstanceOf[Long]
                  } else {
                    right.asInstanceOf[Long] compare left.asInstanceOf[Long]
                  }
                } else if (curDataType == StringType) {
                  if (curDirection == Ascending) {
                    left.asInstanceOf[String] compare right.asInstanceOf[String]
                  } else {
                    right.asInstanceOf[String] compare left.asInstanceOf[String]
                  }
                } else {
                  sys.error(s"Comparison not yet implemented for: $curDataType")
                }

              if (comparison != 0) return comparison
              i += 1
            }
            return 0
          }
        }

        val rdd = child.execute().map { row =>
          val input = Vector(row)
          val sortKey = new SortKey(
            sortingExpressions.map(s => Evaluate(s.child, input)).toIndexedSeq)

          (sortKey, row)
        }
        val part = new RangePartitioner(width, rdd, ascending = true)
        val shuffled = new ShuffledRDD[SortKey, Row, (SortKey, Row)](rdd, part)

        shuffled.map(_._2)
      case _ => sys.error("Not implemented")
    }
  }
}

object AddExchange extends Rule[SharkPlan] {
  // TODO: determine the number of partitions.
  val numPartitions = 8

  def apply(plan: SharkPlan): SharkPlan = plan.transformUp {
    case operator: SharkPlan =>
      def meetsRequirements =
        !operator.requiredChildDistribution.zip(operator.children).map {
          case (required, child) => !child.outputPartitioning.satisfies(required)
        }.exists(_ == false)

      // TODO ASUUMES TRANSITIVITY?
      def compatible =
        !operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .map {
            case Seq(a) => true
            case Seq(a,b) => a compatibleWith b
          }.exists(_ == false)


      if (false && meetsRequirements && compatible) {
        operator
      } else {
        val repartitionedChildren = operator.requiredChildDistribution.zip(operator.children).map {
          case (ClusteredDistribution(clustering), child) =>
            Exchange(HashPartitioning(clustering, 8), child)
          case (OrderedDistribution(ordering), child) =>
            Exchange(RangePartitioning(ordering, 8), child)
          case (UnknownDistribution, child) => child
          case (dist, _) => sys.error(s"Don't know how to ensure $dist")
        }
        operator.withNewChildren(repartitionedChildren)
      }
  }
}
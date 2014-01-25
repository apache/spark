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
        // TODO: ShuffledRDD should take an Ordering.
        implicit val ordering = new RowOrdering(sortingExpressions)

        val rdd = child.execute().map(r => (r,null))
        val part = new RangePartitioner(width, rdd, ascending = true)
        val shuffled = new ShuffledRDD[Row, Null, (Row, Null)](rdd, part)
        shuffled.map(_._1)
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
  }
}

/**
 * Ensures that the [[catalyst.plans.physical.Partitioning Partitioning]] of input data meets the
 * [[catalyst.plans.physical.Distribution Distribution]] requirements for each operator by inserting
 * [[Exchange]] Operators where required.
 */
object AddExchange extends Rule[SharkPlan] {
  // TODO: determine the number of partitions.
  val numPartitions = 8

  def apply(plan: SharkPlan): SharkPlan = plan.transformUp {
    case operator: SharkPlan =>
      def meetsRequirements =
        !operator.requiredChildDistribution.zip(operator.children).map {
          case (required, child) =>
            val valid = child.outputPartitioning.satisfies(required)
            logger.debug(
              s"${if (valid) "Valid" else "Invalid"} distribution," +
                s"required: $required current: ${child.outputPartitioning}")
            valid
        }.exists(_ == false)

      // TODO ASSUMES TRANSITIVITY?
      def compatible =
        !operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .map {
            case Seq(a) => true
            case Seq(a,b) => a compatibleWith b
          }.exists(_ == false)


      if (meetsRequirements && compatible) {
        operator
      } else {
        val repartitionedChildren = operator.requiredChildDistribution.zip(operator.children).map {
          case (ClusteredDistribution(clustering), child) =>
            Exchange(HashPartitioning(clustering, numPartitions), child)
          case (OrderedDistribution(ordering), child) =>
            Exchange(RangePartitioning(ordering, numPartitions), child)
          case (UnknownDistribution, child) => child
          case (dist, _) => sys.error(s"Don't know how to ensure $dist")
        }
        operator.withNewChildren(repartitionedChildren)
      }
  }
}
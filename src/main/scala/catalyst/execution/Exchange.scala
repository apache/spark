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
        import scala.math.Ordering.Implicits._
        implicit val ordering = new RowOrdering(sortingExpressions)

        val rdd = child.execute().map(r => (r,null))
        val part = new RangePartitioner(width, rdd, ascending = true)
        val shuffled = new ShuffledRDD[Row, Null, (Row, Null)](rdd, part)
        shuffled.map(_._1)
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
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
package catalyst
package plans
package physical

import expressions._
import types._

/**
 * Specifies how tuples that share common expressions will be distributed.  Distribution can
 * be used to refer to two distinct physical properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., [[Aggregate]]) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
 *
 *  TOOD(marmbrus): Given the way that Spark does partitioning, I think the order of the grouping
 *                  actually does matter, and thus our subset checking is probably not sufficient
 *                  to ensure correct colocation for joins.?
 *  TODO(marmbrus): Similarly I'm not sure that satisfies is a sufficient check to see if an sort
 *                  aggregation can be done.  Maybe we need two checks? Separate ordering from
 *                  partitioning?
 */
sealed trait Distribution {
}

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnknownDistribution extends Distribution

/**
 * Represents a distribution where a single operation can observe all tuples in the dataset.
 */
case object AllTuples extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering` [[Expression]]s will
 * be co-located. Based on the context, this can mean such tuples are either co-located in the same
 * partition or they will be contiguous within a single partition.
 */
case class ClusteredDistribution(clustering: Seq[Expression]) extends Distribution

/**
 * Represents data where tuples have been ordered according to the `ordering` [[Expression]]s.  This
 * is a strictly stronger guarantee than [[ClusteredDistribution]] as an ordering will ensure that
 * tuples that share the same value for the ordering expressions are contiguous and will never be
 * split across partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  def clustering = ordering.map(_.child).toSet
}

sealed abstract trait Partitioning {
  self: Product =>

  val width: Int

  /**
   * Returns true iff the guarantees made by this [[Distribution]] are sufficient to satisfy all
   * guarantees mandated by the `required` distribution.
   */
  def satisfies(required: Distribution): Boolean

  def compatibleWith(other: Partitioning): Boolean
}

case class UnknownPartitioning(width: Int) extends Partitioning {
  def satisfies(required: Distribution): Boolean = required match {
    case UnknownDistribution => true
    case _ => false
  }
  def compatibleWith(other: Partitioning): Boolean = other match {
    case UnknownPartitioning(_) => true
    case _ => false
  }
}

case object Unpartitioned extends Partitioning {
  val width = 1

  override def satisfies(required: Distribution): Boolean = true
  override def compatibleWith(other: Partitioning) = other match {
    case Unpartitioned => true
    case _ => false
  }
}

case object Broadcast extends Partitioning {
  val width = 1

  override def satisfies(required:Distribution): Boolean = true
  override def compatibleWith(other: Partitioning) = other match {
    case Unpartitioned => true
    case _ => false
  }
}

case class HashPartitioning(expressions: Seq[Expression], width: Int) extends Expression with Partitioning {
  def children = expressions.toSeq
  def references = expressions.flatMap(_.references).toSet
  def nullable = false
  def dataType = IntegerType

  lazy val clusteringSet = expressions.toSet

  def satisfies(required: Distribution): Boolean = required match {
    case UnknownDistribution => true
    case ClusteredDistribution(requiredClustering) =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }
  override def compatibleWith(other: Partitioning) = other match {
    case Broadcast => true
    case h: HashPartitioning if h == this => true
    case _ => false
  }
}

case class RangePartitioning(ordering: Seq[SortOrder], width: Int) extends Expression with Partitioning {
  def children = ordering.toSeq
  def references = ordering.flatMap(_.references).toSet
  def nullable = false
  def dataType = IntegerType

  lazy val clusteringSet = ordering.map(_.child).toSet

  def satisfies(required: Distribution): Boolean = required match {
    case UnknownDistribution => true
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    case ClusteredDistribution(requiredClustering) =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }

  def compatibleWith(other: Partitioning) = other match {
    case Broadcast => true
    case r: RangePartitioning if r == this => true
    case _ => false
  }
}
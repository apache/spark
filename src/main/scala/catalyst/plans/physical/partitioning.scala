package catalyst
package plans
package physical

import expressions._
import types._

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.  Distribution can be used to refer to two distinct physical
 * properties:
 *  - Inter-node partitioning of data: In this case the distribution describes how tuples are
 *    partitioned across physical machines in a cluster.  Knowing this property allows some
 *    operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 *  - Intra-partition ordering of data: In this case the distribution describes guarantees made
 *    about how tuples are distributed within a single partition.
 */
sealed trait Distribution

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnknownDistribution extends Distribution

/**
 * Represents a distribution where a single operation can observe all tuples in the dataset.
 */
case object AllTuples extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[catalyst.expressions.Expression Expressions]] will be co-located. Based on the context, this
 * can mean such tuples are either co-located in the same partition or they will be contiguous
 * within a single partition.
 */
case class ClusteredDistribution(clustering: Seq[Expression]) extends Distribution

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[catalyst.expressions.Expression Expressions]].  This is a strictly stronger guarantee than
 * [[ClusteredDistribution]] as an ordering will ensure that tuples that share the same value for
 * the ordering expressions are contiguous and will never be split across partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  def clustering = ordering.map(_.child).toSet
}

sealed trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val width: Int

  /**
   * Returns true iff the guarantees made by this [[Distribution]] are sufficient to satisfy all
   * guarantees mandated by the `required` distribution.
   */
  def satisfies(required: Distribution): Boolean

  /**
   * Returns true iff all distribution guarantees made by this partitioning can also be made
   * for the `other` specified partitioning.  For example, [[HashPartitioning]]s are only compatible
   * if the `width` of the two partitionings is the same.
   */
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

  override def satisfies(required: Distribution): Boolean = true

  override def compatibleWith(other: Partitioning) = other match {
    case Unpartitioned => true
    case _ => false
  }
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 */
case class HashPartitioning(expressions: Seq[Expression], width: Int)
  extends Expression
  with Partitioning {

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

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner the following
 * two conditions are guaranteed to hold:
 *  - All row where the expressions in `ordering` evaluate to the same values will be in the same
 *    partition.
 *  - Each partition will have a `min` and `max` row, relative to the given ordering.  All rows
 *    that are in between `min` and `max` in this `ordering` will reside in this partition.
 */
case class RangePartitioning(ordering: Seq[SortOrder], width: Int)
  extends Expression
  with Partitioning {

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
package catalyst
package execution

import expressions.{Expression, SortOrder}
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
abstract sealed class Distribution extends Expression {
  self: Product =>

  /**
   * Returns true iff the guarantees made by this [[Distribution]] are sufficient to satisfy all
   * guarantees mandated by the `required` distribution.
   */
  def satisfies(required: Distribution): Boolean
}

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnknownDistribution extends Distribution with trees.LeafNode[Expression] {
  def references = Set.empty
  def nullable = false
  def dataType = NullType

  def satisfies(required: Distribution): Boolean = required == UnknownDistribution
}

/**
 * Represents data where tuples that share the same values for the `clustering` [[Expression]]s will
 * be co-located. Based on the context, this can mean such tuples are either co-located in the same
 * partition or they will be contiguous within a single partition.
 */
case class ClusteredDistribution(clustering: Seq[Expression]) extends Distribution {
  def children = clustering.toSeq
  def references = clustering.flatMap(_.references).toSet
  def nullable = false
  def dataType = NullType

  def satisfies(required: Distribution): Boolean = required match {
    case UnknownDistribution => true
    // No clustering expressions means only one partition.
    case _ if clustering.isEmpty => true
    case ClusteredDistribution(requiredClustering) =>
      clustering.toSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }
}

/**
 * Represents data where tuples have been ordered according to the `ordering` [[Expression]]s.  This
 * is a strictly stronger guarantee than [[ClusteredDistribution]] as an ordering will ensure that
 * tuples that share the same value for the ordering expressions are contiguous and will never be
 * split across partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  def children = ordering
  def references = ordering.flatMap(_.references).toSet
  def nullable = false
  def dataType = NullType

  def clustering = ordering.map(_.child).toSet

  def satisfies(required: Distribution): Boolean = required match {
    case UnknownDistribution => true
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    case ClusteredDistribution(requiredClustering) =>
      clustering.subsetOf(requiredClustering.toSet)
    case _ => false
  }
}
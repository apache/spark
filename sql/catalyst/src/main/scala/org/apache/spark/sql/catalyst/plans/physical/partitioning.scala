/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions.{Unevaluable, Expression, SortOrder}
import org.apache.spark.sql.types.{DataType, IntegerType}

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
case object UnspecifiedDistribution extends Distribution

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 */
case object AllTuples extends Distribution

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located. Based on the context, this
 * can mean such tuples are either co-located in the same partition or they will be contiguous
 * within a single partition.
 */
case class ClusteredDistribution(clustering: Seq[Expression]) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]].  This is a strictly stronger guarantee than
 * [[ClusteredDistribution]] as an ordering will ensure that tuples that share the
 * same value for the ordering expressions are contiguous and will never be split across
 * partitions.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of a OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  // TODO: This is not really valid...
  def clustering: Set[Expression] = ordering.map(_.child).toSet
}

sealed trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val numPartitions: Int

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   */
  def satisfies(required: Distribution): Boolean

  /**
   * Returns true iff we can say that the partitioning scheme of this [[Partitioning]]
   * guarantees the same partitioning scheme described by `other`.
   */
  // TODO: Add an example once we have the `nullSafe` concept.
  def guarantees(other: Partitioning): Boolean
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning {
  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = false
}

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = true

  override def guarantees(other: Partitioning): Boolean = other match {
    case SinglePartition => true
    case _ => false
  }
}

case object BroadcastPartitioning extends Partitioning {
  val numPartitions = 1

  override def satisfies(required: Distribution): Boolean = true

  override def guarantees(other: Partitioning): Boolean = other match {
    case BroadcastPartitioning => true
    case _ => false
  }
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  lazy val clusteringSet = expressions.toSet

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case ClusteredDistribution(requiredClustering) =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: HashPartitioning =>
      this.clusteringSet == o.clusteringSet && this.numPartitions == o.numPartitions
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
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  private[this] lazy val clusteringSet = ordering.map(_.child).toSet

  override def satisfies(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case OrderedDistribution(requiredOrdering) =>
      val minSize = Seq(requiredOrdering.size, ordering.size).min
      requiredOrdering.take(minSize) == ordering.take(minSize)
    case ClusteredDistribution(requiredClustering) =>
      clusteringSet.subsetOf(requiredClustering.toSet)
    case _ => false
  }

  override def guarantees(other: Partitioning): Boolean = other match {
    case o: RangePartitioning => this == o
    case _ => false
  }
}

/**
 * A collection of [[Partitioning]]s that can be used to describe the partitioning
 * scheme of the output of a physical operator. It is usually used for an operator
 * that has multiple children. In this case, a [[Partitioning]] in this collection
 * describes how this operator's output is partitioned based on expressions from
 * a child. For example, for a Join operator on two tables `A` and `B`
 * with a join condition `A.key1 = B.key2`, assuming we use HashPartitioning schema,
 * there are two [[Partitioning]]s can be used to describe how the output of
 * this Join operator is partitioned, which are `HashPartitioning(A.key1)` and
 * `HashPartitioning(B.key2)`. It is also worth noting that `partitionings`
 * in this collection do not need to be equivalent, which is useful for
 * Outer Join operators.
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  /**
   * Returns true if any `partitioning` of this collection guarantees
   * the given [[Partitioning]].
   */
  override def guarantees(other: Partitioning): Boolean =
    partitionings.exists(_.guarantees(other))

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}

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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.
 *
 * Distribution here refers to inter-node partitioning of data. That is, it describes how tuples
 * are partitioned across physical machines in a cluster. Knowing this property allows some
 * operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 */
sealed trait Distribution {
  /**
   * The required number of partitions for this distribution. If it's None, then any number of
   * partitions is allowed for this distribution.
   */
  def requiredNumPartitions: Option[Int]

  /**
   * Creates a default partitioning for this distribution, which can satisfy this distribution while
   * matching the given number of partitions.
   */
  def createPartitioning(numPartitions: Int): Partitioning
}

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
case object UnspecifiedDistribution extends Distribution {
  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    throw new IllegalStateException("UnspecifiedDistribution does not have default partitioning.")
  }
}

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 */
case object AllTuples extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1, "The default partitioning of AllTuples can only have 1 partition.")
    SinglePartition
  }
}

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located in the same partition.
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(clustering, numPartitions)
  }
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]]. Its requirement is defined as the following:
 *   - Given any 2 adjacent partitions, all the rows of the second partition must be larger than or
 *     equal to any row in the first partition, according to the `ordering` expressions.
 *
 * In other words, this distribution requires the rows to be ordered across partitions, but not
 * necessarily within a partition.
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    RangePartitioning(ordering, numPartitions)
  }
}

/**
 * Represents data where tuples are broadcasted to every node. It is quite common that the
 * entire set of tuples is transformed into different data structure.
 */
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1,
      "The default partitioning of BroadcastDistribution can only have 1 partition.")
    BroadcastPartitioning(mode)
  }
}

/**
 * Describes how an operator's output is split across partitions. It has 2 major properties:
 *   1. number of partitions.
 *   2. if it can satisfy a given distribution.
 */
trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val numPartitions: Int

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   *
   * A [[Partitioning]] can never satisfy a [[Distribution]] if its `numPartitions` doesn't match
   * [[Distribution.requiredNumPartitions]].
   */
  final def satisfies(required: Distribution): Boolean = {
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
  }

  /**
   * Only return non-empty if the requirement can be used to repartition another side to match
   * the distribution of this side.
   */
  final def createRequirement(distribution: Distribution): Option[ShuffleSpec] =
    distribution match {
      case clustered: ClusteredDistribution =>
        createRequirement0(clustered)
      case _ =>
        throw new IllegalStateException(s"Unexpected distribution: " +
            s"${distribution.getClass.getSimpleName}")
    }

  /**
   * The actual method that defines whether this [[Partitioning]] can satisfy the given
   * [[Distribution]], after the `numPartitions` check.
   *
   * By default a [[Partitioning]] can satisfy [[UnspecifiedDistribution]], and [[AllTuples]] if
   * the [[Partitioning]] only have one partition. Implementations can also overwrite this method
   * with special logic.
   */
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case AllTuples => numPartitions == 1
    case _ => false
  }

  protected def createRequirement0(distribution: ClusteredDistribution): Option[ShuffleSpec] =
    None
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning

case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }

  override protected def createRequirement0(
      distribution: ClusteredDistribution): Option[ShuffleSpec] = Some(SinglePartitionShuffleSpec$)
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

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  override def createRequirement0(distribution: ClusteredDistribution): Option[ShuffleSpec] = {
    Some(HashShuffleSpec(this, distribution))
  }

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HashPartitioning = copy(expressions = newChildren)
}

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner, it guarantees:
 * Given any 2 adjacent partitions, all the rows of the second partition must be larger than any row
 * in the first partition, according to the `ordering` expressions.
 *
 * This is a strictly stronger guarantee than what `OrderedDistribution(ordering)` requires, as
 * there is no overlap between partitions.
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a previous
          //   partition which is larger than a [a2, b2] from the following partition, then there
          //   must be a [a1, b1 c1] larger than [a2, b2, c2], which violates RangePartitioning
          //   definition. So it's guaranteed that, any [a, b] in a previous partition must not be
          //   greater(i.e. smaller or equal to) than any [a, b] in the following partition. Thus
          //   `RangePartitioning(a, b, c)` satisfies `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, ordering.size).min
          requiredOrdering.take(minSize) == ordering.take(minSize)
        case ClusteredDistribution(requiredClustering, _) =>
          ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RangePartitioning =
    copy(ordering = newChildren.asInstanceOf[Seq[SortOrder]])
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
  override def satisfies0(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  override def createRequirement0(distribution: ClusteredDistribution): Option[ShuffleSpec] = {
    val eligible = partitionings
        .filter(_.satisfies(distribution))
        .flatMap(_.createRequirement(distribution))
    if (eligible.nonEmpty) {
      Some(ShuffleSpecCollection(eligible))
    } else {
      None
    }
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PartitioningCollection =
    super.legacyWithNewChildren(newChildren).asInstanceOf[PartitioningCollection]
}

/**
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }
}

/**
 * This specifies that, when a operator has more than one children where each of which has
 * its own partitioning and required distribution, the requirement for the other children to be
 * co-partitioned with the current child.
 */

/**
 * This is used in the scenario where an operator has multiple children (e.g., join), and each of
 * which has its own partitioning and required distribution. The spec is mainly used for two things:
 *
 *   1. Compare with specs from other children and check if they are compatible. When two specs
 *      are compatible, we can say their data are co-partitioned, and thus will allow Spark to
 *      eliminate shuffle in operators such as join.
 *   2. In case this spec is not compatible with another, create a partitioning that can be used to
 *      re-partition the other side.
 */
trait ShuffleSpec extends Ordered[ShuffleSpec] {
  /**
   * Returns true iff this spec is compatible with the other [[Partitioning]] and
   * clustering expressions (e.g., from [[ClusteredDistribution]]).
   *
   * A true return value means that the data partitioning from this spec can be seen as
   * co-partitioned with the `otherPartitioning`, and therefore no shuffle is required when
   * joining the two sides.
   */
  def isCompatibleWith(other: ShuffleSpec): Boolean

  /**
   * Create a partitioning that can be used to re-partitioned the other side whose required
   * distribution is specified via `clustering`.
   *
   * Note: this will only be called after `isCompatibleWith` returns true on the side where the
   * `clustering` is returned from.
   */
  def createPartitioning(clustering: Seq[Expression]): Partitioning
}

case object SinglePartitionShuffleSpec$ extends ShuffleSpec {
  override def isCompatibleWith(
      otherPartitioning: Partitioning,
      otherClustering: Seq[Expression]): Boolean = {
    otherPartitioning.numPartitions == 1
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning =
    SinglePartition

  override def compare(that: ShuffleSpec): Int = that match {
    case SinglePartitionShuffleSpec$ =>
      0
    case HashShuffleSpec(partitioning, _) =>
      1.compare(partitioning.numPartitions)
    case ShuffleSpecCollection(requirements) =>
      requirements.map(compare).min
  }
}

case class HashShuffleSpec(
    partitioning: HashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {
  private lazy val matchingIndexes = indexMap(distribution.clustering, partitioning.expressions)

  override def isCompatibleWith(
      otherPartitioning: Partitioning,
      otherClustering: Seq[Expression]): Boolean = otherPartitioning match {
    case SinglePartition =>
      partitioning.numPartitions == 1
    case HashPartitioning(expressions, _) =>
      // we need to check:
      //  1. both partitioning have the same number of expressions
      //  2. each corresponding expression in both partitioning is used in the same positions
      //     of the corresponding distribution.
      partitioning.expressions.length == expressions.length &&
          matchingIndexes == indexMap(otherClustering, expressions)
    case PartitioningCollection(partitionings) =>
      partitionings.exists(isCompatibleWith(_, otherClustering))
    case _ =>
      false
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = clustering
        .zipWithIndex
        .filter(x => matchingIndexes.keySet.contains(x._2))
        .map(_._1)
    HashPartitioning(exprs, partitioning.numPartitions)
  }

  override def compare(that: ShuffleSpec): Int = that match {
    case SinglePartitionShuffleSpec$ =>
      partitioning.numPartitions.compare(1)
    case HashShuffleSpec(otherPartitioning, _) =>
      if (partitioning.numPartitions != otherPartitioning.numPartitions) {
        partitioning.numPartitions.compare(otherPartitioning.numPartitions)
      } else {
        partitioning.expressions.length.compare(otherPartitioning.expressions.length)
      }
    case ShuffleSpecCollection(requirements) =>
      // pick the best requirement in the other collection
      requirements.map(compare).min
  }

  // For each expression in the `HashPartitioning` that has occurrences in
  // `ClusteredDistribution`, returns a mapping from its index in the partitioning to the
  // indexes where it appears in the distribution.
  // For instance, if `partitioning` is `[a, b]` and `distribution is `[a, a, b]`, then the
  // result mapping could be `{ 0 -> (0, 1), 1 -> (2) }`.
  private def indexMap(
      clustering: Seq[Expression],
      expressions: Seq[Expression]): mutable.Map[Int, mutable.BitSet] = {
    val result = mutable.Map.empty[Int, mutable.BitSet]
    val expressionToIndex = expressions.zipWithIndex.toMap
    clustering.zipWithIndex.foreach { case (distKey, distKeyIdx) =>
      expressionToIndex.find { case (partKey, _) => partKey.semanticEquals(distKey) }.forall {
        case (_, partIdx) =>
          result.getOrElseUpdate(partIdx, mutable.BitSet.empty).add(distKeyIdx)
      }
    }
    result
  }
}

case class ShuffleSpecCollection(requirements: Seq[ShuffleSpec]) extends ShuffleSpec {
  override def isCompatibleWith(
      otherPartitioning: Partitioning,
      otherClustering: Seq[Expression]): Boolean = {
    requirements.exists(_.isCompatibleWith(otherPartitioning, otherClustering))
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    // choose the best requirement (e.g., maximum shuffle parallelism, min shuffle data size, etc)
    // from the collection and use that to repartition the other sides
    requirements.max.createPartitioning(clustering)
  }

  override def compare(that: ShuffleSpec): Int = {
    requirements.map(_.compare(that)).max
  }
}

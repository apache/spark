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

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.internal.SQLConf
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
    throw SparkException.internalError(
      "UnspecifiedDistribution does not have default partitioning.")
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
 *
 * @param requireAllClusterKeys When true, `Partitioning` which satisfies this distribution,
 *                              must match all `clustering` expressions in the same ordering.
 * @param allowNullKeySpreading When true, the default partitioning may spread rows whose
 *                              clustering keys contain NULL values. This is a permission for
 *                              consumers that do not require NULL-key co-location; ordinary
 *                              [[HashPartitioning]] can still satisfy this distribution.
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    requireAllClusterKeys: Boolean = SQLConf.get.getConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION),
    requiredNumPartitions: Option[Int] = None,
    allowNullKeySpreading: Boolean = false) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    if (allowNullKeySpreading) {
      NullAwareHashPartitioning(clustering, numPartitions)
    } else {
      HashPartitioning(clustering, numPartitions)
    }
  }

  /**
   * Checks if `expressions` match all `clustering` expressions in the same ordering.
   *
   * `Partitioning` should call this to check its expressions when `requireAllClusterKeys`
   * is set to true.
   */
  def areAllClusterKeysMatched(expressions: Seq[Expression]): Boolean = {
    expressions.length == clustering.length &&
      expressions.zip(clustering).forall {
        case (l, r) => l.semanticEquals(r)
      }
  }
}

/**
 * Represents the requirement of distribution on the stateful operator in Structured Streaming.
 *
 * Each partition in stateful operator initializes state store(s), which are independent with state
 * store(s) in other partitions. Since it is not possible to repartition the data in state store,
 * Spark should make sure the physical partitioning of the stateful operator is unchanged across
 * Spark versions. Violation of this requirement may bring silent correctness issue.
 *
 * Since this distribution relies on [[HashPartitioning]] on the physical partitioning of the
 * stateful operator, only [[HashPartitioning]] (and HashPartitioning in
 * [[PartitioningCollection]]) can satisfy this distribution.
 * When `_requiredNumPartitions` is 1, [[SinglePartition]] is essentially same as
 * [[HashPartitioning]], so it can satisfy this distribution as well.
 *
 * NOTE: This is applied only to stream-stream join as of now. For other stateful operators, we
 * have been using ClusteredDistribution, which could construct the physical partitioning of the
 * state in different way (ClusteredDistribution requires relaxed condition and multiple
 * partitionings can satisfy the requirement.) We need to construct the way to fix this with
 * minimizing possibility to break the existing checkpoints.
 *
 * TODO(SPARK-38204): address the issue explained in above note.
 */
case class StatefulOpClusteredDistribution(
    expressions: Seq[Expression],
    _requiredNumPartitions: Int) extends Distribution {
  require(
    expressions != Nil,
    "The expressions for hash of a StatefulOpClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override val requiredNumPartitions: Option[Int] = Some(_requiredNumPartitions)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(_requiredNumPartitions == numPartitions,
      s"This StatefulOpClusteredDistribution requires ${_requiredNumPartitions} " +
        s"partitions, but the actual number of partitions is $numPartitions.")
    HashPartitioning(expressions, numPartitions)
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

  def areAllClusterKeysMatched(expressions: Seq[Expression]): Boolean = {
    expressions.length == ordering.length &&
      expressions.zip(ordering).forall {
        case (x, o) => x.semanticEquals(o.child)
      }
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
   * Creates a shuffle spec for this partitioning and its required distribution. The
   * spec is used in the scenario where an operator has multiple children (e.g., join), and is
   * used to decide whether this child is co-partitioned with others, therefore whether extra
   * shuffle shall be introduced.
   *
   * @param distribution the required clustered distribution for this partitioning
   */
  def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    throw SparkException.internalError(s"Unexpected partitioning: ${getClass.getSimpleName}")

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

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    SinglePartitionShuffleSpec
}

trait HashPartitioningLike extends Expression with Partitioning with Unevaluable {
  def expressions: Seq[Expression]

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: StatefulOpClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _, _) =>
          if (requireAllClusterKeys) {
            // Checks `HashPartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 *
 * Since [[StatefulOpClusteredDistribution]] relies on this partitioning and Spark requires
 * stateful operators to retain the same physical partitioning during the lifetime of the query
 * (including restart), the result of evaluation on `partitionIdExpression` must be unchanged
 * across Spark versions. Violation of this requirement may bring silent correctness issue.
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends HashPartitioningLike {

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    HashShuffleSpec(this, distribution)

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  def partitionIdExpression: Expression = Pmod(
    new CollationAwareMurmur3Hash(expressions), Literal(numPartitions)
  )

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HashPartitioning = copy(expressions = newChildren)
}

/**
 * Represents a hash partitioning for equi-join inputs where rows with a NULL join key do not need
 * to be co-located. Non-NULL join keys preserve the same partitioning contract as
 * [[HashPartitioning]], while rows with any NULL join key may be spread across partitions. As a
 * result, this partitioning intentionally does not satisfy a strict [[ClusteredDistribution]].
 */
case class NullAwareHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends HashPartitioningLike {

  override def satisfies0(required: Distribution): Boolean = {
    (required match {
      case UnspecifiedDistribution => true
      case AllTuples => numPartitions == 1
      case _ => false
    }) || {
      // Stateful operators require strict NULL-key co-location and therefore cannot consume
      // null-aware hash partitioning as a compatible clustered layout.
      required match {
        case c @ ClusteredDistribution(
            requiredClustering, requireAllClusterKeys, _, allowNullKeySpreading)
            if allowNullKeySpreading =>
          if (requireAllClusterKeys) {
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    NullAwareHashShuffleSpec(this, distribution)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): NullAwareHashPartitioning =
    copy(expressions = newChildren)
}

case class CoalescedBoundary(startReducerIndex: Int, endReducerIndex: Int)

/**
 * Represents a partitioning where partitions have been coalesced from a HashPartitioning into a
 * fewer number of partitions.
 */
case class CoalescedHashPartitioning(from: HashPartitioning, partitions: Seq[CoalescedBoundary])
  extends HashPartitioningLike {

  override def expressions: Seq[Expression] = from.expressions

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    CoalescedHashShuffleSpec(from.createShuffleSpec(distribution), partitions)

  override val numPartitions: Int = partitions.length

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CoalescedHashPartitioning =
    copy(from = from.copy(expressions = newChildren))
}

/**
 * Represents a null-aware hash partitioning whose reducer ranges have been coalesced into fewer
 * partitions. It preserves the same relaxed NULL-key co-location contract as
 * [[NullAwareHashPartitioning]].
 */
case class CoalescedNullAwareHashPartitioning(
    from: NullAwareHashPartitioning,
    partitions: Seq[CoalescedBoundary]) extends HashPartitioningLike {

  override def expressions: Seq[Expression] = from.expressions

  override def satisfies0(required: Distribution): Boolean = {
    (required match {
      case UnspecifiedDistribution => true
      case AllTuples => numPartitions == 1
      case _ => false
    }) || {
      required match {
        case c @ ClusteredDistribution(
            requiredClustering, requireAllClusterKeys, _, allowNullKeySpreading)
            if allowNullKeySpreading =>
          if (requireAllClusterKeys) {
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    CoalescedHashShuffleSpec(from.createShuffleSpec(distribution), partitions)

  override val numPartitions: Int = partitions.length

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CoalescedNullAwareHashPartitioning =
    copy(from = from.copy(expressions = newChildren))
}

/**
 * Represents a partitioning where rows are split across partitions based on transforms defined by
 * `expressions`.
 *
 * == Partition Keys ==
 * A partition key is a property of the partition it belongs to. `partitionKeys(i)` is the constant
 * value that `expressions` takes for every row in partition `i`, so there is one key per partition.
 * Keys may repeat while the partitioning is ungrouped. They become unique once
 * `GroupPartitionsExec` has grouped it.
 *
 * `EnsureRequirements` uses the keys for three things:
 *
 * - Deciding whether the partitioning meets a required distribution, for instance whether a
 *   group-by can run on these partitions as they are.
 * - Pairing up the partitions of two children that hold the same key, which is what lets a join
 *   read both sides without shuffling either.
 * - Laying another child out on these keys, when it cannot pair the partitions up.
 *   `ShuffleExchangeExec` builds a `KeyGroupedPartitioner` from this order, so the shuffled side
 *   lands in the same partitions as the side that declared the keys. This is also why a consumer
 *   must keep the order given here.
 *
 * == Grouping State ==
 * A KeyedPartitioning can be in two states:
 *
 * - '''Ungrouped''' (when `isGrouped == false`): `partitionKeys` contains duplicates, meaning
 *   multiple input partitions share the same key. This occurs when a data source has multiple
 *   splits for the same partition value.
 *
 * - '''Grouped''' (when `isGrouped == true`): `partitionKeys` contains only unique values, with
 *   each partition having a distinct key. A data source can report unique partition keys natively,
 *   or a `GroupPartitionsExec` can coalesce the partitions that share a key.
 *
 * == Distribution Satisfaction and Grouping ==
 * Besides the default `satisfies()`, `KeyedPartitioning` answers four questions. They differ in
 * what they let happen to the data before the distribution counts as met. Only `keysMaySatisfy()`
 * is asked from outside the class. The other three build it and `satisfies()` up.
 *
 * - `nonGroupedSatisfies()`: is the distribution met by the partitioning as it stands, with no node
 *   inserted? It is the default `Partitioning` implementation, so for a `ClusteredDistribution` it
 *   is always false.
 * - `keysSatisfy()`: do the partition keys match what the distribution asks for? Duplicate keys are
 *   ignored, so this asks about the key expressions alone. `satisfies()` is this plus `isGrouped`,
 *   and nothing more.
 * - `mayGroupToSatisfy()`: may `EnsureRequirements` insert a `GroupPartitionsExec` to meet the
 *   distribution? It is asked of non-grouped partitionings only, where such a node coalesces the
 *   duplicate keys. So the answer is `keysSatisfy()`, plus whether that coalescing is allowed.
 *   "Key Collapse" below says when it is not.
 * - `keysMaySatisfy()`: the same question for a partitioning that may already be grouped, which is
 *   what `EnsureRequirements` asks. It is `mayGroupToSatisfy()` for a non-grouped one, and
 *   `keysSatisfy()` for a grouped one, which has no duplicate keys left for the node to coalesce,
 *   and so nothing for the permission to govern.
 *
 * For `OrderedDistribution`, `GroupPartitionsExec` must also sort the partition keys to meet the
 * ordering requirement.
 *
 * == Key Collapse ==
 * Two things happen to partition keys, and only the first is a loss of granularity:
 *
 * - '''Key collapse''': a projection or a reduction maps two keys that were distinct onto the same
 *   new key. `[(1, 'a'), (1, 'b'), (2, 'c')]` projected onto the first position gives `[1, 1, 2]`,
 *   so three distinct keys became two. `isCollapsed` records this.
 * - '''Grouping''': `GroupPartitionsExec` physically combines the partitions that share a key.
 *   `[1, 1, 2]` becomes `[1, 2]`. `isGrouped` says the keys are unique, not how they became unique.
 *
 * Nothing downstream makes a partitioning finer, so once the flag is set, every partitioning
 * derived from that one inherits it.
 *
 * Whether a grouping needs `allowKeysSubsetOfPartitionKeys` follows from that difference:
 *
 * - '''Grouping without a collapse''' merges only partitions that already shared a key. A source
 *   reporting several splits per key produces those, and so does a union of children whose keys
 *   overlap. A partitioning that went through neither a projection nor a reduction is always in
 *   this case, so no opt-in is needed.
 * - '''Grouping after a collapse''' merges partitions that the finer-grained partitioning held
 *   apart. The two `1` partitions above came from the different keys `(1, 'a')` and `(1, 'b')`, so
 *   the merged partition holds more data than any partition the source declared. This is what the
 *   opt-in exists to gate.
 * - '''Grouping for an `OrderedDistribution`''' is not gated at all. `GroupPartitionsExec` pads
 *   that path out to the expected split counts instead of coalescing, so it merges nothing.
 *
 * A collapsed partitioning is therefore in one of two states, and only the first is gated:
 *
 * - '''Collapsed and ungrouped''': duplicate keys remain, so grouping them would merge partitions
 *   the source held apart. `mayGroupToSatisfy()` refuses a `ClusteredDistribution` unless the
 *   config is on.
 * - '''Collapsed and grouped''': the keys are already unique, so grouping merges nothing and
 *   `satisfies()` accepts a `ClusteredDistribution` whatever the config says. A partitioning
 *   reaches this state by being grouped with the config on, or by having its keys reduced onto a
 *   coarser transform.
 *
 * A collapsed partitioning is still reported as it is. The gate above refuses one route only, and
 * that route is meeting a `ClusteredDistribution` by grouping. It refuses nothing when the operator
 * asks for `UnspecifiedDistribution`, nothing once the keys are unique, and nothing on the path
 * that lays another child out on these keys, which never reads the flag. Which of those applies
 * depends on the required distribution, and the producer of the partitioning does not know it.
 * Reporting `UnknownPartitioning` would give up all of them, and make the plan shape depend on a
 * config.
 *
 * == Key Order ==
 * `partitionKeys` is usually in ascending order, but nothing guarantees it, so a consumer must not
 * assume it. A projection that drops key positions can leave the keys unsorted. So can a
 * `UnionExec` that concatenates its children's keys. The keys inside a `KeyedShuffleSpec` are not
 * sorted at all.
 *
 * Sorted keys are still worth having. Data sources produce them that way, and `GroupPartitionsExec`
 * sorts while grouping. When both sides of a storage-partitioned join report them,
 * `EnsureRequirements` can often match the two sides without inserting an additional
 * `GroupPartitionsExec`.
 *
 * == Example ==
 * Consider a data source with partition transform `[years(ts_col)]` and 4 input splits:
 *
 * '''Before GroupPartitionsExec''' (ungrouped):
 * {{{
 *   expressions:                                 [years(ts_col)]
 *   partitionKeys:                               [0, 1, 2, 2]  // partitions 2 and 3 share a key
 *   numPartitions:                               4
 *   isGrouped:                                   false
 *   keysSatisfy(ClusteredDistribution(...))      == true       // the keys match
 *   satisfies(ClusteredDistribution(...))        == false      // but they are not unique yet
 *   mayGroupToSatisfy(...)                       == true       // grouping would settle it
 * }}}
 *
 * '''After GroupPartitionsExec''' (grouped):
 * {{{
 *   expressions:                                [years(ts_col)]
 *   partitionKeys:                              [0, 1, 2]      // duplicates removed
 *   numPartitions:                              3
 *   isGrouped:                                  true
 *   keysSatisfy(ClusteredDistribution(...))     == true         // the keys still match
 *   satisfies(ClusteredDistribution(...))       == true         // and now they are unique
 * }}}
 *
 * @param expressions Partition transform expressions (e.g., `years(col)`, `bucket(10, col)`).
 * @param partitionKeys Partition keys wrapped in InternalRowComparableWrapper for efficient
 *                      comparison and grouping. One per partition. Typically in sorted order when
 *                      produced by a data source or `GroupPartitionsExec`, but this is not
 *                      guaranteed after projection. May contain duplicates when ungrouped.
 * @param isGrouped Whether partition keys are unique (no duplicates). Computed on first
 *                  creation, then preserved through copy operations to avoid recomputation.
 * @param isCollapsed Whether a projection or a reduction mapped keys that were distinct in the
 *                    partitioning this one was derived from onto the same key, so one key here can
 *                    stand for several of the original ones. Sticky. See "Key Collapse" above for
 *                    what it gates and how it travels.
 */
case class KeyedPartitioning(
    expressions: Seq[Expression],
    @transient partitionKeys: Seq[InternalRowComparableWrapper],
    isGrouped: Boolean,
    isCollapsed: Boolean) extends Expression with Partitioning with Unevaluable {
  override val numPartitions = partitionKeys.length

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): KeyedPartitioning =
    copy(expressions = newChildren)

  /** Need not be what the `partitionKeys` rows hold. See `keyDataTypes`. */
  @transient lazy val expressionDataTypes: Seq[DataType] = expressions.map(_.dataType)

  /**
   * The types the `partitionKeys` rows were built with. Anything reading those rows should take its
   * types from here. It is a driver-side value, since `partitionKeys` is `@transient`.
   *
   * They differ from the `expressionDataTypes` in two cases. A join that reduced both sides' keys
   * onto a key space no transform names leaves a marked expression whose type can be anything, see
   * `expressionsDescribeKeys`. A one-side reduce keeps them equal, because the expression the
   * partitioning then reports is the target transform and `EnsureRequirements` refuses a reducer
   * whose result type disagrees with it. `KeyedShuffleSpec.createPartitioning` is the other case.
   * It puts the other child's expressions over these keys with no reducer in sight, so a struct
   * field can be named differently on the two sides. With no key at all the expressions are all
   * there is, and there is no row to read or to place.
   *
   * The two cases can meet, and then the fallback is not truthful. A marked partitioning can end up
   * with no key, for instance when `v2BucketingPartitionFilterEnabled` intersects two sides that
   * hold disjoint keys, and this then reports the un-reduced transform's type. What it reports is a
   * fact about the key rows, so with no key row there is no fact, and a caller must not hold the
   * fallback against a real answer. The reduced-types comparison in `EnsureRequirements` leaves out
   * a marked side that has no key for that reason (SPARK-59176). An unmarked one still answers,
   * since its expressions describe the keys it would have had, and stays in the comparison.
   *
   * `ShuffleExchangeExec` is the one reader that stays on `expressionDataTypes`. It evaluates the
   * expressions to place the other child's rows, and it runs on executors, where this value is not
   * available. `expressionsDescribeKeys` is what keeps that site sound.
   *
   * Only the first key's types are read, and nothing enforces that the rest match. SPARK-59187 is
   * to carry the types on the partitioning instead of sampling a key row.
   */
  @transient lazy val keyDataTypes: Seq[DataType] =
    partitionKeys.headOption.map(_.dataTypes).getOrElse(expressionDataTypes)

  /** Driver-side, like the `keyDataTypes` it comes from. */
  @transient lazy val keyRowOrdering =
    KeyedPartitioning.groupedKeyRowOrdering(keyDataTypes)

  @transient lazy val keyOrdering = keyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)

  /**
   * Whether the partition expressions still describe the `partitionKeys`, i.e. whether evaluating
   * them on a row produces the key the row belongs under.
   *
   * They stop describing the keys when a join reduces both sides onto a common key space. The keys
   * become `r1(f1(x))` = `r2(f2(x))`, which is a third space no transform names, so the reduce
   * marks the expressions instead (`TransformExpression.reducedWith`). Reducing one side only keeps
   * them truthful, because the other side's transform describes the reduced keys exactly and
   * `KeyedShuffleSpec.reducersBothWays` reports that one.
   */
  def expressionsDescribeKeys: Boolean = !expressions.exists(TransformExpression.hasReducedKeys)

  /**
   * Projects this partitioning onto the key positions in `positions`, whose order becomes the order
   * of the projected expressions and key fields. The projected `isGrouped` and `isCollapsed` are
   * computed together, because dropping a position can map keys that were distinct here onto the
   * same key. Keeping every position changes no key, so both answers are inherited as they are,
   * with no pass over the keys. `GroupPartitionsExec` decides the same two answers from the key
   * groups it keeps, so it does not use this.
   */
  def project(positions: Seq[Int]): KeyedPartitioning = {
    if (positions == expressions.indices) {
      this
    } else {
      // One pass answers both questions, walking the projected keys alongside the keys they came
      // from. Two different source keys landing on one projected key is the collapse, and it is
      // also what makes the projected keys non-unique, so the walk stops at the first one. The
      // source keys are never hashed, only compared where a projected key repeats.
      val projectedKeys = projectKeys(positions)._2
      val sourceOf =
        mutable.HashMap.empty[InternalRowComparableWrapper, InternalRowComparableWrapper]
      var collapses = false
      val projectedIter = projectedKeys.iterator
      val sourceIter = partitionKeys.iterator
      while (projectedIter.hasNext && !collapses) {
        val projected = projectedIter.next()
        val source = sourceIter.next()
        sourceOf.put(projected, source) match {
          case Some(previous) if previous != source => collapses = true
          case _ =>
        }
      }
      copy(
        expressions = positions.map(expressions),
        partitionKeys = projectedKeys,
        isGrouped = !collapses && sourceOf.size == projectedKeys.length,
        isCollapsed = isCollapsed || collapses)
    }
  }

  def toGrouped: KeyedPartitioning = {
    // Unique keys need no dedup, only the sort.
    val uniqueKeys = if (isGrouped) partitionKeys else partitionKeys.distinct
    copy(partitionKeys = uniqueKeys.sorted(keyOrdering), isGrouped = true)
  }

  /**
   * Projects this partitioning's expressions by selecting only the specified positions.
   * Returns the projected expressions and their data types together with the projected keys.
   */
  def projectKeys(positions: Seq[Int]): (Seq[DataType], Seq[InternalRowComparableWrapper]) =
    KeyedPartitioning.projectKeys(partitionKeys, keyDataTypes, positions)

  /**
   * The number of partitions a `GroupPartitionsExec` projecting these keys to `positions` would
   * leave, which is the number of distinct projected keys. `positions` must be distinct and in
   * range, as it must be for `project` and `projectKeys`.
   *
   * A projection that keeps every position is the identity on the key values, so it needs no
   * projected rows at all. The rest allocate a row per partition and hash it with an uncached
   * `hashCode`, which makes this the expensive question to ask of a partitioning. A caller asking
   * it for several position sets should memoize on the set.
   */
  def numPartitionsProjectedOn(positions: Seq[Int]): Int = {
    if (positions.length < expressions.length) {
      projectKeys(positions)._2.distinct.size
    } else if (isGrouped) {
      numPartitions
    } else {
      partitionKeys.distinct.size
    }
  }

  /**
   * Reduces this partitioning's partition keys by applying the given reducers.
   * Returns the reduced keys and their data types.
   */
  def reduceKeys(
      reducers: Seq[Option[KeyReducer]]): (Seq[DataType], Seq[InternalRowComparableWrapper]) =
    KeyedPartitioning.reduceKeys(partitionKeys, keyDataTypes, reducers)

  override def satisfies0(required: Distribution): Boolean = {
    nonGroupedSatisfies(required) || (isGrouped && keysSatisfy(required))
  }

  /** The first of the four questions the class doc lists. */
  private def nonGroupedSatisfies(required: Distribution): Boolean = super.satisfies0(required)

  /** The second of the four questions the class doc lists. */
  private def keysSatisfy(required: Distribution): Boolean = {
    required match {
      case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _, _) =>
        if (requireAllClusterKeys) {
          // Checks whether this partitioning is partitioned on exactly same clustering keys of
          // `ClusteredDistribution`.
          c.areAllClusterKeysMatched(expressions)
        } else {
          // We'll need to find leaf attributes from the partition expressions first.
          val attributes = AttributeSet.fromAttributeSets(expressions.map(_.references))

          if (SQLConf.get.v2BucketingAllowKeysSubsetOfPartitionKeys) {
            // The operation keys may be a subset of the partition keys, so one partition expression
            // covering one of them is enough. Partitions can then still hold rows that share an
            // operation key. What makes the distribution true is the projection onto the covering
            // positions, plus the grouping that follows it. Given the single reference per
            // expression that `supportsExpressions` enforces, the test below is exactly
            // `KeyedShuffleSpec.keyPositions.exists(_.nonEmpty)`, where `createShuffleSpec` takes
            // its `joinKeyPositions` from. Consolidating the two is left to a follow-up.
            requiredClustering.exists(x => attributes.exists(_.semanticEquals(x))) &&
              expressions.forall(_.references.size == 1)
          } else {
            attributes.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        }

      case o @ OrderedDistribution(_) if SQLConf.get.v2BucketingAllowSorting =>
        // `EnsureRequirements` orders the key rows by the attributes the expressions are over, and
        // nothing makes a reducer order-preserving, so that ordering says nothing about reduced
        // keys. This is the local gate. A reduced position always carries a transform and an
        // `ORDER BY` cannot name one, so the match below already refuses every case a query can
        // reach. Do not read the first clause as redundant.
        expressionsDescribeKeys && o.areAllClusterKeysMatched(expressions)

      case _ =>
        false
    }
  }

  /**
   * The third of the four questions the class doc lists. Ask it only of a partitioning that is not
   * grouped, since a grouped one has nothing to coalesce and `keysMaySatisfy` covers both.
   */
  private[sql] def mayGroupToSatisfy(required: Distribution): Boolean = {
    val mayCoalesce = required match {
      case _: ClusteredDistribution =>
        !isCollapsed || SQLConf.get.v2BucketingAllowKeysSubsetOfPartitionKeys
      case _ => true
    }
    // The permission is the cheap half, so it is asked first.
    mayCoalesce && keysSatisfy(required)
  }

  /** The fourth of the four questions the class doc lists. */
  private[sql] def keysMaySatisfy(required: Distribution): Boolean = {
    if (isGrouped) keysSatisfy(required) else mayGroupToSatisfy(required)
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec = {
    val result = KeyedShuffleSpec(this, distribution)
    if (SQLConf.get.v2BucketingAllowKeysSubsetOfPartitionKeys) {
      // If allowing operation keys to be a subset of partition keys, create a new
      // `KeyedPartitioning` grouped on the operation keys, and use that as
      // the returned shuffle spec.
      val joinKeyPositions = result.keyPositions.map(_.nonEmpty).zipWithIndex.filter(_._1).map(_._2)
      // `toGrouped` sorts the keys the same way `GroupPartitionsExec` does (both sort with
      // `KeyedPartitioning.groupedKeyRowOrdering`). Otherwise, when only the keyed side is
      // grouped and the other side is re-shuffled using this spec, the two `KeyedPartitioning`s
      // carry the same keys in a different order and `PartitioningCollection.fromPartitionings`
      // rejects them.
      val projectedPartitioning = project(joinKeyPositions).toGrouped
      result.copy(partitioning = projectedPartitioning, joinKeyPositions = Some(joinKeyPositions))
    } else {
      result
    }
  }
}

object KeyedPartitioning {
  /**
   * Creates a KeyedPartitioning with isGrouped computed from the partition keys.
   * Use this when creating a new KeyedPartitioning from scratch (e.g., from a data source).
   */
  def apply(
      expressions: Seq[Expression],
      partitionKeys: Seq[InternalRow]): KeyedPartitioning = {
    val dataTypes = expressions.map(_.dataType)
    val comparableKeyWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(dataTypes)
    val comparablePartitionKeys = partitionKeys.map(comparableKeyWrapperFactory)
    val isGrouped = comparablePartitionKeys.distinct.size == comparablePartitionKeys.size
    // Built from scratch, so it is the layout everything else is compared against.
    new KeyedPartitioning(expressions, comparablePartitionKeys, isGrouped, isCollapsed = false)
  }

  /**
   * Concatenates partitionings that agree on their expressions, which is what a `UnionExec` does to
   * its children's partitions. The result reports one key per output partition, so its keys are the
   * children's keys in child order.
   *
   * Keys repeating across children is not a collapse. Only a child's own collapse carries over,
   * since such a key still stands for several finer-grained ones in the concatenation.
   */
  def concat(kps: Seq[KeyedPartitioning]): KeyedPartitioning = {
    val concatenatedKeys = kps.flatMap(_.partitionKeys)
    kps.head.copy(
      partitionKeys = concatenatedKeys,
      // A child that has duplicates of its own puts them in the concatenation too, which answers
      // this without walking the keys.
      isGrouped = kps.forall(_.isGrouped) &&
        concatenatedKeys.distinct.length == concatenatedKeys.length,
      isCollapsed = kps.exists(_.isCollapsed))
  }

  def supportsExpressions(expressions: Seq[Expression]): Boolean = {
    def isSupportedTransform(transform: TransformExpression): Boolean = {
      transform.children.size == 1 && isReference(transform.children.head)
    }

    @tailrec
    def isReference(e: Expression): Boolean = e match {
      case _: Attribute => true
      case g: GetStructField => isReference(g.child)
      case _ => false
    }

    expressions.forall {
      case t: TransformExpression if isSupportedTransform(t) => true
      case e: Expression if isReference(e) => true
      case _ => false
    }
  }

  /**
   * The ascending ordering in which grouped partition keys are laid out, for keys of the given
   * data types.
   *
   * This is a shared contract, not a convenience: with `allowKeysSubsetOfPartitionKeys`,
   * `createShuffleSpec` declares the keyed side's projected keys in this order (via `toGrouped`),
   * and the other side of the join may be shuffled onto exactly those keys, while the
   * `GroupPartitionsExec` inserted on the keyed side independently re-groups its partitions with
   * the same key positions and sorts them with this same ordering
   * (`GroupPartitionsExec.groupAndSortByKeys`). If the two sorts diverged, inner joins would fail
   * loudly at planning time -- `ShuffledJoin` wraps both sides' partitionings into a
   * `PartitioningCollection`, whose invariant requires equal partition keys -- but join types that
   * expose only one side's partitioning (e.g. LEFT OUTER) run nothing that compares the two
   * orders, and silently return wrong results.
   */
  def groupedKeyRowOrdering(dataTypes: Seq[DataType]): BaseOrdering =
    RowOrdering.createNaturalAscendingOrdering(dataTypes)

  /**
   * Projects a sequence of partition keys by selecting only the specified positions.
   */
  def projectKeys(
      keys: Seq[InternalRowComparableWrapper],
      dataTypes: Seq[DataType],
      positions: Seq[Int]): (Seq[DataType], Seq[InternalRowComparableWrapper]) = {
    val projectedDataTypes = positions.map(dataTypes)
    val comparableKeyWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(projectedDataTypes)
    // Indexed arrays rather than `Seq`s, because the loop below runs once per key and a key list is
    // as long as the number of splits the scan reported.
    val positionArray = positions.toArray
    val typeArray = projectedDataTypes.toArray
    val projectedKeys = keys.map { key =>
      val projectedKey = new Array[Any](positionArray.length)
      var i = 0
      while (i < positionArray.length) {
        projectedKey(i) = key.row.get(positionArray(i), typeArray(i))
        i += 1
      }
      comparableKeyWrapperFactory(new GenericInternalRow(projectedKey))
    }

    (projectedDataTypes, projectedKeys)
  }

  /**
   * Reduces a sequence of partition keys by applying reducers to each position.
   */
  def reduceKeys(
      keys: Seq[InternalRowComparableWrapper],
      dataTypes: Seq[DataType],
      reducers: Seq[Option[KeyReducer]]): (Seq[DataType], Seq[InternalRowComparableWrapper]) = {
    // The `Reducer[Any, Any]` match is erased, so it only ever checks for `Some`. Settling it per
    // position keeps it out of the key loop below, and gives the result types with it.
    val reducerArray =
      reducers.map(_.map(_.reducer.asInstanceOf[Reducer[Any, Any]]).orNull).toArray
    val reducedDataTypes = dataTypes.zip(reducerArray).map {
      case (t, reducer) => if (reducer == null) t else reducer.resultType()
    }
    val comparableKeyWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(reducedDataTypes)
    val typeArray = dataTypes.toArray
    // `InternalRow.toSeq(dataTypes)`, which the loop below replaces, asserted the row's arity once
    // per key. All the keys of a partitioning share an arity, so asserting on the first one keeps
    // the check. The loop also indexes `reducerArray` by the same bound, where the old `zip` would
    // have truncated to the shorter of the two, so that length is asserted with it.
    assert(reducerArray.length == typeArray.length)
    keys.headOption.foreach(k => assert(k.row.numFields == typeArray.length))
    val reducedKeys = keys.map { key =>
      val reducedKey = new Array[Any](typeArray.length)
      var i = 0
      while (i < typeArray.length) {
        val value = key.row.get(i, typeArray(i))
        val reducer = reducerArray(i)
        reducedKey(i) = if (reducer == null) value else reducer.reduce(value)
        i += 1
      }
      comparableKeyWrapperFactory(new GenericInternalRow(reducedKey))
    }

    (reducedDataTypes, reducedKeys)
  }
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
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _, _) =>
          val expressions = ordering.map(_.child)
          if (requireAllClusterKeys) {
            // Checks `RangePartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    RangeShuffleSpec(this.numPartitions, distribution)

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
 *
 * [[KeyedPartitioning]]s within a `PartitioningCollection` describe the same physical partitioning.
 * The constructor therefore requires all of them to share the same `partitionKeys` reference and
 * `isCollapsed` flag, and to have matching expression arity. Only their `expressions` differ.
 *
 * Use [[PartitioningCollection.fromPartitionings]] to build one from independently-computed
 * partitionings, such as a join's `outputPartitioning`. Its inputs need not agree on `isCollapsed`.
 * Each member carries the history of the child it came from, so one side can have collapsed its
 * keys in a projection while the other reports what its source declared. `fromPartitionings` ORs
 * the flags, including across nested collections, which is right because the members name one
 * shared layout, and one coarse member makes that layout coarse. Uniformity matters because
 * consumers read the flag off a single member. `satisfies0` and `EnsureRequirements` accept when
 * any one member satisfies the distribution, so a member that under-reported the collapse would let
 * the gate through.
 *
 * The key lists are required rather than reconciled. `fromPartitionings` interns the reference when
 * they are structurally equal, since members whose keys differ describe different layouts, which
 * one collection cannot stand for.
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  checkKeyedPartitioningInvariant()

  /**
   * First [[KeyedPartitioning]] reachable from this collection through direct members or nested
   * collections, if any. Since every collection validates the invariant on construction, this
   * single representative stands for all [[KeyedPartitioning]]s in the subtree. The invariant check
   * forces this lazy val during construction, so it is only recomputed after deserialization.
   */
  @transient private[physical] lazy val firstKeyedPartitioning: Option[KeyedPartitioning] =
    partitionings.view.flatMap(PartitioningCollection.representativeOf).headOption

  /**
   * Nested collections already enforced the invariant on their own construction, so comparing one
   * representative per direct member against [[firstKeyedPartitioning]] validates the whole
   * subtree without walking it. Keeping this check O(partitionings.size) matters: join
   * `outputPartitioning` builds these collections afresh on every call, and plans chaining many
   * same-key joins nest them linearly deep.
   *
   * `expressionsDescribeKeys` is deliberately not in that list. It would matter if the members
   * could disagree, since `satisfies0` is an `exists` over them and would then answer from an
   * unmarked one. They cannot disagree. The members share one key list, so they describe one
   * reduce, and a reduce that marks one side's expressions marks the other's in the same step,
   * while a one-side reduce marks neither.
   */
  private def checkKeyedPartitioningInvariant(): Unit = {
    firstKeyedPartitioning.foreach { first =>
      partitionings.iterator.flatMap(PartitioningCollection.representativeOf).foreach { rep =>
        if (rep ne first) {
          require(rep.expressions.length == first.expressions.length,
            "All KeyedPartitionings in a PartitioningCollection must have matching expression " +
              "arity")
          require(rep.partitionKeys eq first.partitionKeys,
            "All KeyedPartitionings in a PartitioningCollection must share the same " +
              "partitionKeys reference")
          require(rep.isCollapsed == first.isCollapsed,
            "All KeyedPartitionings in a PartitioningCollection must agree on isCollapsed")
        }
      }
    }
  }

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

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec = {
    val filtered = partitionings.filter(_.satisfies(distribution))
    ShuffleSpecCollection(filtered.map(_.createShuffleSpec(distribution)))
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PartitioningCollection =
    super.legacyWithNewChildren(newChildren).asInstanceOf[PartitioningCollection]
}

object PartitioningCollection {
  /**
   * One [[KeyedPartitioning]] standing for every one in this partitioning, if there is any. By the
   * invariant in the class doc, any of them describes the layout.
   */
  private[physical] def representativeOf(p: Partitioning): Option[KeyedPartitioning] = p match {
    case k: KeyedPartitioning => Some(k)
    case pc: PartitioningCollection => pc.firstKeyedPartitioning
    case _ => None
  }

  /**
   * Builds a [[PartitioningCollection]], unifying the `partitionKeys` reference across all
   * [[KeyedPartitioning]]s (including those in nested collections). Use this when combining
   * independently-computed partitionings (e.g. join `outputPartitioning`) where
   * `KeyedPartitioning.partitionKeys` are structurally equal but may not be reference-equal.
   *
   * Note: this can't be implemented with `TreeNode.transform`.
   */
  def fromPartitionings(partitionings: Seq[Partitioning]): PartitioningCollection = {
    // See the class doc for why the flag is normalized by OR rather than required to agree. One
    // representative per member is enough, because every collection agrees on the flag internally
    // by this same construction, and only a member that disagrees is rebuilt.
    val anyCollapsed = partitionings.exists(representativeOf(_).exists(_.isCollapsed))

    var canonicalKeys: Seq[InternalRowComparableWrapper] = null
    // A partitioning with no `KeyedPartitioning` in it has nothing to normalize, and one that
    // already agrees on both the keys and the flag is returned as it is. That is what keeps
    // repeated `outputPartitioning` computations over deeply nested collections (e.g. chains of
    // same-key joins) O(1) per level.
    def intern(p: Partitioning): Partitioning = representativeOf(p) match {
      case None => p
      case Some(representative) =>
        if (canonicalKeys == null) canonicalKeys = representative.partitionKeys
        if ((representative.partitionKeys eq canonicalKeys) &&
            representative.isCollapsed == anyCollapsed) {
          p
        } else {
          require(representative.partitionKeys == canonicalKeys,
            "All KeyedPartitionings in a PartitioningCollection must have equal partitionKeys")
          p match {
            case keyed: KeyedPartitioning =>
              keyed.copy(partitionKeys = canonicalKeys, isCollapsed = anyCollapsed)
            case pc: PartitioningCollection =>
              new PartitioningCollection(pc.partitionings.map(intern))
          }
        }
    }
    new PartitioningCollection(partitionings.map(intern))
  }

  /**
   * Flattens a partitioning into its leaf partitionings: a [[PartitioningCollection]] is
   * recursively replaced by its members, and any other partitioning yields itself.
   */
  def flatten(partitioning: Partitioning): Seq[Partitioning] = partitioning match {
    case PartitioningCollection(partitionings) => partitionings.flatMap(flatten)
    case other => other +: Nil
  }
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
 * This is used in the scenario where an operator has multiple children (e.g., join) and one or more
 * of which have their own requirement regarding whether its data can be considered as
 * co-partitioned from others. This offers APIs for:
 *
 *   - Comparing with specs from other children of the operator and check if they are compatible.
 *      When two specs are compatible, we can say their data are co-partitioned, and Spark will
 *      potentially be able to eliminate shuffle if necessary.
 *   - Creating a partitioning that can be used to re-partition another child, so that to make it
 *      having a compatible partitioning as this node.
 */

/**
 * Represents a partitioning where partition IDs are passed through directly from the
 * DirectShufflePartitionID expression. This partitioning scheme is used when users
 * want to directly control partition placement rather than using hash-based partitioning.
 *
 * This partitioning maps directly to the PartitionIdPassthrough RDD partitioner.
 */
case class ShufflePartitionIdPassThrough(
    expr: DirectShufflePartitionID,
    numPartitions: Int) extends Expression with Partitioning with Unevaluable {

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec = {
    ShufflePartitionIdPassThroughSpec(this, distribution)
  }

  def partitionIdExpression: Expression = Pmod(expr.child, Literal(numPartitions))

  def expressions: Seq[Expression] = expr :: Nil
  override def children: Seq[Expression] = expr :: Nil
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        // TODO(SPARK-53428): Support Direct Passthrough Partitioning in the Streaming Joins
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _, _) =>
          val partitioningExpressions = expr.child :: Nil
          if (requireAllClusterKeys) {
            c.areAllClusterKeysMatched(partitioningExpressions)
          } else {
            partitioningExpressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ShufflePartitionIdPassThrough =
    copy(expr = newChildren.head.asInstanceOf[DirectShufflePartitionID])
}

trait ShuffleSpec {
  /**
   * Returns the number of partitions of this shuffle spec
   */
  def numPartitions: Int

  /**
   * Returns true iff this spec is compatible with the provided shuffle spec.
   *
   * A true return value means that the data partitioning from this spec can be seen as
   * co-partitioned with the `other`, and therefore no shuffle is required when joining the two
   * sides.
   *
   * Note that Spark assumes this to be reflexive, symmetric and transitive.
   */
  def isCompatibleWith(other: ShuffleSpec): Boolean

  /**
   * Whether this shuffle spec can be used to create partitionings for the other children.
   */
  def canCreatePartitioning: Boolean

  /**
   * Creates a partitioning that can be used to re-partition the other side with the given
   * clustering expressions.
   *
   * This will only be called when:
   *  - [[isCompatibleWith]] returns false on the side where the `clustering` is from.
   */
  def createPartitioning(clustering: Seq[Expression]): Partitioning =
    throw SparkUnsupportedOperationException()
}

case object SinglePartitionShuffleSpec extends ShuffleSpec {
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    other.numPartitions == 1
  }

  override def canCreatePartitioning: Boolean = false

  override def createPartitioning(clustering: Seq[Expression]): Partitioning =
    SinglePartition

  override def numPartitions: Int = 1
}

case class RangeShuffleSpec(
    numPartitions: Int,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
  // data are co-partitioned for all the children, as range boundaries are randomly sampled. We
  // can't let `RangeShuffleSpec` to create a partitioning.
  override def canCreatePartitioning: Boolean = false

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec => numPartitions == 1
    case ShuffleSpecCollection(specs) => specs.exists(isCompatibleWith)
    // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
    // data are co-partitioned for all the children, as range boundaries are randomly sampled.
    case _ => false
  }
}

private object HashShuffleSpecCompatibility {
  def isCompatible(
      leftDistribution: ClusteredDistribution,
      leftNumPartitions: Int,
      leftExpressions: Seq[Expression],
      leftHashKeyPositions: Seq[mutable.BitSet],
      rightDistribution: ClusteredDistribution,
      rightNumPartitions: Int,
      rightExpressions: Seq[Expression],
      rightHashKeyPositions: Seq[mutable.BitSet]): Boolean = {
    leftDistribution.clustering.length == rightDistribution.clustering.length &&
    leftNumPartitions == rightNumPartitions &&
    leftExpressions.length == rightExpressions.length &&
    leftHashKeyPositions.zip(rightHashKeyPositions).forall { case (left, right) =>
      left.intersect(right).nonEmpty
    }
  }
}

case class HashShuffleSpec(
    partitioning: HashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the hash partition key to the cluster
   * keys. For instance, if cluster keys are [a, b, b] and hash partition keys are [a, b], the
   * result will be [(0), (1, 2)].
   *
   * This is useful to check compatibility between two `HashShuffleSpec`s. If the cluster keys are
   * [a, b, b] and [x, y, z] for the two join children, and the hash partition keys are
   * [a, b] and [x, z], they are compatible. With the positions, we can do the compatibility check
   * by looking at if the positions of hash partition keys from two sides have overlapping.
   */
  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherHashSpec @ HashShuffleSpec(otherPartitioning, otherDistribution) =>
      // we need to check:
      //  1. both distributions have the same number of clustering expressions
      //  2. both partitioning have the same number of partitions
      //  3. both partitioning have the same number of expressions
      //  4. each pair of partitioning expression from both sides has overlapping positions in their
      //     corresponding distributions.
      HashShuffleSpecCompatibility.isCompatible(
        distribution,
        partitioning.numPartitions,
        partitioning.expressions,
        hashKeyPositions,
        otherDistribution,
        otherPartitioning.numPartitions,
        otherPartitioning.expressions,
        otherHashSpec.hashKeyPositions)
    case otherNullAwareSpec @ NullAwareHashShuffleSpec(otherPartitioning, otherDistribution)
        if distribution.allowNullKeySpreading && otherDistribution.allowNullKeySpreading =>
      HashShuffleSpecCompatibility.isCompatible(
        distribution,
        partitioning.numPartitions,
        partitioning.expressions,
        hashKeyPositions,
        otherDistribution,
        otherPartitioning.numPartitions,
        otherPartitioning.expressions,
        otherNullAwareSpec.hashKeyPositions)
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = {
    // To avoid potential data skew, we don't allow `HashShuffleSpec` to create partitioning if
    // the hash partition keys are not the full join keys (the cluster keys). Then the planner
    // will add shuffles with the default partitioning of `ClusteredDistribution`, which uses all
    // the join keys.
    if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
      distribution.areAllClusterKeysMatched(partitioning.expressions)
    } else {
      true
    }
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    if (distribution.allowNullKeySpreading) {
      NullAwareHashPartitioning(exprs, partitioning.numPartitions)
    } else {
      HashPartitioning(exprs, partitioning.numPartitions)
    }
  }

  override def numPartitions: Int = partitioning.numPartitions
}

/**
 * Shuffle specification for [[NullAwareHashPartitioning]]. It is compatible only with shuffle
 * layouts whose distributions explicitly allow NULL-key spreading.
 */
case class NullAwareHashShuffleSpec(
    partitioning: NullAwareHashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherSpec @ NullAwareHashShuffleSpec(otherPartitioning, otherDistribution) =>
      HashShuffleSpecCompatibility.isCompatible(
        distribution,
        partitioning.numPartitions,
        partitioning.expressions,
        hashKeyPositions,
        otherDistribution,
        otherPartitioning.numPartitions,
        otherPartitioning.expressions,
        otherSpec.hashKeyPositions)
    case otherHashSpec @ HashShuffleSpec(otherPartitioning, otherDistribution)
        if distribution.allowNullKeySpreading && otherDistribution.allowNullKeySpreading =>
      HashShuffleSpecCompatibility.isCompatible(
        distribution,
        partitioning.numPartitions,
        partitioning.expressions,
        hashKeyPositions,
        otherDistribution,
        otherPartitioning.numPartitions,
        otherPartitioning.expressions,
        otherHashSpec.hashKeyPositions)
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = {
    if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
      distribution.areAllClusterKeysMatched(partitioning.expressions)
    } else {
      true
    }
  }

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    NullAwareHashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions
}

case class CoalescedHashShuffleSpec(
    from: ShuffleSpec,
    partitions: Seq[CoalescedBoundary]) extends ShuffleSpec {

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      numPartitions == 1
    case CoalescedHashShuffleSpec(otherParent, otherPartitions) =>
      partitions == otherPartitions && from.isCompatibleWith(otherParent)
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = false

  override def numPartitions: Int = partitions.length
}

/**
 * A [[Reducer]] paired with the reduced partition expression it produces.
 *
 * When a key-grouped partitioning is reduced onto another partitioning's key space, the original
 * partition expressions no longer describe the reduced keys (their data type and their value are
 * those of the target key space). This pair carries both the reducer and the expression the
 * reduced keys correspond to, so the output partitioning can report an expression that either
 * describes the keys or says that it does not.
 *
 * @param reducer reducer that maps this side's partition key values onto the other side's key space
 * @param reducedExpression the expression the reduced keys correspond to. When only this side
 *                          reduces, that is the other side's transform and its data type matches
 *                          the reduced keys. When both sides reduce, no transform describes the
 *                          keys, and this is this side's own expression marked with the pairing
 *                          that reduced it (`TransformExpression.reducedWith`). Stored as-is from
 *                          the expression pair that produced the reducer; the only structural
 *                          consumer, `GroupPartitionsExec`, re-targets it at each reported
 *                          `KeyedPartitioning`'s own key attribute in `outputPartitioning` and
 *                          normalizes it positionally in `doCanonicalize`, so the attribute it
 *                          carries here is not load-bearing.
 */
case class KeyReducer(reducer: Reducer[_, _], reducedExpression: TransformExpression)

/**
 * A [[Reducer]] that reduces an identity transform onto a partition transform: it applies the
 * given partition transform to the raw value of the identity transform's key. For instance, an
 * `identity(id)` side joined to a `bucket(4, id)` side reduces with
 * `IdentityReducer(bucket(4, id))`, which maps each raw id value to its `bucket(4, id)` value.
 *
 * It is a case class so that structurally identical reducers compare by value, and plans holding
 * them stay canonicalization-equal.
 *
 * @param transform the partition transform, re-targeted at the identity side's key attribute
 */
case class IdentityReducer(transform: TransformExpression) extends Reducer[Any, Any] {
  // `transform` has a single leaf attribute (`KeyedPartitioning.supportsExpressions`), which is
  // bound to ordinal 0 of the single-value row `reduce` evaluates it against.
  @transient private lazy val bound: Expression =
    BindReferences.bindReference(transform, AttributeSeq(transform.references.toSeq))

  override def reduce(v: Any): Any = bound.eval(new GenericInternalRow(Array[Any](v)))

  override def resultType(): DataType = transform.dataType

  override def displayName(): String = transform.toString
}

/**
 * [[ShuffleSpec]] created by [[KeyedPartitioning]].
 *
 * @param partitioning key grouped partitioning
 * @param distribution distribution
 * @param joinKeyPositions position of join keys among cluster keys.
 *                         This is set if joining on a subset of cluster keys is allowed.
 */
case class KeyedShuffleSpec(
    partitioning: KeyedPartitioning,
    distribution: ClusteredDistribution,
    joinKeyPositions: Option[Seq[Int]] = None) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the partition expression to the cluster
   * keys. For instance, if cluster keys are [a, b, b] and partition expressions are
   * [bucket(4, a), years(b)], the result will be [(0), (1, 2)].
   *
   * Note that we only allow each partition expression to contain a single partition key.
   * Therefore the mapping here is very similar to that from `HashShuffleSpec`.
   */
  lazy val keyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map { e =>
      val refs = e.references
      assert(refs.size == 1, s"Expected exactly one child from $e, but found ${refs.size}")
      distKeyToPos.getOrElse(refs.head.canonicalized, mutable.BitSet.empty)
    }
  }

  override def numPartitions: Int = partitioning.numPartitions

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    // Here we check:
    //  1. both distributions have the same number of clustering keys
    //  2. both partitioning have the same number of partitions
    //  3. partition expressions from both sides are compatible, which means:
    //    3.1 both sides have the same number of partition expressions
    //    3.2 for each pair of partition expressions at the same index, the corresponding
    //        partition keys must share overlapping positions in their respective clustering keys.
    //    3.3 each pair of partition expressions at the same index must share compatible
    //        transform functions.
    //  4. the partition values from both sides are following the same order.
    case otherSpec @ KeyedShuffleSpec(otherPartitioning, otherDistribution, _) =>
      distribution.clustering.length == otherDistribution.clustering.length &&
        numPartitions == other.numPartitions && areKeysCompatible(otherSpec) &&
          partitioning.partitionKeys == otherPartitioning.partitionKeys
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ => false
  }

  // Whether the partition keys (i.e., partition expressions) are compatible between this and the
  // `other` spec.
  def areKeysCompatible(other: KeyedShuffleSpec): Boolean = {
    val expressions = partitioning.expressions
    val otherExpressions = other.partitioning.expressions

    expressions.length == otherExpressions.length && {
      val otherKeyPositions = other.keyPositions
      keyPositions.zip(otherKeyPositions).forall { case (left, right) =>
        left.intersect(right).nonEmpty
      }
    } && expressions.zip(otherExpressions).forall {
      case (l, r) => isExpressionCompatible(l, r)
    }
  }

  private def isExpressionCompatible(left: Expression, right: Expression): Boolean = {
    if (TransformExpression.hasReducedKeys(left) || TransformExpression.hasReducedKeys(right)) {
      // Reduced keys are in a key space that neither transform names, so comparing the transforms
      // says nothing about whether the two sides are laid out the same way. The pair that was
      // reduced together is laid out the same way, since its two sides came out of one reduce onto
      // one key space. Anything else has to shuffle. That includes a pair that reduced onto the
      // same space through a different pairing, which nothing here can tell apart, and an identity
      // side, which holds raw values.
      (left, right) match {
        case (l: TransformExpression, r: TransformExpression) => l.hasSameReducedKeys(r)
        case _ => false
      }
    } else {
      (left, right) match {
        case (_: LeafExpression, _: LeafExpression) => true
        case (left: TransformExpression, right: TransformExpression) =>
          if (canReduceKeys) left.isCompatible(right) else left.isSameFunction(right)
        case (_: AttributeReference, _: TransformExpression) |
             (_: TransformExpression, _: AttributeReference) => canReduceKeys
        case _ => false
      }
    }
  }

  /**
   * Whether a join may reduce one or both sides' partition keys onto a common key space, which is
   * what lets two different transforms be compatible in the first place.
   */
  private def canReduceKeys: Boolean = {
    val conf = SQLConf.get
    conf.v2BucketingPushPartValuesEnabled &&
      !conf.v2BucketingPartiallyClusteredDistributionEnabled &&
      conf.v2BucketingAllowCompatibleTransforms
  }

  /**
   * Compute the reducers for both sides of a join between this shuffle spec and `other`, in a
   * single pass over the two sides' partition expressions. A pair's reducer lookups
   * (`TransformExpression.reducers`) are shared between the two directions: the reverse lookup a
   * direction needs to detect a single-side reduce is exactly the other direction's reducer, so
   * this materializes each catalog `Reducer` once per direction.
   * <p>
   * A [[Reducer]] exists for a partition expression of one side if it is 'reducible' on the
   * corresponding partition expression of the other side. If a side's value is returned, there
   * must be one entry per partition expression of that side. A None entry indicates that the
   * particular partition expression is not reducible on the corresponding expression.
   * <p>
   * Returning none for a side indicates that none of its partition expressions can be reduced on
   * the corresponding expression of the other side.
   *
   * @param other other key-grouped shuffle spec
   * @return the reducers for this side and for `other` w.r.t. each other
   */
  def reducersBothWays(other: KeyedShuffleSpec)
      : (Option[Seq[Option[KeyReducer]]], Option[Seq[Option[KeyReducer]]]) = {
    val results: Seq[(Option[KeyReducer], Option[KeyReducer])] =
      partitioning.expressions.zip(other.partitioning.expressions).map {
      // Keys an earlier join already reduced live in a key space that neither expression
      // describes, so a reducer derived from those expressions would be applied to values it was
      // not built for. `areKeysCompatible` admits only the pair that was reduced together, and
      // that pair is already in one key space, so there is nothing left to reduce.
      case (e1, e2)
          if TransformExpression.hasReducedKeys(e1) || TransformExpression.hasReducedKeys(e2) =>
        (None, None)

      case (e1: TransformExpression, e2: TransformExpression) =>
        val thisReducer = e1.reducers(e2)
        val otherReducer = e2.reducers(e1)

        val thisResult = thisReducer.map { reducer =>
          if (otherReducer.isEmpty) {
            // Only this side reduces. The reducer contract is r(f1(x)) = f2(x) where "=" matches
            // both value and data type, so the reduced keys equal the target transform applied to
            // this side's child. Report the target transform (re-targeted at the reporting
            // partitioning's key attribute by `GroupPartitionsExec`) instead of the un-reduced
            // `e1`, whose type can differ from the reduced keys. A connector that violates the
            // contract with a reducer of a different result type fails the reduced-types check in
            // `EnsureRequirements`, since the other side's keys are typed by the target transform.
            KeyReducer(reducer, e2)
          } else {
            // Both sides reduce: the reduced keys are r1(f1(x)) = r2(f2(x)), which no single
            // transform describes. Report `e1` and mark it with the pairing that produced that key
            // space, so that whatever needs the keys' values or their type refuses it, while the
            // partitionings that share the space are still recognised.
            KeyReducer(reducer, e1.reducedTogetherWith(e2))
          }
        }

        val otherResult = otherReducer.map { reducer =>
          if (thisReducer.isEmpty) {
            // Only the other side reduces; symmetric to the single-side case above.
            KeyReducer(reducer, e1)
          } else {
            // Both sides reduce; symmetric to the both-sides case above.
            KeyReducer(reducer, e2.reducedTogetherWith(e1))
          }
        }

        (thisResult, otherResult)

      // Identity transform on this side, arbitrary transform on the other side: create a reducer
      // that applies the other's transform to the raw identity values. Each partition expression
      // is guaranteed to have exactly one leaf child (asserted in keyPositions), which
      // `IdentityReducer` binds to ordinal 0.
      case (a: AttributeReference, t: TransformExpression) =>
        (Some(KeyReducer(IdentityReducer(t.withReference(a)), t)), None)

      // Symmetric: identity transform on the other side.
      case (t: TransformExpression, a: AttributeReference) =>
        (None, Some(KeyReducer(IdentityReducer(t.withReference(a)), t)))

      case (_, _) => (None, None)
    }

    // optimize to not return a value, if none of the partition expressions are reducible
    val thisReducers = results.map(_._1)
    val otherReducers = results.map(_._2)
    val thisResult = if (thisReducers.forall(_.isEmpty)) None else Some(thisReducers)
    val otherResult = if (otherReducers.forall(_.isEmpty)) None else Some(otherReducers)
    (thisResult, otherResult)
  }

  override def canCreatePartitioning: Boolean =
    SQLConf.get.v2BucketingShuffleEnabled &&
      !SQLConf.get.v2BucketingPartiallyClusteredDistributionEnabled &&
      // Shuffling another child onto these partition keys assigns each key a single partition, so
      // an ungrouped partitioning cannot be reproduced: its duplicate keys live in more than one
      // partition. This is the local gate. Such a spec is not reachable today for a non-local
      // reason -- `EnsureRequirements` wraps a child whose `KeyedPartitioning` does not satisfy the
      // distribution in a `GroupPartitionsExec` before it builds any spec -- so do not read this
      // clause as redundant.
      partitioning.isGrouped &&
      partitioning.expressions.forall { e =>
        e.isInstanceOf[AttributeReference] || e.isInstanceOf[TransformExpression]
      } &&
      // Shuffling another child onto these keys evaluates the partition expressions per row to
      // decide where each row goes, and reduced keys are not what those expressions compute.
      partitioning.expressionsDescribeKeys

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    assert(clustering.size == distribution.clustering.size,
      "Required distributions of join legs should be the same size.")

    val newExpressions = partitioning.expressions.zip(keyPositions).map {
      case (te: TransformExpression, positionSet) =>
        te.copy(children = te.children.map(_ => clustering(positionSet.head)))
      case (_, positionSet) => clustering(positionSet.head)
    }
    // The shuffled side is laid out on this side's partition keys, so it inherits the flag. That
    // is conservative rather than strictly true, and it can only ever add a shuffle: a later
    // grouping of the shared key set carries the collapsed side's risk.
    partitioning.copy(expressions = newExpressions)
  }
}

case class ShufflePartitionIdPassThroughSpec(
    partitioning: ShufflePartitionIdPassThrough,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the partition key to the cluster
   * keys. Similar to HashShuffleSpec, this maps the partitioning expression to positions
   * in the distribution clustering keys.
   */
  lazy val keyPositions: mutable.BitSet = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    distKeyToPos.getOrElse(partitioning.expr.child.canonicalized, mutable.BitSet.empty)
  }

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherPassThroughSpec @ ShufflePartitionIdPassThroughSpec(
        otherPartitioning, otherDistribution) =>
      // As ShufflePartitionIdPassThrough only allows a single expression
      // as the partitioning expression, we check compatibility as follows:
      // 1. Same number of clustering expressions
      // 2. Same number of partitions
      // 3. each partitioning expression from both sides has overlapping positions in their
      //    corresponding distributions.
      distribution.clustering.length == otherDistribution.clustering.length &&
      partitioning.numPartitions == otherPartitioning.numPartitions && {
        val otherKeyPositions = otherPassThroughSpec.keyPositions
        keyPositions.intersect(otherKeyPositions).nonEmpty
      }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  // We don't support creating partitioning for ShufflePartitionIdPassThrough.
  override def canCreatePartitioning: Boolean = false

  override def numPartitions: Int = partitioning.numPartitions
}

case class ShuffleSpecCollection(specs: Seq[ShuffleSpec]) extends ShuffleSpec {
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    specs.exists(_.isCompatibleWith(other))
  }

  override def canCreatePartitioning: Boolean =
    specs.forall(_.canCreatePartitioning)

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    // as we only consider # of partitions as the cost now, it doesn't matter which one we choose
    // since they should all have the same # of partitions.
    require(specs.map(_.numPartitions).toSet.size == 1, "expected all specs in the collection " +
      "to have the same number of partitions")
    specs.head.createPartitioning(clustering)
  }

  override def numPartitions: Int = {
    require(specs.nonEmpty, "expected specs to be non-empty")
    specs.head.numPartitions
  }
}

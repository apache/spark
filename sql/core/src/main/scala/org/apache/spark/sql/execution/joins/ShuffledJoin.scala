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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Coalesce, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSingle, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, CoalescedNullAwareHashPartitioning, Distribution, HashPartitioningLike, KeyedPartitioning, NullAwareHashPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.trees.MultiTransform
import org.apache.spark.sql.internal.SQLConf

/**
 * Holds common logic for join operators by shuffling two child relations
 * using the join keys.
 */
trait ShuffledJoin extends JoinCodegenSupport {
  def isSkewJoin: Boolean

  private val fullOuterExpandLimit =
    conf.getConf(SQLConf.FULL_OUTER_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT)

  private lazy val canSpreadNullJoinKeys: Boolean = {
    // Only NULL keys on the preserved side of an outer or left anti join can create this skew:
    // they must be emitted, but cannot satisfy ordinary equi-join predicates. Non-preserved
    // NULL-keyed rows are filtered out by `=` and never emitted, so their reducer placement does
    // not matter here.
    //
    // Null-safe equality usually rewrites to non-null shuffle keys. The NullType corner can still
    // produce NULL shuffle keys, but shuffled join execution already treats those rows as
    // unmatched, so spreading them does not change the result.
    val preservedSideHasNullableKeys = joinType match {
      case LeftOuter | LeftAnti => leftKeys.exists(_.nullable)
      case RightOuter => rightKeys.exists(_.nullable)
      case FullOuter => leftKeys.exists(_.nullable) || rightKeys.exists(_.nullable)
      case _ => false
    }
    conf.getConf(SQLConf.SHUFFLE_SPREAD_NULL_JOIN_KEYS_ENABLED) &&
      preservedSideHasNullableKeys
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `ClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else if (canSpreadNullJoinKeys) {
      ClusteredDistribution(leftKeys, allowNullKeySpreading = true) ::
        ClusteredDistribution(rightKeys, allowNullKeySpreading = true) :: Nil
    } else {
      ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      // Every `KeyedPartitioning` in the joined collection speaks the same declared key set (the
      // `PartitioningCollection` invariant), and `keysSatisfy` admits a marked side into it only
      // for full-key join keys, where the join equality ties every column of its claim: a row of
      // an undeclared key matches nothing on the accurate side and is filtered, so a marked
      // member alongside an unmarked one is spurious. Clear it here so consumers can take
      // members' markers at face value (see
      // `KeyedPartitioning.mayContainUnknownPartitionKeys`).
      PartitioningCollection.fromPartitionings(
        clearUnknownPartitionKeys(Seq(left.outputPartitioning, right.outputPartitioning)))
    case LeftOuter | LeftSingle => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => fullOuterOutputPartitioning
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
  }

  /**
   * Clears the `mayContainUnknownPartitionKeys` marker of every `KeyedPartitioning` in
   * `partitionings` when marked and unmarked keyed inputs meet in an `InnerLike` join.
   * See the call site for why the marker can be cleared.
   */
  private def clearUnknownPartitionKeys(
      partitionings: Seq[Partitioning]): Seq[Partitioning] = {
    // One cached keyed member answers per input instead of re-flattening a left-deep join
    // chain, and keyless inputs drop out of the `flatMap` rather than reading as unmarked. The
    // all-unmarked path must reach no `copy`: `transform`'s `fastEquals` would compare every
    // partition key.
    val representatives = partitionings.map(PartitioningCollection.representativeOf)
    val markers = representatives.flatten.map(_.mayContainUnknownPartitionKeys)
    if (markers.isEmpty || markers.forall(_ == markers.head)) {
      partitionings
    } else {
      // The whole input has to come out holding one layout object, not merely equal ones.
      // `PartitioningCollection.fromPartitionings` returns a member untouched only where its layout
      // is `eq` the canonical one, so a fresh copy here would make the unmarked side rebuild, and
      // re-check the collection's invariant, on every `outputPartitioning` call.
      //
      // The guard above means an unmarked side exists, so its layout is the one to keep. The `eq`
      // on the keys is what makes this free: the two sides share that reference wherever they were
      // laid out on one another, which is the shape this arm is for, and where they do not the
      // copy is used without walking the keys twice.
      val unmarkedLayout = representatives.flatten
        .find(!_.mayContainUnknownPartitionKeys).map(_.layout)
      partitionings.zip(representatives).map {
        case (partitioning: Partitioning with Expression, Some(representative))
            if representative.mayContainUnknownPartitionKeys =>
          val copied = representative.layout.copy(unknownKeysOrigin = None)
          val cleared = unmarkedLayout
            .filter(l => (l.partitionKeys eq copied.partitionKeys) && l == copied)
            .getOrElse(copied)
          partitioning.transform {
            case k: KeyedPartitioning => k.copy(layout = cleared)
          }.asInstanceOf[Partitioning]
        case (p, _) => p
      }
    }
  }

  /**
   * The output partitioning of a FULL OUTER join, rebuilt on the coalesced join keys per
   * partition expression, or `UnknownPartitioning` where that is not sound.
   *
   * Neither side's partitioning describes the output: a NULL-padded row of one side was placed by
   * the other side's key. The coalesced key does describe every row. A matched row has equal keys
   * on both sides, and an unmatched row has its own key next to NULLs, so it sits in the partition
   * its coalesced key hashes to. That takes three things:
   *  - the two sides are co-partitioned hash layouts, so the i-th expressions of the two sides are
   *    the two sides of one join key pair and hash into the same partition space;
   *  - every key evaluates to NULL on a NULL-padded row (see `isNullOnNullPadding`);
   *  - every key is deterministic.
   * A null-aware side may have spread its NULL-keyed rows, and those are exactly the rows whose
   * coalesced key contains a NULL, so the output is null-aware whenever either side is.
   *
   * Every sound pair of the children's members is reported, each in up to
   * `FULL_OUTER_JOIN_OUTPUT_PARTITIONING_EXPAND_LIMIT` argument orders (see `coalescedKeys`),
   * in a `PartitioningCollection` when there is more than one.
   */
  private def fullOuterOutputPartitioning: Partitioning = {
    val leftPartitioning = left.outputPartitioning
    val fallback = UnknownPartitioning(leftPartitioning.numPartitions)
    if (fullOuterExpandLimit == 0) {
      fallback
    } else {
      requiredChildDistribution match {
        case Seq(leftDist: ClusteredDistribution, rightDist: ClusteredDistribution) =>
          val leftHashes = hashPartitionings(leftPartitioning)
          val rightHashes = hashPartitionings(right.outputPartitioning)
          val coalesced = for {
            l <- leftHashes
            r <- rightHashes
            p <- coalescedPartitionings(l, leftDist, r, rightDist)
          } yield p
          coalesced match {
            case Seq() => fallback
            case Seq(single) => single
            case multiple => PartitioningCollection.fromPartitionings(multiple)
          }
        case _ => fallback
      }
    }
  }

  private def hashPartitionings(partitioning: Partitioning): Seq[HashPartitioningLike] =
    PartitioningCollection.flatten(partitioning).collect { case h: HashPartitioningLike => h }

  private def coalescedPartitionings(
      leftPartitioning: HashPartitioningLike,
      leftDist: ClusteredDistribution,
      rightPartitioning: HashPartitioningLike,
      rightDist: ClusteredDistribution): Seq[Partitioning] = {
    // Compatibility pairs the i-th expressions of the two sides by join key position, and for a
    // coalesced pair also requires the same reducer boundaries.
    val compatible = leftPartitioning.createShuffleSpec(leftDist)
      .isCompatibleWith(rightPartitioning.createShuffleSpec(rightDist))
    if (!compatible) return Nil

    val keyPairs = leftPartitioning.expressions.zip(rightPartitioning.expressions)
    val allowCoalesced = keyPairs.forall { case (l, r) => canCoalesce(l, r) }
    if (!allowCoalesced) return Nil

    // A compatible pair has the same number of partitions and, if coalesced, the same boundaries,
    // so either side fits, and a null-aware side is the one to keep.
    val candidatePartitioning = rightPartitioning match {
      case _: NullAwareHashPartitioning | _: CoalescedNullAwareHashPartitioning => rightPartitioning
      case _ => leftPartitioning
    }
    coalescedKeys(keyPairs).map { keys =>
      candidatePartitioning.withNewChildren(keys).asInstanceOf[Partitioning]
    }
  }

  /**
   * The coalesced keys of `keyPairs`, at most `fullOuterExpandLimit` of them. `Coalesce(l, r)` and
   * `Coalesce(r, l)` agree on every output row: a matched row has equal keys, and an unmatched
   * row has a NULL on the other side. So each position may take either order. Every key left
   * first and every key right first come first, as a query usually writes all of its keys in one
   * order, and the mixed orders follow.
   */
  private def coalescedKeys(keyPairs: Seq[(Expression, Expression)]): Seq[Seq[Expression]] = {
    val alternatives = keyPairs.map { case (l, r) => Seq(Coalesce(Seq(l, r)), Coalesce(Seq(r, l))) }
    val leftFirst = alternatives.map(_.head)
    val rightFirst = alternatives.map(_.last)
    val mixed = MultiTransform.generateCartesianProduct(alternatives.map(a => () => a))
      .filter(keys => keys != leftFirst && keys != rightFirst)
    // A strict `List`, as an unforced `LazyList` held by a plan node can overflow the stack.
    (leftFirst #:: rightFirst #:: mixed)
      .take(fullOuterExpandLimit)
      .toList
  }

  /**
   * Whether `Coalesce(leftKey, rightKey)` places every output row where its side placed it: both
   * keys are deterministic and NULL on the rows that pad their side.
   */
  private def canCoalesce(leftKey: Expression, rightKey: Expression): Boolean =
    leftKey.deterministic && rightKey.deterministic &&
      isNullOnNullPadding(leftKey, left.outputSet) && isNullOnNullPadding(rightKey, right.outputSet)

  /**
   * Whether `e` is NULL whenever every attribute of `side` is NULL, as on the rows FULL OUTER
   * pads with NULLs for that side. Conservative: an attribute of `side` is, and so is a
   * null-intolerant expression over at least one such child. `coalesce(a.x, 0)` is not, since it
   * is 0 on a padded row, which would put the coalesced key of an unmatched row of the other side
   * on the wrong partition.
   */
  private def isNullOnNullPadding(e: Expression, side: AttributeSet): Boolean = e match {
    case a: Attribute => side.contains(a)
    case _ => e.nullIntolerant && e.children.exists(isNullOnNullPadding(_, side))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter | LeftSingle =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }
}

object ShuffledJoin {
  /**
   * Whether replicating the right side over splits of the left cannot change the result. The
   * right partition then reaches every left split, so no output row may come from a right row
   * alone. Every join that drops unmatched right rows qualifies: its output is one row per left
   * row or one per matching pair, and each left row still lands in exactly one split.
   */
  def canDuplicateRightSide(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSingle | LeftExistence(_) => true
    case _ => false
  }

  /**
   * Whether replicating the left side over splits of the right cannot change the result. Every
   * output row must then be tied to one right row. The left-preserving joins are out for that
   * reason, and so is LeftSemi: it drops unmatched left rows yet emits one row per left row, so
   * a left row matching in two right splits would come out twice.
   */
  def canDuplicateLeftSide(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }
}

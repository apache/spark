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

package org.apache.spark.sql.execution.exchange

import scala.collection.immutable.BitSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.{LogKeys}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 *
 * @param optimizeOutRepartition A flag to indicate that if this rule should optimize out
 *                               user-specified repartition shuffles or not. This is mostly true,
 *                               but can be false in AQE when AQE optimization may change the plan
 *                               output partitioning and need to retain the user-specified
 *                               repartition shuffles in the plan.
 * @param requiredDistribution The root required distribution we should ensure. This value is used
 *                             in AQE in case we change final stage output partitioning.
 */
case class EnsureRequirements(
    optimizeOutRepartition: Boolean = true,
    requiredDistribution: Option[Distribution] = None)
  extends Rule[SparkPlan] {

  /**
   * How a [[KeyedPartitioning]] can satisfy a required distribution. A `Left` satisfies it as it
   * is. A `Right` satisfies it after a [[GroupPartitionsExec]] that projects its partition keys to
   * the given partition expression positions, and carries `None` positions when the node only has
   * to coalesce duplicate partition keys. See `splitKeyedPartitionings`.
   */
  private type KeyedResolution =
    Either[KeyedPartitioning, (KeyedPartitioning, Option[Seq[Int]])]

  private def ensureDistributionAndOrdering(
      parent: Option[SparkPlan],
      originalChildren: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution],
      requiredChildOrderings: Seq[Seq[SortOrder]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)

    // What each child has to satisfy to line up with its siblings, in child order, and nothing for
    // a child that answers for itself. Only a `ClusteredDistribution` asks to be co-partitioned,
    // and only against another one, so a lone clustered child is not in here either: it stands
    // alone like a child that wants a broadcast or nothing.
    val clustered = requiredChildDistributions.map {
      case required: ClusteredDistribution => Some(required)
      case _ => None
    }
    val isCoPartitioned = clustered.count(_.isDefined) > 1
    val coPartitioned: Seq[Option[ClusteredDistribution]] =
      if (isCoPartitioned) clustered else Seq.fill(clustered.length)(None)

    // Two paths, kept apart. A child that has to line up with another cannot be resolved on its
    // own. Whether it needs a `GroupPartitionsExec`, and on which keys, depends on what the other
    // side turns out to offer. So this loop resolves only the children that answer for themselves,
    // and `coPartitionChildren` owns the rest end to end.
    val resolved = originalChildren.lazyZip(requiredChildDistributions).lazyZip(coPartitioned).map {
      case (child, _, Some(_)) => child
      case (child, distribution, None) =>
        resolveChild(child, distribution, shuffleOrigin)
    }

    val distributed = if (isCoPartitioned) {
      coPartitionChildren(parent, resolved, coPartitioned, shuffleOrigin)
    } else {
      resolved
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    distributed.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        // Before adding a SortExec, check whether a GroupPartitionsExec anywhere in the child
        // subtree can self-satisfy via sorted merge. tryEnableSortedMerge generates all alternative
        // plans where one or more GPEs have sorted merge enabled; we take the first one whose
        // outputOrdering satisfies the requirement.
        tryEnableSortedMerge(child)
          .find(newChild => SortOrder.orderingSatisfies(newChild.outputOrdering, requiredOrdering))
          .getOrElse(SortExec(requiredOrdering, global = false, child = child))
      }
    }
  }

  /**
   * What `child` needs to satisfy `distribution` on its own: nothing, a [[GroupPartitionsExec]], a
   * broadcast, or a shuffle.
   */
  private def resolveChild(
      child: SparkPlan,
      distribution: Distribution,
      shuffleOrigin: ShuffleOrigin): SparkPlan = {
    // Ask what the child's partitioning still needs to satisfy the distribution
    val (otherSatisfies, keyed) =
      splitKeyedPartitionings(child.outputPartitioning, distribution)

    // If a non-KeyedPartitioning already satisfies, no changes needed
    if (otherSatisfies) {
      child
    } else {
      keyed match {
        case Some(resolution) =>
          (distribution, resolution) match {
            case (o: OrderedDistribution, _) =>
              // OrderedDistribution requires grouped KeyedPartitioning with sorted keys
              // according to the distribution's ordering.
              val satisfyingKeyedPartitioning = resolution.fold(identity, _._1)
              // The single-column invariant in KeyedPartitioning.supportsExpressions guarantees
              // one attribute per partition expression.
              val attrs = satisfyingKeyedPartitioning.expressions.flatMap(_.references)
              val keyRowOrdering = RowOrdering.create(o.ordering, attrs)
              val keyOrdering = keyRowOrdering.on((t: InternalRowComparableWrapper) => t.row)
              val keys = satisfyingKeyedPartitioning.partitionKeys
              // An empty zip is vacuously sorted, which is the answer for a single key.
              if (keys.zip(keys.drop(1)).forall { case (k1, k2) => keyOrdering.lteq(k1, k2) }) {
                child
              } else {
                // Use distributePartitions to spread splits across expected partitions
                val sortedGroupedKeys = keys
                  .groupBy(identity).view.mapValues(_.size)
                  .toSeq.sortBy(_._1)(keyOrdering)
                GroupPartitionsExec(child,
                  expectedPartitionKeys = Some(sortedGroupedKeys),
                  distributePartitions = true
                )
              }

            // A KeyedPartitioning satisfies the distribution and a node would change nothing
            case (_, scala.Left(_)) =>
              child

            // A KeyedPartitioning satisfies the distribution only after a GroupPartitionsExec:
            // to coalesce duplicate partition keys, to project the partition keys down to the
            // cluster keys, or both. The positions to project to come from whichever member
            // of the child's partitioning leaves the most partitions.
            case (_, scala.Right((_, positions))) =>
              GroupPartitionsExec(child, joinKeyPositions = positions)
          }

        case None =>
          // No partitioning satisfies - need broadcast or shuffle
          val numPartitions = distribution.requiredNumPartitions
            .getOrElse(conf.numShufflePartitions)
          distribution match {
            case BroadcastDistribution(mode) =>
              BroadcastExchangeExec(mode, child)
            case _: StatefulOpClusteredDistribution =>
              ShuffleExchangeExec(
                distribution.createPartitioning(numPartitions), child,
                REQUIRED_BY_STATEFUL_OPERATOR)
            case _ =>
              ShuffleExchangeExec(
                distribution.createPartitioning(numPartitions), child, shuffleOrigin)
          }
      }
    }
  }

  /**
   * Resolves each co-partitioned child on its own, which is what is left once nothing is arranged
   * between them.
   */
  private def resolveEachChild(
      children: Seq[SparkPlan],
      coPartitioned: Seq[Option[ClusteredDistribution]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = children.zip(coPartitioned).map {
    case (child, required) => required.fold(child)(resolveChild(child, _, shuffleOrigin))
  }

  /**
   * Decides the children an operator co-partitions, together, because neither side's answer stands
   * on its own.
   *
   * Three shapes need nothing arranged between the children, and every one of them still resolves
   * each child on its own: a child can owe its distribution a [[GroupPartitionsExec]] or a
   * partition count whether or not it lines up with its siblings.
   *   1. Every side is a single small partition, so they line up whatever they hold.
   *   2. The sources report layouts a storage-partitioned join can align, which
   *      `checkKeyGroupCompatible` decides and builds end to end.
   *   3. Both sides pass shuffle partition ids through, on compatible specs.
   *
   * Failing all three, `shuffleToCoPartition` shuffles whichever children are not aligned onto one
   * that is.
   */
  private def coPartitionChildren(
      parent: Option[SparkPlan],
      children: Seq[SparkPlan],
      coPartitioned: Seq[Option[ClusteredDistribution]],
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = {
    // Every way of lining the children up as they are still needs this, and only the shuffle below
    // reads more than it, hence `lazy`: the storage-partitioned join plans from the children as
    // their sources report them, so when it succeeds nothing here is asked for.
    lazy val resolved = resolveEachChild(children, coPartitioned, shuffleOrigin)

    // Special case: if all sides of the join are single partition and their physical size is at
    // most spark.sql.maxSinglePartitionBytes.
    val preferSinglePartition = children.zip(coPartitioned).forall {
      case (child, Some(_)) =>
        child.outputPartitioning == SinglePartition &&
          child.logicalLink
            .forall(_.stats.sizeInBytes <= conf.getConf(SQLConf.MAX_SINGLE_PARTITION_BYTES))
      case _ => true
    }

    if (preferSinglePartition) {
      // Nothing to arrange between them, but each still has to satisfy its own requirement. A
      // `SinglePartition` child does unless the distribution asks for a partition count, which a
      // stateful operator's does.
      resolved
    } else {
      // The two checks below decide on a pair: the operator, and what each of its children has to
      // satisfy. Spark doesn't support multi-way join at the moment, so a parent that
      // co-partitions has exactly two children and both of them are in the decision.
      val linedUp = (parent, coPartitioned) match {
        case (Some(operator), Seq(Some(leftRequired), Some(rightRequired))) =>
          // key group compatibility check
          checkKeyGroupCompatible(
            operator, children.head, leftRequired, children(1), rightRequired)
            // If key group check fails, check ShufflePartitionIdPassThrough compatibility. That
            // one reads the children once each has satisfied its distribution on its own.
            .orElse(Option.when(checkShufflePartitionIdPassThroughCompatible(
              resolved.head, leftRequired, resolved(1), rightRequired))(resolved))
        case _ => None
      }
      linedUp.getOrElse(shuffleToCoPartition(resolved, coPartitioned))
    }
  }

  /**
   * The layout the co-partitioned children are lined up on, and the member each of them pairs with
   * it. A child with no pair is one that has to be shuffled onto `member`.
   */
  private case class CoPartitionTarget(
      member: LeafShuffleSpec,
      pairedMembers: Seq[Option[LeafShuffleSpec]])

  /**
   * Lines the co-partitioned children up by shuffle, which is what is left once no way of taking
   * them as they are has worked. One child's layout is picked and the others are shuffled onto it.
   *
   * The children arrive having each satisfied its distribution on its own.
   */
  private def shuffleToCoPartition(
      children: Seq[SparkPlan],
      coPartitioned: Seq[Option[ClusteredDistribution]]): Seq[SparkPlan] = {
    // One spec per co-partitioned child, in child order, and nothing for a child that is not in
    // the decision.
    val specs = children.zip(coPartitioned).map { case (child, required) =>
      required.map(child.outputPartitioning.createShuffleSpec)
    }
    val target = pickCoPartitionTarget(children, specs)
    // Nothing pairs when no child can serve as the layout, and then each of them is shuffled below
    // on its own required distribution.
    val pairedMembers = target.map(_.pairedMembers).getOrElse(Seq.fill(children.length)(None))

    children.lazyZip(coPartitioned).lazyZip(pairedMembers).map {
      case (child, None, _) =>
        child

      // The positions come from this child's own paired member, since they index into its own
      // partition expressions -- the layout picked only says which member of it the two sides
      // agreed on.
      //
      // The storage-partitioned join above declined, but one can still be had by shuffling the
      // other side onto this one's keys (see the last case below). So the partitioned side's scan
      // has to end up grouped by those keys, which is what pushing the positions in does. They
      // index into the child's own report, which already carries whatever projection
      // `resolveChild` gave it, so `withJoinKeyPositions` composes the two rather than replacing.
      case (child, Some(_), Some(KeyedShuffleSpec(_, _, Some(joinKeyPositions)))) =>
        withJoinKeyPositions(child, joinKeyPositions)

      // Paired as it stands, so there is nothing to shuffle and nothing to push in.
      case (child, Some(_), Some(_)) =>
        child

      case (child, Some(required), None) =>
        val newPartitioning = target.map { picked =>
          // Use the picked layout to create a new partitioning to re-shuffle this child
          picked.member.createPartitioning(required.clustering)
        }.getOrElse {
          // No layout was picked, so we create default partitioning from the required distribution
          val numPartitions = required.requiredNumPartitions
              .getOrElse(conf.numShufflePartitions)
          required.createPartitioning(numPartitions)
        }

        child match {
          case s: ShuffleExchangeExec =>
            s.copy(outputPartitioning = newPartitioning)
          case gpe: GroupPartitionsExec =>
            // Strip every grouping this rule inserted (they can stack on a re-run): a
            // replicating one repeats every row, so none of them may feed the shuffle.
            ShuffleExchangeExec(newPartitioning, peelGroupPartitions(gpe))
          case _ => ShuffleExchangeExec(newPartitioning, child)
        }
    }
  }

  /**
   * Picks the layout to line the co-partitioned children up on. It is one of their own, so that at
   * least the child offering it keeps its partitioning and only the others are shuffled. `None`
   * when no child can serve, and then they all take a shuffle.
   *
   * Find out the shuffle spec that gives better parallelism. A child with no `ShuffleExchangeLike`
   * node comes first, and the largest number of partitions decides among those.
   *
   * NOTE: this is not optimal for the case when there are more than 2 children. Consider:
   *   (10, 10, 11)
   * where each number is that child's partition count, it's better to pick 10
   * here since we only need to shuffle one side - we'd need to shuffle two sides if we pick 11.
   *
   * However this should be sufficient for now since in Spark nodes with multiple children
   * always have exactly 2 children.
   */
  private def pickCoPartitionTarget(
      children: Seq[SparkPlan],
      specs: Seq[Option[ShuffleSpec]]): Option[CoPartitionTarget] = {
    // Whether we should consider `spark.sql.shuffle.partitions` and ensure enough parallelism
    // during shuffle. To achieve a good trade-off between parallelism and shuffle cost, we only
    // consider the minimum parallelism iff ALL children need to be re-shuffled.
    //
    // A child needs to be re-shuffled iff either of the following is true:
    //   1. It can't create partitioning by itself, i.e., `canCreatePartitioning` returns false
    //      (as for the case of `RangePartitioning`), therefore it needs to be re-shuffled
    //      according to another child's shuffle spec.
    //   2. It already has `ShuffleExchangeLike`, so we can re-use existing shuffle without
    //      introducing extra shuffle.
    //
    // On the other hand, in scenarios such as:
    //   HashPartitioning(5) <-> HashPartitioning(6)
    // while `spark.sql.shuffle.partitions` is 10, we'll only re-shuffle the left side and make it
    // HashPartitioning(6).
    val shouldConsiderMinParallelism = children.zip(specs).forall { case (child, spec) =>
      spec.forall(!_.canCreatePartitioning) || child.isInstanceOf[ShuffleExchangeLike]
    }
    // Choose all the specs that can be used to shuffle other children
    val candidateSpecs = children.zip(specs).collect {
      case (child, Some(spec)) if spec.canCreatePartitioning &&
          (!shouldConsiderMinParallelism ||
            child.outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions) =>
        child -> spec
    }
    // Rank on two things at once. A child with no `ShuffleExchangeLike` node comes first, since
    // keeping it costs nothing. For instance, if we have:
    //   A: (No_Exchange, 100) <---> B: (Exchange, 120)
    // it's better to pick A and change B to (Exchange, 100) instead of picking B and insert a
    // new shuffle for A. Then the best parallelism decides, and for a collection that is the best
    // any member offers, since a collection has no count of its own.
    //
    // What the winner contributes is its members, the alternatives the layout is picked from below.
    // Empty when no child can serve as the layout.
    val bestMembers: Seq[LeafShuffleSpec] = candidateSpecs.maxByOption { case (child, spec) =>
      (!child.isInstanceOf[ShuffleExchangeLike], spec.flatten.map(_.numPartitions).max)
    }.toSeq.flatMap(_._2.flatten)

    // A `ShuffleSpecCollection` answers `isCompatibleWith` if *any* of its members does, so the
    // winner alone does not say which member the sides agreed on. The projection pushed into a
    // paired child and the partitioning built for a re-shuffled child both have to come from one
    // member, otherwise the sides end up grouped on different keys, or on a key set the child does
    // not even have. So what is picked here is the pairing, not a member per side.
    //
    // Per member of the winner, the member each child pairs with it, in child order.
    val childMembers: Seq[Seq[LeafShuffleSpec]] = specs.map(_.toSeq.flatMap(_.flatten))
    val pairings: Seq[(LeafShuffleSpec, Seq[Option[LeafShuffleSpec]])] = bestMembers.map { member =>
      member -> childMembers.map(_.find(member.isCompatibleWith))
    }
    // Which children the winner reaches at all, over all of its members.
    val reached: Seq[Boolean] = pairings.map(_._2).transpose.map(_.exists(_.isDefined))
    // The member picked has to pair with every child the winner reaches. A child it does not
    // reach, and a child that is not in the decision, are shuffled whichever member wins, so
    // neither asks for anything here. None surviving means there is no layout to align them on, so
    // they all take the ordinary shuffle. That needs three or more clustered children, since with
    // two the member that reported the match serves both, and no operator has three today.
    //
    // Ranked by parallelism, preferring the finest.
    pairings
      .filter { case (_, paired) =>
        paired.zip(reached).forall { case (pairedMember, isReached) =>
          pairedMember.isDefined || !isReached
        }
      }
      .maxByOption { case (member, _) => member.numPartitions }
      .map { case (member, paired) => CoPartitionTarget(member, paired) }
  }

  private def hasKeyedPartitioning(p: Partitioning): Boolean = p match {
    case e: Expression => e.exists(_.isInstanceOf[KeyedPartitioning])
    case _ => false
  }

  // Generates all alternative plans in which one or more GroupPartitionsExec nodes in the subtree
  // have sorted-merge enabled (every possible combination). Returns a LazyList so the caller can
  // stop evaluating once a satisfying alternative is found.
  //
  // Pruning: traversal stops at SortExec (which reorders data, making sorted merge below it
  // pointless) and at any node whose outputPartitioning no longer carries a KeyedPartitioning.
  // This is a good heuristic, though not strictly equivalent to "ordering no longer propagates":
  // partition-key expressions are constant within each coalesced partition and therefore usually
  // prefix outputOrdering. When a node prunes the KeyedPartitioning (e.g. a Project that drops
  // partition keys), it also prunes that ordering prefix. Since Spark has no notion of constant
  // expressions in SortOrder, dropping a prefix invalidates the rest of the ordering too -- so in
  // practice the two are always pruned together.
  //
  // At each GPE the rule emits [original, sorted-merge-enabled] alternatives (or just [original]
  // when sorted merge cannot be enabled). multiTransformDownWithPruning then builds the Cartesian
  // product across all GPEs in the subtree, giving every combination.
  private[exchange] def tryEnableSortedMerge(plan: SparkPlan): LazyList[SparkPlan] =
    plan.multiTransformDownWithPruning(
      p => !p.isInstanceOf[SortExec] &&
        hasKeyedPartitioning(p.asInstanceOf[SparkPlan].outputPartitioning)) {
      case gpe: GroupPartitionsExec =>
        // Include the original so that peer GPEs are still independently considered.
        gpe +: gpe.tryEnableSortedMerge().toSeq
    }

  private def reorder(
      leftKeys: IndexedSeq[Expression],
      rightKeys: IndexedSeq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): Option[(Seq[Expression], Seq[Expression])] = {
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return None
    }

    // Check if the current order already satisfies the expected order.
    if (expectedOrderOfKeys.zip(currentOrderOfKeys).forall(p => p._1.semanticEquals(p._2))) {
      return Some(leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          return None
      }
    }
    Some(leftKeysBuffer.toSeq, rightKeysBuffer.toSeq)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      reorderJoinKeysRecursively(
        leftKeys,
        rightKeys,
        Some(leftPartitioning),
        Some(rightPartitioning))
        .getOrElse((leftKeys, rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * Recursively reorders the join keys based on partitioning. It starts reordering the
   * join keys to match HashPartitioning on either side, followed by PartitioningCollection.
   */
  private def reorderJoinKeysRecursively(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Option[Partitioning],
      rightPartitioning: Option[Partitioning]): Option[(Seq[Expression], Seq[Expression])] = {
    (leftPartitioning, rightPartitioning) match {
      case (Some(HashPartitioning(leftExpressions, _)), _) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leftExpressions, leftKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(HashPartitioning(rightExpressions, _))) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, rightExpressions, rightKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, leftPartitioning, None))
      case (Some(kp: KeyedPartitioning), _) =>
        // The single-column invariant in KeyedPartitioning.supportsExpressions guarantees one
        // attribute per partition expression.
        val leafExprs = kp.expressions.flatMap(_.references)
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(kp: KeyedPartitioning)) =>
        // The single-column invariant in KeyedPartitioning.supportsExpressions guarantees one
        // attribute per partition expression.
        val leafExprs = kp.expressions.flatMap(_.references)
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, rightKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, leftPartitioning, None))
      case (Some(PartitioningCollection(partitionings)), _) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, Some(p), rightPartitioning))
        }.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, None, rightPartitioning))
      case (_, Some(PartitioningCollection(partitionings))) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, leftPartitioning, Some(p)))
        }.orElse(None)
      case _ =>
        None
    }
  }

  /**
   * When the physical operators are created for JOIN, the ordering of join keys is based on order
   * in which the join keys appear in the user query. That might not match with the output
   * partitioning of the join node's children (thus leading to extra sort / shuffle being
   * introduced). This rule will change the ordering of the join keys to match with the
   * partitioning of the join nodes' children.
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right, isSkew)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkew) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isSkew)

      case other => other
    }
  }

  /**
   * Checks whether two children, `left` and `right`, of a join operator have compatible
   * `KeyedPartitioning`, and can benefit from storage-partitioned join.
   *
   * Returns the updated new children if the check is successful, otherwise `None`.
   */
  private def checkKeyGroupCompatible(
      parent: SparkPlan,
      left: SparkPlan,
      leftRequired: ClusteredDistribution,
      right: SparkPlan,
      rightRequired: ClusteredDistribution): Option[Seq[SparkPlan]] = {
    parent match {
      case smj: SortMergeJoinExec =>
        checkKeyGroupCompatible(left, leftRequired, right, rightRequired, smj.joinType)
      case sj: ShuffledHashJoinExec =>
        checkKeyGroupCompatible(left, leftRequired, right, rightRequired, sj.joinType)
      case _ =>
        None
    }
  }

  private def checkKeyGroupCompatible(
      left: SparkPlan,
      leftRequired: ClusteredDistribution,
      right: SparkPlan,
      rightRequired: ClusteredDistribution,
      joinType: JoinType): Option[Seq[SparkPlan]] = {
    // Plan from the children as their sources report them, stripping any grouping this rule put
    // there: the one the distribution step above just added, and on a re-run the aligned one an
    // earlier pass left behind. Everything below then lives in one index space, the source's raw
    // partition keys, so the positions, the reducers and the merged keys all mean one thing. This
    // is also what lets the node be built once, here, instead of being placed as a guess and
    // rewritten.
    val rawLeft = peelGroupPartitions(left)
    val rawRight = peelGroupPartitions(right)

    // Peeling something off means an earlier run of this rule already settled a pairing here, and
    // re-deciding it does not give the same answer. This one plans from the sources again, on an
    // input that by then holds the keyed shuffle that run inserted, so the pairing sees two keyed
    // sides where the first run saw one and aligns both to a merged key set they already hold.
    // Keeping what arrived is what makes the rule idempotent.
    //
    // Three terms. Nothing was peeled on a plan this rule has not seen, so the question is not even
    // asked there and the pairing below decides as it always did. Partially clustered distribution
    // is excluded because it pushes even for two children that line up, assigning per-key slot
    // counts against skew, which only the push branch builds. And the pair has to be one this rule
    // would settle on anyway.
    val keepArrivedPairing = ((left ne rawLeft) || (right ne rawRight)) &&
      !conf.v2BucketingPartiallyClusteredDistributionEnabled &&
      alreadyCoPartitioned(left, leftRequired, right, rightRequired)
    if (keepArrivedPairing) {
      return Some(Seq(left, right))
    }

    val leftCandidates = candidatesFor(rawLeft, leftRequired)
    val rightCandidates = candidatesFor(rawRight, rightRequired)
    if (leftCandidates.isEmpty || rightCandidates.isEmpty) return None

    // The two sides are co-partitioned as they stand when each spec still describes its own
    // source's layout, so no projection narrowed either. A side needing the plain grouping node
    // below is not a reason to push anything. That node only coalesces the splits the source
    // reports for one key.
    def bothUnprojected(l: KeyedShuffleSpec, r: KeyedShuffleSpec): Boolean =
      l.joinKeyPositions.isEmpty && r.joinKeyPositions.isEmpty

    // Whether the merged key list below may be narrowed to what the join type allows. Not when
    // either side is marked. Narrowing drops groups that side holds, so its regrouping stops being
    // the identity, and `GroupPartitionsExec` gives up a marked claim it does not regroup
    // identically. That costs the pair its join at the gate at the end of this method, which is a
    // poor trade for pruning a few groups.
    //
    // Narrowing is the only thing here that can shrink the list below a marked side's own keys: the
    // merging arms take the union, and `KeyedShuffleSpec.areKeysCompatible` pairs a marked layout
    // only with one whose keys are a subset of its own.
    //
    // Sorting is the other way a regrouping stops being the identity, and this does not address it.
    // `mergeAndDedupPartitions` sorts, so a marked side whose declared order is not the sorted one
    // is relabelled even where the key set is unchanged.
    val partitionFilter = conf.getConf(SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED)
    def filtersKeys(l: KeyedPartitioning, r: KeyedPartitioning): Boolean =
      partitionFilter && !l.mayContainUnknownPartitionKeys && !r.mayContainUnknownPartitionKeys

    // How many key groups the pushdown below would leave this pair. `mergeAndDedupPartitions`
    // keeps one side's keys and drops the other's for the filtered one-sided join types, and there
    // the dropped side's count says nothing, so rank on the side that survives. The arms that
    // really merge have no cheap answer, so they take the larger of the two counts. Keep the join
    // types here in step with `mergeAndDedupPartitions`.
    def rank(l: KeyedShuffleSpec, r: KeyedShuffleSpec): Int = {
      val filtered = filtersKeys(l.partitioning, r.partitioning)
      joinType match {
        case LeftOuter | LeftAnti | LeftSingle | ExistenceJoin(_) if filtered =>
          l.numPartitions
        case RightOuter if filtered => r.numPartitions
        case _ => l.numPartitions.max(r.numPartitions)
      }
    }

    // Each side may offer several members, and the right one is the one the other side can pair
    // with, which neither side can tell on its own. So pick the pair rather than a member per side,
    // and rank the pairs that agree on the keys by the parallelism they offer, the same trade
    // `pickCoPartitionTarget` makes between children when it picks the layout to shuffle onto.
    //
    // Two things keep `rank` from being what the join actually gets, both on the merging arms.
    // `InnerLike` and `LeftSemi` intersect under `v2BucketingPartitionFilterEnabled`, unless
    // `filtersKeys` turned filtering off for the pair, and an
    // intersection is not monotone in member granularity: members cover different clustering keys
    // rather than nested ones, so a finer pair can rank above a coarser one and still meet the
    // other side in fewer groups. And a union does not merely exceed the rank either, because
    // `reduceKeys` runs between this pick and the merge and collapses distinct keys, so a
    // `bucket(16)` side reduced onto `bucket(8)` brings 8 keys to a merge its spec ranked at 16.
    // Ranking on the merged count would match what is delivered, at the cost of merging every
    // candidate pair, which would also raise `storagePartitionJoinIncompatibleReducedTypesError`
    // for pairs that are never chosen.
    //
    // Ties go to a pair both children report as it stands, to keep the no-grouping-node path. A
    // projected count never exceeds the physical one, so nothing outranks such a pair, but a coarse
    // member whose projected count happens to equal it ties, and enumeration order would decide.
    val agreeingPairs = for {
      l <- leftCandidates
      r <- rightCandidates
      // This is the one caller that runs the reduce, so it is the one that asks with the reduce
      // allowed. See `areKeysCompatible`'s `allowReduce`.
      if l.areKeysCompatible(r, allowReduce = true)
    } yield (l, r)
    val bestPair = agreeingPairs.maxByOption { case (l, r) => (rank(l, r), bothUnprojected(l, r)) }
    // No agreeing pair means no pairing can be planned at all: `isCompatibleWith` and the push
    // branch's `areKeysCompatible` both fail for every pair, so carrying one to the end would
    // return `None` after logging a pushdown that does not happen.
    if (bestPair.isEmpty) return None
    val (leftSpec, rightSpec) = bestPair.get
    val leftPartitioning = leftSpec.partitioning
    val rightPartitioning = rightSpec.partitioning

    val compatibleAsIs =
      bothUnprojected(leftSpec, rightSpec) && leftSpec.isCompatibleWith(rightSpec)
    // Entering the push branch is not the same as taking it. The keys agree for this pair by
    // construction, since `agreeingPairs` filtered on exactly that and an empty result returned
    // above, so what the branch pushes always applies to the pair it was chosen for. The gate at
    // the end can still discard the result, when a node it builds gives up its keyed claim.
    val pushCommonValues =
      (!compatibleAsIs || conf.v2BucketingPartiallyClusteredDistributionEnabled) &&
        (conf.v2BucketingPushPartValuesEnabled ||
          conf.v2BucketingAllowKeysSubsetOfPartitionKeys)
    // Neither route lines the two sides up, so there is no pairing to commit to. Asked here rather
    // than after the push branch, which is the only thing below that does any work.
    if (!compatibleAsIs && !pushCommonValues) {
      return None
    }
    // What the push branch builds, when it runs. Empty otherwise, and then each side gets a plain
    // grouping node instead, which it needs exactly when its source reports more than one partition
    // per key. Building both eagerly would derive a grouping the push branch throws away, and that
    // is one hash per partition key.
    val pushed = Option.when(pushCommonValues) {
      // Partition expressions are compatible. Regardless of whether partition values
      // match from both sides of children, we can calculate a superset of partition values and
      // push-down to respective data sources so they can adjust their output partitioning by
      // filling missing partition keys with empty partitions. As result, we can still avoid
      // shuffle.
      //
      // For instance, if two sides of a join have partition expressions
      // `day(a)` and `day(b)` respectively
      // (the join query could be `SELECT ... FROM t1 JOIN t2 on t1.a = t2.b`), but
      // with different partition values:
      //   `day(a)`: [0, 1]
      //   `day(b)`: [1, 2, 3]
      // Following the case 2 above, we don't have to shuffle both sides, but instead can
      // just push the common set of partition values: `[0, 1, 2, 3]` down to the two data
      // sources.
      val leftPartKeys = leftPartitioning.partitionKeys
      val rightPartKeys = rightPartitioning.partitionKeys

      val numLeftPartKeys = MDC(LogKeys.NUM_LEFT_PARTITION_VALUES, leftPartKeys.size)
      val numRightPartKeys = MDC(LogKeys.NUM_RIGHT_PARTITION_VALUES, rightPartKeys.size)
      logInfo(
        log"""
            |Left side # of partitions: $numLeftPartKeys
            |Right side # of partitions: $numRightPartKeys
            |""".stripMargin)

      // in case of compatible but not identical partition expressions, we apply 'reduce'
      // transforms to group one side's partitions as well as the common partition values
      val (leftReducers, rightReducers) = leftSpec.reducersBothWays(rightSpec)
      val (leftReducedDataTypes, leftReducedKeys) = leftReducers.fold(
        (leftPartitioning.keyDataTypes, leftPartitioning.partitionKeys)
      )(leftPartitioning.reduceKeys)
      val (rightReducedDataTypes, rightReducedKeys) = rightReducers.fold(
        (rightPartitioning.keyDataTypes, rightPartitioning.partitionKeys)
      )(rightPartitioning.reduceKeys)
      // The reduced types are the types of the key rows the merge below sees, and a side with no
      // key row left answers for them from its layout, which a reduce writes them into. So the
      // comparison is meaningful on both sides whether or not either has a key (SPARK-59176).
      if (leftReducedDataTypes != rightReducedDataTypes) {
        // The two lists are the erased ones, so a struct in the message prints positional field
        // names. That is deliberate: the names do not decide where a key belongs, so printing the
        // connector's own would point a reader at a difference that is not the cause.
        throw QueryExecutionErrors.storagePartitionJoinIncompatibleReducedTypesError(
          leftReducers = leftReducers,
          leftReducedDataTypes = leftReducedDataTypes,
          rightReducers = rightReducers,
          rightReducedDataTypes = rightReducedDataTypes)
      }

      val reducedKeyOrdering = KeyedPartitioning.groupedKeyRowOrdering(leftReducedDataTypes)
        .on((t: InternalRowComparableWrapper) => t.row)

      // merge values on both sides
      var mergedPartitionKeys =
        mergeAndDedupPartitions(leftReducedKeys, rightReducedKeys, joinType, reducedKeyOrdering,
          filterPartitions = filtersKeys(leftPartitioning, rightPartitioning))
          .map((_, 1))

      logInfo(log"After merging, there are " +
        log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.size)} partitions")

      var replicateLeftSide = false
      var replicateRightSide = false
      var applyPartialClustering = false

      // This means we allow partitions that are not clustered on their values,
      // that is, multiple partitions with the same partition value. In the
      // following, we calculate how many partitions that each distinct partition
      // value has, and pushdown the information to scans, so they can adjust their
      // final input partitions respectively.
      if (conf.v2BucketingPartiallyClusteredDistributionEnabled) {
        logInfo("Calculating partially clustered distribution for " +
            "storage-partitioned join")

        // Similar to `OptimizeSkewedJoin`, we need to check join type and decide
        // whether partially clustered distribution can be applied. For instance, the
        // optimization cannot be applied to a left outer join, where the left hand
        // side is chosen as the side to replicate partitions according to stats.
        // Otherwise, query result could be incorrect.
        val canReplicateLeft = ShuffledJoin.canDuplicateLeftSide(joinType)
        val canReplicateRight = ShuffledJoin.canDuplicateRightSide(joinType)

        if (!canReplicateLeft && !canReplicateRight) {
          logInfo(log"Skipping partially clustered distribution as it cannot be applied for " +
            log"join type '${MDC(LogKeys.JOIN_TYPE, joinType)}'")
        } else {
          // The statistics and the original partition keys come from the pre-alignment plans,
          // which is what both sides already are here.
          val leftLink = rawLeft.logicalLink
          val rightLink = rawRight.logicalLink

          replicateLeftSide = if (
            leftLink.isDefined && rightLink.isDefined &&
                leftLink.get.stats.sizeInBytes > 1 &&
                rightLink.get.stats.sizeInBytes > 1) {
            val leftLinkStatsSizeInBytes = MDC(LogKeys.LEFT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES,
              leftLink.get.stats.sizeInBytes)
            val rightLinkStatsSizeInBytes = MDC(LogKeys.RIGHT_LOGICAL_PLAN_STATS_SIZE_IN_BYTES,
              rightLink.get.stats.sizeInBytes)
            logInfo(
              log"""
                 |Using plan statistics to determine which side of join to fully
                 |cluster partition values:
                 |Left side size (in bytes): $leftLinkStatsSizeInBytes
                 |Right side size (in bytes): $rightLinkStatsSizeInBytes
                 |""".stripMargin)
            leftLink.get.stats.sizeInBytes < rightLink.get.stats.sizeInBytes
          } else {
            // As a simple heuristic, we pick the side with fewer partitions to apply the
            // grouping & replication of partitions. The counts read the pre-alignment plans,
            // for the same reason the statistics do. An aligned report holds the merged keys, so
            // comparing two of them decides nothing.
            logInfo("Using number of partitions to determine which side of join " +
                "to fully cluster partition values")
            PartitioningCollection.numKeyedPartitions(rawLeft.outputPartitioning)
              .getOrElse(leftPartKeys.size) <
              PartitioningCollection.numKeyedPartitions(rawRight.outputPartitioning)
              .getOrElse(rightPartKeys.size)
          }

          replicateRightSide = !replicateLeftSide

          // Similar to skewed join, we need to check the join type to see whether replication
          // of partitions can be applied. For instance, replication should not be allowed for
          // the left-hand side of a right outer join.
          if (replicateLeftSide && !canReplicateLeft) {
            logInfo(log"Left-hand side is picked but cannot be applied to join type " +
              log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
              log"distribution.")
            replicateLeftSide = false
          } else if (replicateRightSide && !canReplicateRight) {
            logInfo(log"Right-hand side is picked but cannot be applied to join type " +
              log"'${MDC(LogKeys.JOIN_TYPE, joinType)}'. Skipping partially clustered " +
              log"distribution.")
            replicateRightSide = false
          } else {
            // In partially clustered distribution, we should use un-grouped partition values.
            // The child that keeps its splits and the positions projecting its keys both come
            // from the raw side. The positions are the spec's, and the spec was built from that
            // same raw report.
            val (partiallyClusteredChild, partiallyClusteredPositions) =
              if (replicateLeftSide) {
                (rawRight, rightSpec.joinKeyPositions)
              } else {
                (rawLeft, leftSpec.joinKeyPositions)
              }
            // The side that keeps its splits still holds the original partition keys, one per
            // input split. `outputPartitioning` is either a `PartitioningCollection` or a
            // `KeyedPartitioning`, otherwise `createKeyedShuffleSpecs()` would have returned
            // nothing.
            val originalKeyedPartitioning = PartitioningCollection
              .representativeOf(partiallyClusteredChild.outputPartitioning).get
            val projectedOriginalPartitionKeys = partiallyClusteredPositions
              .fold(originalKeyedPartitioning.partitionKeys)(
                originalKeyedPartitioning.projectKeys(_)._2)

            val numExpectedPartitions =
              projectedOriginalPartitionKeys.groupBy(identity).view.mapValues(_.size)

            mergedPartitionKeys = mergedPartitionKeys.map { case (key, numParts) =>
              (key, numExpectedPartitions.getOrElse(key, numParts))
            }

            logInfo(log"After applying partially clustered distribution, there are " +
              log"${MDC(LogKeys.NUM_PARTITIONS, mergedPartitionKeys.map(_._2).sum)} partitions.")
            applyPartialClustering = true
          }
        }
      }

      // Now we need to push-down the common partition information to the `GroupPartitionsExec`s.
      (
        GroupPartitionsExec(rawLeft, leftSpec.joinKeyPositions,
          Some(mergedPartitionKeys), leftReducers,
          distributePartitions = applyPartialClustering && !replicateLeftSide),
        GroupPartitionsExec(rawRight, rightSpec.joinKeyPositions,
          Some(mergedPartitionKeys), rightReducers,
          distributePartitions = applyPartialClustering && !replicateRightSide))
    }

    // The pairing is only worth committing to if both children still declare the same aligned key
    // sequence once the grouping has been pushed into them. A `GroupPartitionsExec` gives up its
    // keyed claim when it turns out to regroup a layout that pins undeclared rows to
    // `hash(key) % numPartitions` (see `KeyLayout.mayContainUnknownPartitionKeys`), and only the
    // node knows the permutation it performs, so that answer arrives after the pairing was chosen.
    // Asking before returning is what keeps the join from skipping both shuffles for a child that
    // no longer satisfies its distribution, which is a plan `ValidateRequirements` rejects and
    // every AQE rule that needs a valid plan then refuses to touch.
    //
    // The check is pairwise, not a per-side `satisfies`. Partially clustered distribution leaves
    // both children value-aligned yet not grouped on purpose, so a per-side gate would refuse that
    // whole family. What both sides owe each other is the key sequence `alignToExpectedKeys`
    // guarantees, each key repeated as many times as the merge expects, whichever side replicates.
    // `KeyLayout.describesSameKeys` asks exactly that, and it compares the key types as well as
    // the rows.
    //
    // Both routes to `newLeft`/`newRight` build, so both are asked. On the base this was the push
    // branch's question alone, because the other route left the children exactly as the pairing
    // read them. Here `groupIfNeeded` can wrap a side that is not grouped, and that node gives up
    // the claim for the same reason the pushed one does.
    val (newLeft, newRight) =
      pushed.getOrElse((groupIfNeeded(rawLeft), groupIfNeeded(rawRight)))
    def declaredLayout(plan: SparkPlan): Option[KeyLayout] =
      PartitioningCollection.representativeOf(plan.outputPartitioning).map(_.layout)
    val committed = (declaredLayout(newLeft), declaredLayout(newRight)) match {
      case (Some(left), Some(right)) => left.describesSameKeys(right)
      case _ => false
    }
    // Announced here rather than where the branch runs, since the gate can still discard what it
    // built, and a log that names a pushdown should name one that happens.
    if (pushCommonValues) {
      if (committed) {
        logInfo("Pushing common partition values for storage-partitioned join")
      } else {
        logInfo("Not storage-partitioning this join after all: the two sides no longer declare " +
          "the same partition keys once regrouped onto the merged partition values")
      }
    }
    Option.when(committed)(Seq(newLeft, newRight))
  }

  /**
   * Whether the two children satisfy their distributions and line up with each other as they
   * arrive, so that the pairing has nothing to add.
   *
   * Close to what `ValidateRequirements` asks of a finished plan, but not the same question, and
   * the difference is deliberate. This goes through `createKeyedShuffleSpecs`, so it also applies
   * `REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION`, admits a member on `keysMaySatisfy`, and looks at
   * keyed members only. The per-side half is the validator's: `satisfies`, which is strict, so a
   * child that still needs a node answers no.
   *
   * Both sides also have to answer with an unprojected spec. A projected one describes the layout a
   * [[GroupPartitionsExec]] would emit rather than the one the child has, so two sides can agree
   * through their projections while their partitions do not line up at all.
   */
  private def alreadyCoPartitioned(
      left: SparkPlan,
      leftRequired: ClusteredDistribution,
      right: SparkPlan,
      rightRequired: ClusteredDistribution): Boolean = {
    def unprojectedSpecs(plan: SparkPlan, required: ClusteredDistribution): Seq[KeyedShuffleSpec] =
      if (plan.outputPartitioning.satisfies(required)) {
        candidatesFor(plan, required).filter(_.joinKeyPositions.isEmpty)
      } else {
        Nil
      }
    val leftSpecs = unprojectedSpecs(left, leftRequired)
    val rightSpecs = unprojectedSpecs(right, rightRequired)
    leftSpecs.exists(l => rightSpecs.exists(l.isCompatibleWith))
  }

  private def checkShufflePartitionIdPassThroughCompatible(
      left: SparkPlan,
      leftRequired: ClusteredDistribution,
      right: SparkPlan,
      rightRequired: ClusteredDistribution): Boolean = {
    (left.outputPartitioning, right.outputPartitioning) match {
      case (p1: ShufflePartitionIdPassThrough, p2: ShufflePartitionIdPassThrough) =>
        p1.createShuffleSpec(leftRequired).isCompatibleWith(p2.createShuffleSpec(rightRequired))
      case _ =>
        false
    }
  }

  /**
   * The plan a co-partitioned child's source reports, with every grouping and local sort this rule
   * put over it peeled off. `plan` itself when it carries none.
   *
   * The grouping peeled here is one an earlier pass left behind, not one this pass put on. The
   * per-child step skips co-partitioned children entirely, so nothing of this pass's is under
   * there when `checkKeyGroupCompatible` asks. The shuffle step, the other caller, can see both.
   * `EnsureRequirements` is re-run on plans it already produced, since `AdaptiveSparkPlanExec`
   * builds one instance of this rule, and `ConvertSortMergeJoinToShuffledHashJoin` and
   * `OptimizeSkewedJoin` hand the whole tree back to it after rewriting some other join, all within
   * one `queryStagePreparationRules` pass. So a join child can arrive as
   * `GroupPartitionsExec(SortExec(GroupPartitionsExec(scan)))`, and planning from anything but the
   * scan would derive the alignment from an already aligned layout and duplicate rows.
   *
   * The descent only traverses a `GroupPartitionsExec` and a *local* `SortExec`. That bound is a
   * decision, not an omission: a node hidden behind anything else belongs to a different operator,
   * and peeling it would undo that operator's alignment. A global `SortExec` stops the descent for
   * the same reason. It requires `OrderedDistribution`, which a `KeyedPartitioning` can satisfy
   * (behind `spark.sql.sources.v2.bucketing.sorting.enabled`) through a node built to emit the keys
   * in sorted order, and peeling that node would destroy the ordering it exists to provide.
   *
   * A local sort that is peeled off is re-added by the ordering step at the end of
   * `ensureDistributionAndOrdering`, which is what put it there in the first place.
   *
   * The two callers are `checkKeyGroupCompatible` and the shuffle step, both on the co-partitioned
   * path, so every `GroupPartitionsExec` this reaches is one the rule put there itself. A
   * single-child operator genuinely needs its non-grouped input grouped and keeps the wrap
   * `resolveChild` gave it. `withJoinKeyPositions`, which other multi-child operators reach, does
   * not descend at all.
   */
  private[exchange] def peelGroupPartitions(plan: SparkPlan): SparkPlan = {
    // `None` unless a `GroupPartitionsExec` is actually down there. A local sort is only this
    // rule's to drop when it sits over one, otherwise it is the user's `sortWithinPartitions` and
    // peeling it would lose an ordering nothing puts back.
    def peel(p: SparkPlan): Option[SparkPlan] = p match {
      case g: GroupPartitionsExec => Some(peel(g.child).getOrElse(g.child))
      case s: SortExec if !s.global => peel(s.child)
      case _ => None
    }
    peel(plan).getOrElse(plan)
  }

  /**
   * Applies join key positions to a plan by rebuilding a plain grouping node, or by wrapping
   * anything else in a new one.
   *
   * Unlike `peelGroupPartitions`, this does not descend. It serves every multi-child operator,
   * not just joins, so a `GroupPartitionsExec` below the top is not known to be this rule's own.
   */
  private[exchange] def withJoinKeyPositions(plan: SparkPlan, positions: Seq[Int]): SparkPlan = {
    plan match {
      // Only a plain grouping is rebuilt. An aligned one carries a merged key count, a reducer or
      // a replication, none of which this rebuild reproduces, so it is wrapped below instead.
      case g: GroupPartitionsExec
          if g.expectedKeyCount.isEmpty && g.reducers.isEmpty && !g.distributePartitions =>
        // Rebuilt rather than copied: the positions are an input to the node's grouping, and a
        // `copy` would keep the grouping derived from the old ones.
        //
        // `positions` index the layout `g` reports, while `g.child` holds the raw partition
        // expressions, so they are composed rather than replaced. The two index spaces differ
        // whenever `g` already projects, which happens on two paths. `resolveChild` projects a
        // co-partitioned child onto its cluster keys before the pairing declines, and the two
        // derivations disagree about which positions those are. `positionsCoveringClusterKeys`
        // also keeps an expression that *is* a cluster key, where `KeyedShuffleSpec.keyPositions`
        // reads an expression's reference. And a re-run reads the positions off a report an
        // earlier pass already projected.
        val composed = g.joinKeyPositions.fold(positions)(old => positions.map(old))
        val newGroupPartitions =
          GroupPartitionsExec(g.child, Some(composed), enableSortedMerge = g.enableSortedMerge)
        newGroupPartitions.copyTagsFrom(g)
        newGroupPartitions
      // Everything else is wrapped, an aligned grouping included. `positions` index what `plan`
      // reports, which is exactly what the node built here projects, so the two line up with no
      // composition. The base replaced a node's positions with a `copy` instead, which neither
      // composes nor re-derives the grouping, so it had no case to tell apart.
      case _ => GroupPartitionsExec(plan, joinKeyPositions = Some(positions))
    }
  }

  /** `plan` under a plain grouping node, or `plan` itself when its source is already grouped. */
  private def groupIfNeeded(plan: SparkPlan): SparkPlan = {
    // The source is already grouped when it reports one partition per key, and then there is
    // nothing to coalesce. Every keyed member of a partitioning shares one layout, so any of them
    // answers for the plan.
    val sourceIsGrouped =
      PartitioningCollection.representativeOf(plan.outputPartitioning).exists(_.isGrouped)
    if (sourceIsGrouped) plan else GroupPartitionsExec(plan)
  }

  /** The specs `plan` can offer for `required`. */
  private def candidatesFor(
      plan: SparkPlan,
      required: ClusteredDistribution): Seq[KeyedShuffleSpec] =
    createKeyedShuffleSpecs(plan.outputPartitioning, required)

  /**
   * Every spec a co-partitioned child can offer for the given distribution, one per
   * [[KeyedPartitioning]] in its partitioning that can serve it and passes the co-partition key
   * requirement below. That requirement is on by default and is what usually leaves a collection
   * with a single candidate. A [[PartitioningCollection]] yields them in member order, nested
   * collections included, and the caller picks. Returning only the first would decide by
   * enumeration order which member the join is planned on, and only the caller comparing the two
   * sides knows which member pairs with the other side's.
   *
   * A spec describes what the child's [[KeyedPartitioning]] reports once this rule has grouped it.
   * The question is `keysMaySatisfy` rather than `satisfies`, because the caller plans from the
   * child as the source reports it, before any `GroupPartitionsExec` of this rule's. A source that
   * reports several splits per partition key is not grouped, so it does not `satisfy` a
   * `ClusteredDistribution` until the node coalesces them, and gating on `satisfies` here would
   * refuse the very shape storage-partitioned join exists for.
   *
   * The spec is built from `toGrouped` for the same reason, but only for a source that is not
   * grouped: the join is then planned against the layout the node will emit, one partition per
   * distinct key, in the order `GroupPartitionsExec` sorts them. A grouped source gets no node, so
   * its spec keeps its own key order.
   */
  private def createKeyedShuffleSpecs(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Seq[KeyedShuffleSpec] = {
    def tryCreate(partitioning: KeyedPartitioning): Option[KeyedShuffleSpec] = {
      // The config requires all the cluster keys to be covered by the partition keys, to avoid
      // the skew of joining on keys that are coarser than the join keys. Key order and duplicated
      // cluster keys don't matter.
      def allClusterKeysCovered: Boolean =
        // The single-column invariant in KeyedPartitioning.supportsExpressions guarantees one
        // attribute per partition expression.
        distribution.allClusterKeysAmong(partitioning.expressions.flatMap(_.references))

      // The coverage requirement is a comparison of expressions, while `keysMaySatisfy` can end in
      // a projection of the partition keys, so the cheap question is asked first. The requirement
      // is on by default and turns most members away.
      if ((!SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION) ||
            allClusterKeysCovered) && partitioning.keysMaySatisfy(distribution)) {
        // `toGrouped` both dedups and sorts, and only a `GroupPartitionsExec` performs either. A
        // source that already reports one partition per key gets no node, so its own key order is
        // what the join will see and claiming the sorted one would be a lie. This is the whole
        // reason the two cases are told apart here rather than always grouping.
        val grouped = if (partitioning.isGrouped) partitioning else partitioning.toGrouped
        Some(grouped.createShuffleSpec(distribution))
      } else {
        None
      }
    }

    PartitioningCollection.flatten(partitioning).collect { case k: KeyedPartitioning => k }
      .flatMap(tryCreate)
  }

  /**
   * Merge, dedup and sort partitions keys for SPJ and optionally enable partition filtering.
   * Both sides must have matching partition expressions.
   *
   * @param leftPartitionKeys left side partition keys
   * @param rightPartitionKeys right side partition keys
   * @param joinType join type for optional partition filtering
   * @param keyOrdering ordering to sort partition keys
   * @param filterPartitions whether to narrow the merged list to the keys the join type allows.
   *                         The caller decides, see `filtersKeys` in `checkKeyGroupCompatible`.
   * @return merged and sorted partition values
   */
  def mergeAndDedupPartitions(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      joinType: JoinType,
      keyOrdering: Ordering[InternalRowComparableWrapper],
      filterPartitions: Boolean): Seq[InternalRowComparableWrapper] = {
    val merged = if (filterPartitions) {
      // Rows with matching join keys land in the same key group. If a group is absent from one
      // side, whether it can produce output depends on which side's unmatched rows the join
      // preserves. Only equi-joins reach this method, since every SMJ/SHJ takes its keys from
      // `ExtractEquiJoinKeys`. So a Cross join, e.g. `l CROSS JOIN r ON l.id = r.id`, is treated
      // like an Inner join: groups absent from either side cannot produce output.
      joinType match {
        // neither side keeps unmatched rows
        case _: InnerLike | LeftSemi =>
          mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys, intersect = true)
        // every left row is kept or tested
        case LeftOuter | LeftAnti | LeftSingle | ExistenceJoin(_) => leftPartitionKeys.distinct
        case RightOuter => rightPartitionKeys.distinct
        // FullOuter keeps both sides' unmatched rows; any other join type is not filtered
        case _ => mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
      }
    } else {
      mergeAndDedupPartitionKeys(leftPartitionKeys, rightPartitionKeys)
    }

    // SPARK-41471: We keep to order of partitions to make sure the order of
    // partitions is deterministic in different case.
    merged.sorted(keyOrdering)
  }

  private def mergeAndDedupPartitionKeys(
      leftPartitionKeys: Seq[InternalRowComparableWrapper],
      rightPartitionKeys: Seq[InternalRowComparableWrapper],
      intersect: Boolean = false) = {
    val leftKeySet = mutable.HashSet.from(leftPartitionKeys)
    val rightKeySet = mutable.HashSet.from(rightPartitionKeys)
    val result = if (intersect) {
      leftKeySet.intersect(rightKeySet)
    } else {
      leftKeySet.union(rightKeySet)
    }
    result.toSeq
  }

  /**
   * Splits a partitioning into the two questions the caller acts on, in this order:
   *
   * 1. does one of its non-[[KeyedPartitioning]] members (HashPartitioning, RangePartitioning,
   *    etc.) already satisfy `distribution`, in which case the child needs nothing
   * 2. and if not, how can a [[KeyedPartitioning]] member satisfy it. As it is (`Left`), or after a
   *    [[GroupPartitionsExec]] projecting to the given partition expression positions (`Right`,
   *    with `None` positions when the node only has to coalesce duplicate partition keys), or not
   *    at all (`None`)
   *
   * The order matters for more than tidiness. The first question touches no partition key, the
   * second projects them. And a `Left` is not the same answer as a satisfying non-keyed member,
   * because the `OrderedDistribution` arm has to look at the keys of the partitioning it gets.
   *
   * At most one `KeyedPartitioning` comes back, because the caller acts on a single one. Whichever
   * it takes, the child then satisfies the distribution and the rest of its partitioning is
   * irrelevant. A member that satisfies as it stands always comes back as a `Left`, since
   * `satisfies` is the strict question.
   *
   * That is the point of classifying by what still has to happen to the data rather than by how the
   * partitioning was built. An already grouped `KeyedPartitioning` can still need a
   * `GroupPartitionsExec`, because `v2BucketingAllowKeysSubsetOfPartitionKeys` lets it be grouped
   * on more keys than the distribution requires, and `isGrouped` only tells whether the *full*
   * partition keys are unique. Keeping both reasons in one answer leaves the caller a single
   * `ClusteredDistribution` arm that inserts the node, and one place that decides the projection.
   *
   * @param partitioning The partitioning to split
   * @param distribution The distribution to satisfy
   */
  private def splitKeyedPartitionings(
      partitioning: Partitioning,
      distribution: Distribution): (Boolean, Option[KeyedResolution]) = {
    val flattened = PartitioningCollection.flatten(partitioning)

    if (flattened.exists(p => !p.isInstanceOf[KeyedPartitioning] && p.satisfies(distribution))) {
      (true, None)
    } else {
      val keyed = flattened.collect { case k: KeyedPartitioning => k }
      (false, resolveKeyedPartitioning(keyed, distribution))
    }
  }

  /**
   * A [[KeyedPartitioning]] that can satisfy the distribution, and how it gets there.
   *
   * @param satisfiesAsIs whether it satisfies with no [[GroupPartitionsExec]] at all
   * @param positions the partition expression positions covering a cluster key, which is what a
   *                  node would project it to
   */
  private case class AdmittedMember(
      partitioning: KeyedPartitioning,
      satisfiesAsIs: Boolean,
      positions: BitSet)

  /**
   * How one of `keyedPartitionings` can satisfy `distribution`, or `None` when none of them can.
   * See `splitKeyedPartitionings`, which is the only caller.
   */
  private def resolveKeyedPartitioning(
      keyedPartitionings: Seq[KeyedPartitioning],
      distribution: Distribution): Option[KeyedResolution] = {
    // Which members can satisfy the distribution at all, and which of their partition expression
    // positions cover a cluster key. Nothing here touches a partition key.
    //
    // Both questions are asked, because neither implies the other. `satisfies` is the strict one,
    // and it also enforces `requiredNumPartitions`. `keysMaySatisfy` ignores the count and allows a
    // `GroupPartitionsExec` to coalesce duplicate partition keys. Neither subsumes the other:
    // `keysMaySatisfy` only ever answers the keyed questions, so it says no to the `AllTuples` and
    // `UnspecifiedDistribution` that `satisfies` inherits from `Partitioning`, while a non-grouped
    // member needs a node and so fails `satisfies`.
    //
    // Both go through `keysSatisfy` for a `ClusteredDistribution`, so it can be derived twice, but
    // only for a member this loop then rejects. Every other case short-circuits: a non-grouped one
    // never reaches it through `satisfies0`, a wrong partition count stops before `satisfies0`, and
    // when either `keysSatisfy` or `keysCanSatisfy`'s projection question says yes the `||` above
    // stops. What is left is a grouped member whose keys match neither way, and the second
    // derivation there reads a partition key only in the shape where the two position derivations
    // disagree, an expression that *is* a cluster key while its references are not.
    //
    // The positions are only computed for a member that can satisfy. For one that cannot, no
    // position would be covered, and an empty set means something else there.
    val admitted = keyedPartitionings.flatMap { k =>
      val satisfies = k.satisfies(distribution)
      if (satisfies || k.keysMaySatisfy(distribution)) {
        // Every position survives when the distribution names no cluster keys, since then
        // nothing needs projecting. Keeping a position is only sound because `keysSatisfy`'s
        // subset branch also requires one reference per expression, so a kept expression is a
        // function of a single cluster key and coalescing on the projected keys cannot put rows
        // that share a cluster key on different partitions.
        val positions = distribution match {
          case c: ClusteredDistribution => k.positionsCoveringClusterKeys(c)
          case _ => k.expressions.indices.to(BitSet)
        }
        // A member covering no position is skipped rather than projected onto nothing, which would
        // collapse every partition into one. It happens for a member that cannot satisfy at all,
        // and for one whose expressions have no references, which makes every `keysSatisfy` branch
        // vacuously true and which nothing at construction rejects.
        Option.when(positions.nonEmpty || k.expressions.isEmpty)(
          AdmittedMember(k, satisfies, positions))
      } else {
        None
      }
    }

    // A node is pointless when a member satisfies, and that is the whole test.
    // `KeyedPartitioning.satisfies` is strict, so it answers `false` for a member that only
    // satisfies once a node has projected its keys.
    admitted.find(_.satisfiesAsIs) match {
      case Some(member) =>
        // A member that needs no node settles the child, whatever the candidates would have
        // offered. `Left` and `Right` are qualified throughout, because `catalyst.expressions` has
        // its own.
        Some(scala.Left(member.partitioning))

      case None =>
        // Every admitted member needs a node, and every one of them satisfies the distribution
        // once it has one, so what is left is which one to build it from. They are the candidates,
        // keyed by the positions the node would project them to.
        //
        // One entry per distinct position set is enough, and the first member wins. The same set
        // projects to the same keys whichever member applies it, because `PartitioningCollection`
        // guarantees its members share the `partitionKeys` reference and their arity, so position
        // `i` addresses the same key column in all of them.
        //
        // `GroupPartitionsExec` re-derives the member independently, through
        // `PartitioningCollection.representativeOf`, so the member recorded here and the one used
        // at execution agree only because of that same guarantee,
        // `PartitioningCollection.checkKeyedPartitioningInvariant` and the value-equality interning
        // in `fromPartitionings` behind it. Relaxing the invariant means changing both places
        // together, not just this one. What the members may still differ in is how their key types
        // are named, since the collection only requires them to describe one key space. That does
        // not reach the keys, which are shared.
        //
        // The order is the child's, so when two sets leave the same number of partitions, the one
        // from the member the child reports first wins. That tie is the only thing the order
        // decides, and either winner satisfies the distribution. The two project to different keys
        // though, so the choice is visible in the plan.
        val candidates = admitted.map(m => m.partitioning -> m.positions).distinctBy(_._2)

        // A required partition count comes first, because a node derives its count from the keys
        // it is handed rather than from the operator. It filters the candidates rather than
        // vetoing the winner. One that would land on the required count must not lose to one that
        // cannot honour it and send the whole child to a shuffle instead.
        //
        // A member with fewer partitions than the count is refused without projecting anything,
        // since a projection merges partitions and never splits them.
        val eligible = distribution.requiredNumPartitions match {
          case Some(n) => candidates.filter { case (k, ps) =>
            k.numPartitions >= n && k.numPartitionsProjectedOn(ps) == n
          }
          case None => candidates
        }
        if (eligible.isEmpty) {
          // Either no member was admitted at all, or no projection leaves the required count. Both
          // send the caller to a shuffle, which can honour it.
          None
        } else {
          // Among the rest the choice is plan quality. Take the projection leaving the most
          // partitions.
          //
          // A position set contained in another one is dropped without ranking it. Projecting to
          // fewer positions can merge partitions but never split them, so a contained set can
          // never leave more partitions than the set containing it, and can only tie. Dropping it
          // therefore costs no parallelism, and on a tie it settles the choice toward the wider
          // set, which still names the keys the narrower one would have dropped.
          //
          // It saves projections, and the projection is the expensive step here, since
          // `KeyedPartitioning.projectKeys` allocates a row per input partition and
          // `InternalRowComparableWrapper.hashCode` is uncached, while the containment test is
          // not. That test is quadratic in the number of *distinct position sets*, not in the
          // number of members, both are small, and each pair is an int compare that rejects most
          // of them, then a word compare on a `BitSet`. Nothing in it touches a partition key.
          val maximal = eligible.filter { case (_, positions) =>
            !eligible.exists { case (_, o) => o.size > positions.size && positions.subsetOf(o) }
          }
          // With one candidate left there is nothing to rank, and no count is needed either. That
          // is the ordinary shape on the default config, so it is worth not projecting for it. A
          // required count settles it too, since the filter above kept only the sets leaving
          // exactly that many, so every survivor ties and `maxBy` would return the first anyway.
          val (kp, positions) =
            if (maximal.size == 1 || distribution.requiredNumPartitions.isDefined) {
              maximal.head
            } else {
              maximal.maxBy { case (k, ps) => k.numPartitionsProjectedOn(ps) }
            }

          // Satisfied after a node, whether that node has to coalesce duplicate partition keys,
          // project down to the cluster keys, or both. Every member that reaches here needs one,
          // since a member satisfying as it stands was taken by the arm above.
          Some(scala.Right(
            (kp, Option.when(positions.size < kp.expressions.length)(positions.toSeq))))
        }
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transformUp {
      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin, _, _)
          if optimizeOutRepartition &&
            (shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM) =>
        def hasSemanticEqualPartitioning(partitioning: Partitioning): Boolean = {
          partitioning match {
            case lower: HashPartitioning if upper.semanticEquals(lower) => true
            case lower: PartitioningCollection =>
              lower.partitionings.exists(hasSemanticEqualPartitioning)
            case _ => false
          }
        }
        if (hasSemanticEqualPartitioning(child.outputPartitioning)) {
          child
        } else {
          operator
        }

      case operator: SparkPlan =>
        val reordered = reorderJoinPredicates(operator)
        val newChildren = ensureDistributionAndOrdering(
          Some(reordered),
          reordered.children,
          reordered.requiredChildDistribution,
          reordered.requiredChildOrdering,
          ENSURE_REQUIREMENTS)
        reordered.withNewChildren(newChildren)
    }

    if (requiredDistribution.isDefined) {
      val shuffleOrigin = if (requiredDistribution.get.requiredNumPartitions.isDefined) {
        REPARTITION_BY_NUM
      } else {
        REPARTITION_BY_COL
      }
      val finalPlan = ensureDistributionAndOrdering(
        None,
        newPlan :: Nil,
        requiredDistribution.get :: Nil,
        Seq(Nil),
        shuffleOrigin)
      assert(finalPlan.size == 1)
      finalPlan.head
    } else {
      newPlan
    }
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, LeftExistence, LeftOuter, LeftSingle, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, KeyedPartitioning, Partitioning, PartitioningCollection, UnknownPartitioning, UnspecifiedDistribution}

/**
 * Holds common logic for join operators by shuffling two child relations
 * using the join keys.
 */
trait ShuffledJoin extends JoinCodegenSupport {
  def isSkewJoin: Boolean

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `ClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      // Every `KeyedPartitioning` in the joined collection speaks for the join output's layout,
      // and `groupedSatisfies` admits a marked side into it only for full-key join keys, where
      // the join equality ties every column of its claim: a row of an undeclared key matches
      // nothing on the accurate side and is filtered, so a marked member alongside an unmarked
      // one is spurious. Clear it at the one site that can mix them, so consumers take members'
      // markers at face value (see `KeyedPartitioning.mayContainUnknownPartitionKeys`).
      PartitioningCollection(
        clearUnknownPartitionKeys(Seq(left.outputPartitioning, right.outputPartitioning)))
    case LeftOuter | LeftSingle => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
  }

  /**
   * Clears the `mayContainUnknownPartitionKeys` marker of every `KeyedPartitioning` in
   * `partitionings` when marked and unmarked keyed inputs meet. Only `ShuffledJoin`'s
   * `InnerLike` arm can mix inputs this way; see the call site for the argument.
   */
  private def clearUnknownPartitionKeys(
      partitionings: Seq[Partitioning]): Seq[Partitioning] = {
    // Keyless inputs drop out of the `flatMap` rather than reading as unmarked, and a nested
    // collection answers with one marker per keyed member it holds. The all-unmarked path must
    // reach no `copy`: `transform`'s `fastEquals` would compare every partition key.
    def markersOf(p: Partitioning): Seq[Boolean] = p match {
      case k: KeyedPartitioning => k.mayContainUnknownPartitionKeys :: Nil
      case c: PartitioningCollection => c.partitionings.flatMap(markersOf)
      case _ => Nil
    }
    val markers = partitionings.flatMap(markersOf)
    if (markers.isEmpty || markers.forall(_ == markers.head)) {
      partitionings
    } else {
      partitionings.map {
        case partitioning: Partitioning with Expression if markersOf(partitioning).contains(true) =>
          partitioning.transform {
            case k: KeyedPartitioning if k.mayContainUnknownPartitionKeys =>
              k.copy(mayContainUnknownPartitionKeys = false)
          }.asInstanceOf[Partitioning]
        case p => p
      }
    }
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

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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, LeftExistence, LeftOuter, LeftSingle, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning, PartitioningCollection, UnknownPartitioning, UnspecifiedDistribution}
import org.apache.spark.sql.internal.SQLConf

/**
 * Holds common logic for join operators by shuffling two child relations
 * using the join keys.
 */
trait ShuffledJoin extends JoinCodegenSupport {
  def isSkewJoin: Boolean

  private lazy val canSpreadNullJoinKeys: Boolean = {
    // Only NULL keys on the preserved side can create this skew: they must be emitted, but
    // cannot satisfy ordinary equi-join predicates. Non-preserved NULL-keyed rows are filtered
    // out by `=` and never emitted, so their reducer placement does not matter here.
    //
    // Null-safe equality usually rewrites to non-null shuffle keys. The NullType corner can still
    // produce NULL shuffle keys, but shuffled join execution already treats those rows as
    // unmatched, so spreading them does not change the result.
    val preservedSideHasNullableKeys = joinType match {
      case LeftOuter => leftKeys.exists(_.nullable)
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
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter | LeftSingle => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
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

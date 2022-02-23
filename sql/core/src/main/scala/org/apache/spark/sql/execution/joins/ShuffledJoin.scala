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
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning, PartitioningCollection, UnknownPartitioning, UnspecifiedDistribution}

// md: sort-merge也是一种shuffledJoin，因为也是需要先hash、shuffle、再sort
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
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      // md: 相当于说，如果是一个有倾斜的join，强制重新分布数据来实现数据和计算平衡，从而防止某个task计算出现长尾
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      // md: 否则，必须要求两个child的分布是完全按照joinKeys（一部分）来聚集和排序的；这样后续才能做到左右两边的分区数相同
      ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      // md: inner like的join其分区，两边都可以（只要不空）；如果都不空的话从逻辑上看是先选择左表
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    // md： 为什么这里不确定了？因为full outer的过程，产生了很多并不遵循另一边表的分区为依据的数据
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
      case LeftOuter =>
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

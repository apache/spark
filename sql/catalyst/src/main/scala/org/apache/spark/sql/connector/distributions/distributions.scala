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

package org.apache.spark.sql.connector.distributions

import org.apache.spark.sql.connector.expressions.{Expression, SortOrder}

private[sql] object LogicalDistributions {
  val NO_REQUIREMENT_NUM_PARTITIONS: Int = 0

  def unspecified(): UnspecifiedDistribution = {
    UnspecifiedDistributionImpl
  }

  def clustered(clustering: Array[Expression]): ClusteredDistribution = {
    ClusteredDistributionImpl(clustering, NO_REQUIREMENT_NUM_PARTITIONS)
  }

  def clustered(clustering: Array[Expression], numPartitions: Int): ClusteredDistribution = {
    ClusteredDistributionImpl(clustering, numPartitions)
  }

  def ordered(ordering: Array[SortOrder]): OrderedDistribution = {
    OrderedDistributionImpl(ordering, NO_REQUIREMENT_NUM_PARTITIONS)
  }

  def ordered(ordering: Array[SortOrder], numPartitions: Int): OrderedDistribution = {
    OrderedDistributionImpl(ordering, numPartitions)
  }

  def representation(distName: String, exprs: Seq[Expression], numPartitions: Int): String = {
    val distributionStr = if (exprs.nonEmpty) {
      exprs.map(_.describe).mkString(", ")
    } else {
      ""
    }
    val numPartitionsStr = if (numPartitions > 0) {
      s"/ numPartitions=$numPartitions)"
    } else {
      ""
    }
    s"$distName($distributionStr $numPartitionsStr)"
  }
}

private[sql] object UnspecifiedDistributionImpl extends UnspecifiedDistribution {
  override def toString: String = "UnspecifiedDistribution"
}

private[sql] final case class ClusteredDistributionImpl(
    clusteringExprs: Seq[Expression],
    numPartitions: Int) extends ClusteredDistribution {

  override def clustering: Array[Expression] = clusteringExprs.toArray

  override def requiredNumPartitions(): Int = numPartitions

  override def toString: String = {
    LogicalDistributions.representation("ClusteredDistribution", clusteringExprs, numPartitions)
  }
}

private[sql] final case class OrderedDistributionImpl(
    orderingExprs: Seq[SortOrder],
    numPartitions: Int) extends OrderedDistribution {

  override def ordering: Array[SortOrder] = orderingExprs.toArray

  override def requiredNumPartitions(): Int = numPartitions

  override def toString: String = {
    LogicalDistributions.representation("OrderedDistribution", orderingExprs, numPartitions)
  }
}

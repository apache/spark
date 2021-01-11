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

  def unspecified(): UnspecifiedDistribution = {
    UnspecifiedDistributionImpl
  }

  def clustered(clustering: Array[Expression]): ClusteredDistribution = {
    ClusteredDistributionImpl(clustering)
  }

  def clustered(clustering: Array[Expression], numPartitions: Int): ClusteredDistribution = {
    ClusteredDistributionImpl(clustering, Some(numPartitions))
  }

  def ordered(ordering: Array[SortOrder]): OrderedDistribution = {
    OrderedDistributionImpl(ordering)
  }

  def ordered(ordering: Array[SortOrder], numPartitions: Int): OrderedDistribution = {
    OrderedDistributionImpl(ordering, Some(numPartitions))
  }
}

private[sql] object UnspecifiedDistributionImpl extends UnspecifiedDistribution {
  override def toString: String = "UnspecifiedDistribution"
}

private[sql] final case class ClusteredDistributionImpl(
    clusteringExprs: Seq[Expression],
    numPartitions: Option[Int] = None) extends ClusteredDistribution {

  override def clustering: Array[Expression] = clusteringExprs.toArray

  override def requiredNumPartitions(): Int = numPartitions.getOrElse(0)

  override def toString: String = {
    s"ClusteredDistribution(${clusteringExprs.map(_.describe).mkString(", ")}" +
      s" / numPartitions=$numPartitions)"
  }
}

private[sql] final case class OrderedDistributionImpl(
    orderingExprs: Seq[SortOrder],
    numPartitions: Option[Int] = None) extends OrderedDistribution {

  override def ordering: Array[SortOrder] = orderingExprs.toArray

  override def requiredNumPartitions(): Int = numPartitions.getOrElse(0)

  override def toString: String = {
    s"OrderedDistribution(${orderingExprs.map(_.describe).mkString(", ")} " +
      s"/ numPartitions=$numPartitions)"
  }
}

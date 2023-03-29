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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode}

abstract class AQERead extends UnaryExecNode {
  def child: SparkPlan
  def partitionSpecs: Seq[ShufflePartitionSpec]

  assert(partitionSpecs.nonEmpty, s"${getClass.getSimpleName} requires at least one partition")

  override final def output: Seq[Attribute] = child.output
  override final def supportsColumnar: Boolean = child.supportsColumnar
  override final def supportsRowBased: Boolean = child.supportsRowBased

  def outputPartitionWithCoalesced(numPartitions: Int): Partitioning = {
    // For coalesced shuffle read, the data distribution is not changed, only the number of
    // partitions is changed.
    child.outputPartitioning match {
      case h: HashPartitioning =>
        CurrentOrigin.withOrigin(h.origin)(h.copy(numPartitions = numPartitions))
      case r: RangePartitioning =>
        CurrentOrigin.withOrigin(r.origin)(r.copy(numPartitions = numPartitions))
      // This can only happen for `REBALANCE_PARTITIONS_BY_NONE`, which uses
      // `RoundRobinPartitioning` but we don't need to retain the number of partitions.
      case r: RoundRobinPartitioning =>
        r.copy(numPartitions = numPartitions)
      case other@SinglePartition =>
        throw new IllegalStateException(
          "Unexpected partitioning for coalesced shuffle read: " + other)
      case _ =>
        // Spark plugins may have custom partitioning and may replace this operator
        // during the postStageOptimization phase, so return UnknownPartitioning here
        // rather than throw an exception
        UnknownPartitioning(numPartitions)
    }
  }
}

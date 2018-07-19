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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.UUID

import org.apache.spark.{HashPartitioner, SparkEnv}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.continuous.shuffle.{ContinuousShuffleReadPartition, ContinuousShuffleReadRDD}

/**
 * Physical plan for coalescing a continuous processing plan.
 *
 * Currently, only coalesces to a single partition are supported. `numPartitions` must be 1.
 */
case class ContinuousCoalesceExec(numPartitions: Int, child: SparkPlan) extends SparkPlan {
  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = SinglePartition

  override def doExecute(): RDD[InternalRow] = {
    assert(numPartitions == 1)
    new ContinuousCoalesceRDD(
      sparkContext,
      numPartitions,
      conf.continuousStreamingExecutorQueueSize,
      sparkContext.getLocalProperty(ContinuousExecution.EPOCH_INTERVAL_KEY).toLong,
      child.execute())
  }
}

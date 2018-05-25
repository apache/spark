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

package org.apache.spark.sql.execution.streaming.continuous.shuffle

import org.apache.spark.{Partition, Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousExecution, EpochTracker}

/**
 * An RDD which continuously writes epochs from its child into a continuous shuffle.
 *
 * @param prev The RDD to write to the continuous shuffle.
 * @param outputPartitioner The partitioner on the reader side of the shuffle.
 * @param endpoints The [[UnsafeRowReceiver]] endpoints to write to. Indexed by partition ID within
 *                  outputPartitioner.
 */
class ContinuousShuffleWriteRDD(
    var prev: RDD[UnsafeRow],
    outputPartitioner: Partitioner,
    endpoints: Seq[RpcEndpointRef])
    extends RDD[Unit](prev) {

  override def getPartitions: Array[Partition] = prev.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Unit] = {
    EpochTracker.initializeCurrentEpoch(
      context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong)
    val writer: ContinuousShuffleWriter =
      new UnsafeRowWriter(split.index, outputPartitioner, endpoints.toArray)

    while (!context.isInterrupted() && !context.isCompleted()) {
      writer.write(prev.compute(split, context))
      EpochTracker.incrementCurrentEpoch()
    }

    Iterator()
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

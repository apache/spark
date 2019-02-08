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

import java.util.UUID

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.NextIterator

case class ContinuousShuffleReadPartition(
      index: Int,
      endpointName: String,
      queueSize: Int,
      numShuffleWriters: Int,
      epochIntervalMs: Long)
    extends Partition {
  // Initialized only on the executor, and only once even as we call compute() multiple times.
  lazy val (reader: ContinuousShuffleReader, endpoint) = {
    val env = SparkEnv.get.rpcEnv
    val receiver = new RPCContinuousShuffleReader(
      queueSize, numShuffleWriters, epochIntervalMs, env)
    val endpoint = env.setupEndpoint(endpointName, receiver)

    TaskContext.get().addTaskCompletionListener[Unit] { ctx =>
      env.stop(endpoint)
    }
    (receiver, endpoint)
  }
}

/**
 * RDD at the map side of each continuous processing shuffle task. Upstream tasks send their
 * shuffle output to the wrapped receivers in partitions of this RDD; each of the RDD's tasks
 * poll from their receiver until an epoch marker is sent.
 *
 * @param sc the RDD context
 * @param numPartitions the number of read partitions for this RDD
 * @param queueSize the size of the row buffers to use
 * @param numShuffleWriters the number of continuous shuffle writers feeding into this RDD
 * @param epochIntervalMs the checkpoint interval of the streaming query
 */
class ContinuousShuffleReadRDD(
    sc: SparkContext,
    numPartitions: Int,
    queueSize: Int = 1024,
    numShuffleWriters: Int = 1,
    epochIntervalMs: Long = 1000,
    val endpointNames: Seq[String] = Seq(s"RPCContinuousShuffleReader-${UUID.randomUUID()}"))
  extends RDD[UnsafeRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { partIndex =>
      ContinuousShuffleReadPartition(
        partIndex, endpointNames(partIndex), queueSize, numShuffleWriters, epochIntervalMs)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    split.asInstanceOf[ContinuousShuffleReadPartition].reader.read()
  }
}

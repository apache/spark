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

package org.apache.spark.shuffle.local.pipelined

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.{BaseShuffleHandle, PipelinedShuffleManager, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

/**
 * A pipelined shuffle manager whose writer -> reader transport is an in-process bounded
 * channel (see [[ChannelShuffleRendezvous]]) rather than the RPC streaming shuffle. It
 * serves a [[org.apache.spark.PipelinedShuffleDependency]] on a single executor, letting
 * the concurrent-stage scheduler run a shuffle's map and reduce stages at the same time
 * while records flow between them in memory -- the local-repartition v2 execution model.
 *
 * Selected via `spark.shuffle.manager.incremental`.
 *
 * Unlike the RPC streaming manager, this one needs no `StreamingShuffleOutputTracker`: it
 * finds each reader/writer pair through the JVM-local [[ChannelShuffleRendezvous]] rather
 * than a directory of writer host/port locations. It therefore declares
 * `usesStreamingShuffleOutputTracker = false`, so `SparkEnv` creates no tracker and the
 * scheduler registers the shuffle with none (a pipelined stage's availability is tracked on
 * the stage itself, not in any output tracker). This is why it implements the
 * `PipelinedShuffleManager` trait directly instead of subclassing the concrete streaming
 * manager.
 */
private[spark] class PipelinedChannelShuffleManager(conf: SparkConf)
  extends PipelinedShuffleManager {

  // Number of map tasks per shuffle, so a reader knows how many end-of-stream markers to
  // expect before it can finish. Populated at registration on the driver and read on the
  // executor; single-executor, so the same instance serves both.
  private val numMapsByShuffle = new ConcurrentHashMap[Int, Int]()

  override def usesStreamingShuffleOutputTracker: Boolean = false

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    numMapsByShuffle.put(shuffleId, dependency.rdd.partitions.length)
    new BaseShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    new ChannelShuffleWriter[K, V](handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // A reduce task reads the reduce-partition range [startPartition, endPartition). Core
    // ShuffledRDD uses width 1, but SQL's ShuffledRowRDD may coalesce several reduce
    // partitions into one reader task, so the reader must honor the whole range. Map-index
    // bounds are irrelevant to the channel transport (all map tasks share each partition's
    // queue). The final 5-arg getReader forwards here with the correct partition range.
    val h = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    new ChannelShuffleReader[K, C](
      h, startPartition, endPartition, numMapsByShuffle.get(h.shuffleId))
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    numMapsByShuffle.remove(shuffleId)
    ChannelShuffleRendezvous.removeShuffle(shuffleId)
    true
  }

  override def stop(): Unit = {}
}

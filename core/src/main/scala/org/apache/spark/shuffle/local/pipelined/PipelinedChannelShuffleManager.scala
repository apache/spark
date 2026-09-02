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

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.{BaseShuffleHandle, PipelinedShuffleManager, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

/**
 * A pipelined shuffle manager whose writer -> reader transport is an in-process bounded
 * channel (see [[ChannelShuffleRendezvous]]) rather than the RPC streaming shuffle. It
 * serves a [[org.apache.spark.PipelinedShuffleDependency]] on a single executor, letting
 * the concurrent-stage scheduler run a shuffle's map and reduce stages at the same time
 * while records flow between them in memory -- the in-process pipelined shuffle execution model.
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
 *
 * This manager deliberately keeps NO per-shuffle registry. An early version recorded each
 * shuffle's map-task count at registration and looked it up in getReader -- and lost it when
 * an unregisterShuffle arrived BETWEEN registration and the job running, which happens
 * legitimately: Dataset.rdd builds the RDD inside a SQL execution scope that ends (and, with
 * spark.sql.classic.shuffleDependency.fileCleanup.enabled, removes the shuffle from every
 * manager) before any job has run. The reader then saw a missing entry as numMaps = 0 and
 * silently under-read the channel. The count is instead stamped into the shuffle handle at
 * registration ([[ChannelShuffleHandle.numMaps]]): the handle travels with the dependency
 * into every task, a plain Int field survives task serialization (the dependency's own `rdd`
 * reference is @transient and is null inside a deserialized task), and no later unregister
 * can take it away.
 */
private[spark] class PipelinedChannelShuffleManager(conf: SparkConf)
  extends PipelinedShuffleManager {

  // The in-process rendezvous is JVM-local: on a multi-executor deployment each executor would
  // get its own empty queue map, and every reader would block forever on data written in some
  // other JVM -- a silent hang. Refuse to construct anywhere but local mode, so a
  // misconfiguration fails loudly at startup instead.
  require(org.apache.spark.util.Utils.isLocalMaster(conf),
    "PipelinedChannelShuffleManager is an in-process (single-JVM) transport and requires " +
      s"local mode; got master '${conf.get("spark.master", "")}'")

  // Rows accumulated per output partition before a batch is handed across the channel in one
  // queue operation. Batching amortizes the queue's per-operation lock cost; per-row hand-off
  // measured ~19x slower than a regular shuffle on a 20M-row repartition.
  private val batchSize = conf.get(config.SHUFFLE_PIPELINED_CHANNEL_BATCH_SIZE)

  // Per-queue depth in batches (backpressure bound + heap-residency knob). Set the process-wide
  // rendezvous from the conf at construction, before any writer/reader creates a queue.
  ChannelShuffleRendezvous.setCapacity(conf.get(config.SHUFFLE_PIPELINED_CHANNEL_QUEUE_CAPACITY))

  override def usesStreamingShuffleOutputTracker: Boolean = false

  // Records cross the channel as object references read by a concurrent consumer thread; the
  // SQL layer must detach each row from the producer's reused buffer before the writer sees it.
  override def requiresDetachedRecords: Boolean = true

  // The channel writer parks on a full bounded queue, so it needs the driver's
  // live-reduce-partition
  // hint to drop records routed to partitions no reader will drain (see ChannelShuffleWriter's
  // liveMask), and the per-run epoch to key its rendezvous.
  override def supportsLiveReducePartitionHints: Boolean = true

  // Answer from the rendezvous -- the state that would leak -- rather than from a registration
  // record: a shuffle unregistered BEFORE its job runs (Dataset.rdd under fileCleanup)
  // recreates its
  // queues lazily when the job does run, and a registry dropped at unregister would never list it
  // again, leaking those queues for the life of the context.
  override def holdsShuffle(shuffleId: Int): Boolean =
    ChannelShuffleRendezvous.holdsShuffle(shuffleId)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle =
    new ChannelShuffleHandle(shuffleId, dependency, dependency.rdd.partitions.length)

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    new ChannelShuffleWriter[K, V](
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, batchSize, metrics)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // A reduce task reads the reduce-partition range [startPartition, endPartition). The channel
    // transport serves exactly ONE reduce partition per reader task -- ChannelShuffleReader
    // require()s endPartition - startPartition == 1 (its class doc explains why a coalesced
    // multi-partition range cannot be drained safely). A width-1 spec is what actually arrives:
    // the SQL rules (EnablePipelinedShuffle / AQEEnablePipelinedShuffle) refuse to pipeline any
    // shuffle read by a CoalesceExec, and AQE also keeps a pipelined exchange out of a
    // ShuffleQueryStage so CoalesceShufflePartitions never coalesces it -- so both core ShuffledRDD
    // and SQL's ShuffledRowRDD hand a width-1 spec here. The require makes any future wider range
    // fail loud rather than deadlock. Map-index bounds are irrelevant (all map tasks share each
    // partition's queue). The final 5-arg getReader forwards here with the partition range.
    //
    // numMaps -- how many end-of-stream markers to expect per queue -- was stamped into the
    // handle at registration, never kept in mutable manager state (see class scaladoc).
    val h = handle.asInstanceOf[ChannelShuffleHandle[K, _, C]]
    new ChannelShuffleReader[K, C](h, startPartition, endPartition, h.numMaps, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    ChannelShuffleRendezvous.removeShuffle(shuffleId)
    true
  }

  // Called from SparkEnv.stop() when the SparkContext stops. Drop the process-wide rendezvous
  // state: it is keyed by (shuffleId, epoch, reducePartitionId) with no application scoping, so a
  // fresh SparkContext in the same JVM (a test fork, a REPL/notebook restart) would restart both
  // shuffle ids and epochs (jobIds) at 0 and could read rows or end-of-stream markers this context
  // left behind under the same key.
  override def stop(): Unit = ChannelShuffleRendezvous.clear()
}

/**
 * The channel manager's shuffle handle: a [[BaseShuffleHandle]] plus the producer's map-task
 * count, captured at registration while the dependency's (transient) RDD is still reachable.
 * The reader needs it to know how many end-of-stream markers close a queue.
 */
private[spark] class ChannelShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val numMaps: Int)
  extends BaseShuffleHandle(shuffleId, dependency)

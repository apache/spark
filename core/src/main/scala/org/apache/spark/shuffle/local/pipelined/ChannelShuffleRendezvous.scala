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

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

/**
 * Process-wide rendezvous between the map (writer) and reduce (reader) sides of an
 * in-process pipelined shuffle. One bounded queue exists per
 * `(shuffleId, reducePartitionId)`; every map task writing to a given reduce partition
 * shares the queue with the single reduce task that drains it. Queue elements are BATCHES
 * of records (`Array[AnyRef]` of pairs, see [[ChannelShuffleWriter]]) or the
 * [[EndOfStream]] marker, so the queue's per-operation lock cost is paid per batch, not
 * per row.
 *
 * This is correct only when producer and consumer tasks are co-resident in the same JVM,
 * i.e. a single executor (local mode). The concurrent-stage scheduler co-schedules the two
 * stages so both are running, but it does not by itself guarantee co-location; the
 * [[PipelinedChannelShuffleManager]] is only intended for single-executor deployments,
 * where co-location is automatic. Cross-executor pipelined shuffle is served by the RPC
 * streaming shuffle instead.
 *
 * The queues are bounded, so a fast producer blocks on `put` when the consumer lags --
 * this is the backpressure that keeps the pipelined hand-off memory-bounded.
 */
private[spark] object ChannelShuffleRendezvous {

  /**
   * Marker placed on a queue by each map task when it finishes writing to that reduce
   * partition. A reader stops once it has seen one marker per map task.
   */
  val EndOfStream: AnyRef = new AnyRef

  // Keyed by (shuffleId, reducePartitionId). Values are AnyRef because the queue carries
  // both record batches (Array[AnyRef]) and the EndOfStream marker.
  private val queues =
    new ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]()

  /**
   * Default per-queue capacity in BATCHES (not rows): with the default 1024-row batch this
   * bounds the in-flight hand-off at ~64K rows per reduce partition.
   */
  private val DefaultCapacity = 64

  /** The queue for one `(shuffleId, reducePartitionId)`, created on first access. */
  def queue(shuffleId: Int, reducePartitionId: Int): LinkedBlockingQueue[AnyRef] =
    queues.computeIfAbsent(
      (shuffleId, reducePartitionId),
      _ => new LinkedBlockingQueue[AnyRef](DefaultCapacity))

  /** Drop all queues for a shuffle once it is unregistered, releasing their memory. */
  def removeShuffle(shuffleId: Int): Unit = {
    val it = queues.keySet().iterator()
    while (it.hasNext) {
      if (it.next()._1 == shuffleId) it.remove()
    }
  }

  /** Visible for testing: drop every queue. */
  private[pipelined] def clearForTesting(): Unit = queues.clear()

  /** Visible for testing: number of live queues. */
  private[pipelined] def numQueuesForTesting: Int = queues.size()
}

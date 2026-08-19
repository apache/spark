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

  // (shuffleId, reducePartitionId) keys whose reader has departed (its reduce task finished)
  // and will drain no more. A writer stops feeding an abandoned partition and drops the rest.
  // This covers the LIVE-partition early-stop case (e.g. a LIMIT reader that pulled enough
  // and quit): without it the writer fills the partition's bounded queue and blocks forever.
  private val abandoned =
    java.util.concurrent.ConcurrentHashMap.newKeySet[(Int, Int)]()

  /**
   * Per-queue capacity in BATCHES (not rows), the backpressure bound and the heap-residency
   * knob (see spark.shuffle.pipelined.channel.queueCapacity). Set once by the channel manager at
   * construction from that conf; defaults to 64 (with the default 1024-row batch, ~64K rows per
   * reduce partition in flight) until a manager sets it. `@volatile` because the manager sets it
   * on the driver while writer/reader threads read it.
   */
  @volatile private var capacity = 64

  /** Set the per-queue capacity in batches. Called by the channel manager from its conf. */
  private[pipelined] def setCapacity(batches: Int): Unit = { capacity = batches }

  /** The queue for one `(shuffleId, reducePartitionId)`, created on first access. */
  def queue(shuffleId: Int, reducePartitionId: Int): LinkedBlockingQueue[AnyRef] =
    queues.computeIfAbsent(
      (shuffleId, reducePartitionId),
      _ => new LinkedBlockingQueue[AnyRef](capacity))

  /** Whether this reduce partition's reader has departed (see [[abandon]]). */
  def isAbandoned(shuffleId: Int, reducePartitionId: Int): Boolean =
    abandoned.contains((shuffleId, reducePartitionId))

  /**
   * Clear ALL abandoned marks for a shuffle. A pipelined producer is RE-RUN for the same
   * shuffleId within one query (a RangePartitioner sampling job then the main job; executeTake's
   * per-batch jobs), and abandonment is a PER-JOB fact -- a mark left by a previous run's reader
   * must not make the re-run's fresh writers think a partition is dead.
   *
   * This is called ONCE by the DAGScheduler when it submits the producer stage, i.e. BEFORE any
   * map task of the new run has started, so it can never race a concurrently running writer or
   * reader of the SAME run. (An earlier design cleared marks from inside each writer's write();
   * that raced across the run's own map tasks -- a late-starting map task erased a departure a
   * sibling's reader had legitimately recorded, re-hanging the writer. Clearing at the stage's
   * submission, the one point with no live task of that run, removes the race by construction.)
   * Queues are left intact -- only the marks are reset.
   */
  def clearAbandonedForShuffle(shuffleId: Int): Unit = {
    val it = abandoned.iterator()
    while (it.hasNext) {
      if (it.next()._1 == shuffleId) it.remove()
    }
  }

  /**
   * Mark a reduce partition abandoned: its reader task has finished and will drain no more.
   * Called from the reader's task-completion listener. Besides recording the flag (so the
   * writer stops feeding this partition), it DRAINS the queue to unblock a writer already
   * parked in a full-queue `put`: clearing capacity lets that put return, after which the
   * writer's next abandoned-check stops it cooperatively (no reliance on interrupt).
   */
  def abandon(shuffleId: Int, reducePartitionId: Int): Unit = {
    abandoned.add((shuffleId, reducePartitionId))
    val q = queues.get((shuffleId, reducePartitionId))
    if (q != null) q.clear()
  }

  /**
   * Drop all queues for a shuffle once it is unregistered, releasing their memory.
   *
   * A non-empty queue here is NORMAL, not a hazard: a reader may legitimately stop early
   * (e.g. LIMIT reads only the first rows and never drains the rest), so leftover elements
   * at unregister time are expected. Queue occupancy is therefore NOT a usable signal for
   * "unregistered mid-job", and this method makes no such check. The mid-job-unregister
   * hazard is handled by audit rather than a runtime sentinel -- see the class scaladoc.
   */
  def removeShuffle(shuffleId: Int): Unit = {
    val it = queues.keySet().iterator()
    while (it.hasNext) {
      if (it.next()._1 == shuffleId) it.remove()
    }
    // Clear this shuffle's abandoned marks too, so a later re-run of the same shuffleId
    // (executeTake shares one shuffleId across its per-batch jobs; each batch's stage is
    // removed and rebuilt) starts fresh rather than seeing the previous batch's marks.
    val ai = abandoned.iterator()
    while (ai.hasNext) {
      if (ai.next()._1 == shuffleId) ai.remove()
    }
  }

  /**
   * Drop every queue and every abandoned mark. Called when the owning manager stops (i.e. the
   * SparkContext stops): this object is process-wide and keyed only by (shuffleId,
   * reducePartitionId), so without this a new SparkContext in the same JVM -- a test fork, a
   * REPL/notebook restart -- would restart shuffle ids at 0 and its reduce tasks could drain
   * rows or end-of-stream markers the previous context left behind. Also the reset hook used by
   * tests between contexts.
   */
  private[spark] def clear(): Unit = {
    queues.clear()
    abandoned.clear()
  }

  /** Visible for testing: number of live queues. */
  private[pipelined] def numQueuesForTesting: Int = queues.size()
}

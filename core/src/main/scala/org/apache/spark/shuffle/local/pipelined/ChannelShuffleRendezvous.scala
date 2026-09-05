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

import org.apache.spark.{SparkContext, TaskContext}

/**
 * Process-wide rendezvous between the map (writer) and reduce (reader) sides of an
 * in-process pipelined shuffle. One bounded queue exists per
 * `(shuffleId, epoch, reducePartitionId)`; every map task writing to a given reduce partition
 * shares the queue with the single reduce task that drains it. Queue elements are BATCHES
 * of records (`Array[AnyRef]` of pairs, see [[ChannelShuffleWriter]]) or the
 * [[EndOfStream]] marker, so the queue's per-operation lock cost is paid per batch, not
 * per row.
 *
 * The `epoch` is the per-run id (the jobId, propagated to both the writer and the reader of
 * one gang via a job-level local property -- see `SparkContext.SPARK_PIPELINED_RUN_EPOCH`). A
 * shuffleId is RE-RUN within one query (a RangePartitioner sample job then the main job;
 * executeTake's per-batch jobs; a classic Dataset re-executing a reused plan), and each run is
 * a different job, hence a different epoch. Keying by epoch makes each run's queues and marks
 * PHYSICALLY separate: the new run never sees a partition whose reader never started in the old
 * run (whose queue still holds stale batches + end-of-stream markers), and a straggler writer
 * from an aborted run -- still looping because a task kill need not interrupt the thread -- can
 * only touch its OWN (old) epoch's queue, never the new run's. This replaces an earlier design
 * that reused one key per shuffleId and tried to reset shared marks between runs, which left
 * stale queues and raced stragglers.
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

  /**
   * The per-run epoch for the current task, read from the job-level local property the
   * DAGScheduler stamps on a pipelined job (`SparkContext.SPARK_PIPELINED_RUN_EPOCH` = jobId).
   * Both the writer and the reader of one gang read this, so they address the same per-run
   * queues. Absent (a core-RDD path that never sets it) defaults to 0; that is fine because such
   * a shuffleId is never re-run into a colliding second live run.
   */
  def epochOf(tc: TaskContext): Int =
    Option(tc)
      .flatMap(t => Option(t.getLocalProperty(SparkContext.SPARK_PIPELINED_RUN_EPOCH)))
      .map(_.toInt)
      .getOrElse(0)

  // State is nested by shuffleId FIRST, then keyed by (epoch, reducePartitionId) within it. The
  // outer level exists so the two per-shuffle operations the ContextCleaner drives -- holdsShuffle
  // and removeShuffle -- are O(1) map lookups instead of a scan of every live entry: holdsShuffle
  // now runs for EVERY shuffle cleaned in a feature-on session, including the regular prefix
  // shuffles this feature itself produces, so a flat key made that O(live entries) per cleanup.
  //
  // Queue values are AnyRef because a queue carries both record batches (Array[AnyRef]) and the
  // EndOfStream marker.
  private val queues =
    new ConcurrentHashMap[Int, ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]]()

  // (epoch, reducePartitionId) keys, per shuffleId, whose reader has departed (its reduce task
  // finished) and will drain no more. A writer stops feeding an abandoned partition and drops the
  // rest. This covers the LIVE-partition early-stop case (e.g. a LIMIT reader that pulled enough
  // and quit): without it the writer fills the partition's bounded queue and blocks forever.
  private val abandoned =
    new ConcurrentHashMap[Int, java.util.Set[(Int, Int)]]()

  /**
   * Per-queue capacity in BATCHES (not rows), the backpressure bound and the heap-residency
   * knob (see spark.shuffle.channel.queueCapacity). Set once by the channel manager at
   * construction from that conf; defaults to 64 (with the default 1024-row batch, ~64K rows per
   * reduce partition in flight) until a manager sets it. `@volatile` because the manager sets it
   * on the driver while writer/reader threads read it.
   */
  @volatile private var capacity = 64

  /** Set the per-queue capacity in batches. Called by the channel manager from its conf. */
  private[pipelined] def setCapacity(batches: Int): Unit = { capacity = batches }

  /** The queue for one `(shuffleId, epoch, reducePartitionId)`, created on first access. */
  def queue(shuffleId: Int, epoch: Int, reducePartitionId: Int): LinkedBlockingQueue[AnyRef] = {
    val perShuffle = queues.computeIfAbsent(
      shuffleId, _ => new ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]())
    perShuffle.computeIfAbsent(
      (epoch, reducePartitionId), _ => new LinkedBlockingQueue[AnyRef](capacity))
  }

  /** Whether this reduce partition's reader has departed for this run (see [[abandon]]). */
  def isAbandoned(shuffleId: Int, epoch: Int, reducePartitionId: Int): Boolean = {
    val marks = abandoned.get(shuffleId)
    marks != null && marks.contains((epoch, reducePartitionId))
  }

  /**
   * Mark a reduce partition abandoned: its reader task has finished and will drain no more.
   * Called from the reader's task-completion listener. Besides recording the flag (so the
   * writer stops feeding this partition), it DRAINS the queue to unblock a writer already
   * parked in a full-queue `put`: clearing capacity lets that put return, after which the
   * writer's next abandoned-check stops it cooperatively (no reliance on interrupt).
   */
  def abandon(shuffleId: Int, epoch: Int, reducePartitionId: Int): Unit = {
    abandoned
      .computeIfAbsent(shuffleId, _ => ConcurrentHashMap.newKeySet[(Int, Int)]())
      .add((epoch, reducePartitionId))
    val perShuffle = queues.get(shuffleId)
    if (perShuffle != null) {
      val q = perShuffle.get((epoch, reducePartitionId))
      if (q != null) q.clear()
    }
  }

  /**
   * Drop all queues and marks for a shuffle once it is unregistered, releasing their memory.
   * Removes entries for EVERY epoch of the shuffle: cleanup runs without an epoch, and a shuffle
   * may have left queues under several run epochs.
   *
   * A non-empty queue here is NORMAL, not a hazard: a reader may legitimately stop early
   * (e.g. LIMIT reads only the first rows and never drains the rest), so leftover elements
   * at unregister time are expected. Queue occupancy is therefore NOT a usable signal for
   * "unregistered mid-job", and this method makes no such check. The mid-job-unregister
   * hazard is handled by audit rather than a runtime sentinel -- see the class scaladoc.
   */
  def removeShuffle(shuffleId: Int): Unit = {
    // One atomic remove per map, dropping every epoch of the shuffle with it.
    queues.remove(shuffleId)
    abandoned.remove(shuffleId)
  }

  /**
   * Whether this rendezvous currently holds any state (a queue or an abandoned mark, under any
   * epoch) for `shuffleId`. This is the authoritative signal for the `ContextCleaner`'s
   * tracker-less cleanup arm: the queues are created lazily on first writer/reader access, so a
   * shuffle unregistered BEFORE its job runs (Dataset.rdd under fileCleanup) and then run
   * recreates its queues here even though the manager's registry no longer lists it. Keying
   * cleanup off the rendezvous (rather than the manager's registry) frees those recreated queues,
   * and is still false for a regular shuffle (never present here), so the arm stays scoped.
   */
  def holdsShuffle(shuffleId: Int): Boolean = {
    val perShuffle = queues.get(shuffleId)
    val marks = abandoned.get(shuffleId)
    (perShuffle != null && !perShuffle.isEmpty) || (marks != null && !marks.isEmpty)
  }

  /**
   * Drop every queue and every abandoned mark. Called when the owning manager stops (i.e. the
   * SparkContext stops): this object is process-wide and epochs (jobIds) restart at 0 in a new
   * SparkContext, so without this a fresh context in the same JVM -- a test fork, a REPL/notebook
   * restart -- could collide with rows or end-of-stream markers the previous context left behind
   * under the same (shuffleId, epoch, reducePartitionId). Also the reset hook used by tests
   * between contexts.
   */
  private[spark] def clear(): Unit = {
    queues.clear()
    abandoned.clear()
  }

  /** Visible for testing: number of live queues. */
  private[pipelined] def numQueuesForTesting: Int = {
    var n = 0
    val it = queues.values().iterator()
    while (it.hasNext) n += it.next().size()
    n
  }
}

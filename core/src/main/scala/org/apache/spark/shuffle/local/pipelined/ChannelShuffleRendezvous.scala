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

import org.apache.spark.{SparkContext, TaskContext, TaskKilledException}

/**
 * Bounded in-JVM channels, one per (shuffleId, job epoch, reduce partition). All map tasks
 * share a partition's queue with its single reader. Values are record batches or EndOfStream.
 * Each run uses separate state so tasks from an aborted run cannot reach a later run's queues.
 * The scheduler opens the epoch before launching tasks and closes it when the job ends.
 */
private[spark] object ChannelShuffleRendezvous {

  /**
   * Marker placed on a queue by each map task when it finishes writing to that reduce
   * partition. A reader stops once it has seen one marker per map task.
   */
  val EndOfStream: AnyRef = new AnyRef

  /** The job epoch propagated to both sides of a channel; standalone tests use epoch 0. */
  def epochOf(tc: TaskContext): Int =
    Option(tc)
      .flatMap(t => Option(t.getLocalProperty(SparkContext.SPARK_PIPELINED_RUN_EPOCH)))
      .map(_.toInt)
      .getOrElse(0)

  // Index by shuffle first so cleanup inspects only the requested shuffle.
  private val queues =
    new ConcurrentHashMap[Int, ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]]()

  // (epoch, reducePartitionId) keys, per shuffleId, whose reader has departed (its reduce task
  // finished) and will drain no more. A writer stops feeding an abandoned partition and drops the
  // rest. This covers the LIVE-partition early-stop case (e.g. a LIMIT reader that pulled enough
  // and quit): without it the writer fills the partition's bounded queue and blocks forever.
  private val abandoned =
    new ConcurrentHashMap[Int, java.util.Set[(Int, Int)]]()

  // Only active jobs are retained. Access and teardown serialize so a cancelled task cannot
  // recreate registered state after endRun. A task already holding a queue may finish against
  // that detached queue, but cannot affect a later epoch.
  private val activeRuns = ConcurrentHashMap.newKeySet[Int]()

  def startRun(epoch: Int): Unit = synchronized {
    activeRuns.add(epoch)
  }

  def endRun(epoch: Int): Unit = synchronized {
    activeRuns.remove(epoch)
    val queueIterator = queues.entrySet().iterator()
    while (queueIterator.hasNext) {
      val entry = queueIterator.next()
      entry.getValue.keySet().removeIf(_._1 == epoch)
      if (entry.getValue.isEmpty) queueIterator.remove()
    }
    val markIterator = abandoned.entrySet().iterator()
    while (markIterator.hasNext) {
      val entry = markIterator.next()
      entry.getValue.removeIf(_._1 == epoch)
      if (entry.getValue.isEmpty) markIterator.remove()
    }
  }

  private def runHasEnded(epoch: Int): Boolean =
    TaskContext.get() != null && !activeRuns.contains(epoch)

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
  def queue(
      shuffleId: Int,
      epoch: Int,
      reducePartitionId: Int): LinkedBlockingQueue[AnyRef] = {
    def checkRun(): Unit = {
      if (runHasEnded(epoch)) {
        throw new TaskKilledException("Pipelined shuffle run has ended")
      }
    }
    checkRun()
    val existing = queues.get(shuffleId)
    if (existing != null) {
      val q = existing.get((epoch, reducePartitionId))
      if (q != null) return q
    }
    synchronized {
      checkRun()
      val perShuffle = queues.computeIfAbsent(
        shuffleId, _ => new ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]())
      perShuffle.computeIfAbsent(
        (epoch, reducePartitionId), _ => new LinkedBlockingQueue[AnyRef](capacity))
    }
  }

  /** Whether this reduce partition's reader has departed for this run (see [[abandon]]). */
  def isAbandoned(shuffleId: Int, epoch: Int, reducePartitionId: Int): Boolean = {
    val marks = abandoned.get(shuffleId)
    runHasEnded(epoch) || (marks != null && marks.contains((epoch, reducePartitionId)))
  }

  /**
   * Mark a reduce partition abandoned: its reader task has finished and will drain no more.
   * Called from the reader's task-completion listener. Besides recording the flag (so the
   * writer stops feeding this partition), it DRAINS the queue to unblock a writer already
   * parked in a full-queue `put`: clearing capacity lets that put return, after which the
   * writer's next abandoned-check stops it cooperatively (no reliance on interrupt).
   */
  def abandon(shuffleId: Int, epoch: Int, reducePartitionId: Int): Unit = synchronized {
    if (runHasEnded(epoch)) return
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
   * Release idle state on unregister. SQL cleanup can run while another action still owns a
   * live epoch; removing its queues would disconnect its readers and writers. endRun releases
   * those entries when their owner finishes.
   */
  def removeShuffle(shuffleId: Int): Unit = synchronized {
    if (activeRuns.isEmpty) {
      queues.remove(shuffleId)
      abandoned.remove(shuffleId)
      return
    }
    val perShuffle = queues.get(shuffleId)
    if (perShuffle != null) {
      perShuffle.keySet().removeIf(key => !activeRuns.contains(key._1))
      if (perShuffle.isEmpty) queues.remove(shuffleId)
    }
    val marks = abandoned.get(shuffleId)
    if (marks != null) {
      marks.removeIf(key => !activeRuns.contains(key._1))
      if (marks.isEmpty) abandoned.remove(shuffleId)
    }
  }

  /** Whether there is transport state for the cleaner to release. */
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
  private[spark] def clear(): Unit = synchronized {
    activeRuns.clear()
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

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

import java.util.Arrays

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}

/**
 * Map-side of the in-process pipelined shuffle. Each input record is routed to the reduce
 * partition its key hashes to and accumulated in a per-partition batch; a FULL batch (an
 * `Array[AnyRef]` of `batchSize` pairs) is pushed onto that partition's shared queue in one
 * queue operation (see [[ChannelShuffleRendezvous]]) -- the consumer stage, running
 * concurrently, drains it batch by batch. No serialization, no disk, no network.
 *
 * Batching is what makes the transport viable for large unaggregated shuffles: the queue
 * costs a lock acquisition per operation (~hundreds of ns under producer/consumer
 * contention), so handing rows across one at a time costs that PER ROW -- measured at ~19x
 * slower than a regular shuffle on a 20M-row repartition. Batching divides the lock traffic
 * by `batchSize`, the same lesson as any object-batch transport. A batch
 * array is handed off to the consumer and never touched again by the writer (a fresh array
 * is allocated after each put), so ownership transfer is clean across threads.
 */
private[spark] class ChannelShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Long,
    batchSize: Int,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] with org.apache.spark.internal.Logging {

  require(batchSize > 0, s"batchSize must be positive, got $batchSize")

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = handle.shuffleId

  // Per-run epoch (the jobId), read from the job-level local property the DAGScheduler set for a
  // pipelined job. The reader of this gang reads the SAME value, so both address the same
  // per-run queues in the process-wide rendezvous; a re-run of this shuffleId is a different job
  // and gets a different epoch, keeping its queues physically separate. Absent (a core-RDD test
  // path that never sets it, and never re-runs a shuffleId concurrently) means epoch 0.
  private val runEpoch = ChannelShuffleRendezvous.epochOf(TaskContext.get())

  // The reduce partitions this job actually reads, from the producer stage's task property
  // (set by the DAGScheduler from the result stage's partitions). A record routed to a
  // partition NOT in this set has no consumer -- putting it would fill that partition's
  // bounded queue and, because the writer interleaves all partitions on one thread, block
  // the writer before it can feed even the read partitions or emit their end-of-stream,
  // deadlocking the job. So such records are dropped. Absent property (None) means every
  // partition is live (the normal full-read case: collect, count, a full-partition job) and
  // nothing is dropped.
  private val liveReducePartitions: Option[Set[Int]] =
    Option(TaskContext.get())
      .flatMap(tc =>
        Option(tc.getLocalProperty(SparkContext.SPARK_PIPELINED_LIVE_REDUCE_PARTITIONS)))
      .map(_.split(",").filter(_.nonEmpty).map(_.toInt).toSet)

  // Per-partition liveness, precomputed ONCE from the (static) live set: true iff a consumer
  // reads this reduce partition at all. This is the hot-path gate -- checked per input record --
  // so it is a plain Array[Boolean] load, not a boxed Set lookup: on a large repartition the
  // per-record path must not allocate (the transport's whole point is amortizing per-row cost).
  // Absent property means every partition is live. The OTHER half of "worth writing" --
  // abandonment, which happens at runtime when a reader departs early (e.g. LIMIT) -- is dynamic
  // and is checked where it matters (at hand-off, in putUnlessAbandoned), NOT per record:
  // accumulating a few more rows into an in-memory batch for a since-abandoned partition is
  // harmless because that batch is never put (putUnlessAbandoned drops it).
  private val liveMask: Array[Boolean] = {
    val mask = Array.fill(numPartitions)(true)
    liveReducePartitions.foreach { live =>
      var p = 0
      while (p < numPartitions) { mask(p) = live.contains(p); p += 1 }
    }
    mask
  }

  // Hand a batch to a partition's queue, but do NOT block forever if its reader departs:
  // poll with a short timeout and bail out the moment the partition becomes abandoned. This
  // is the cooperative unblock for the early-stop case -- abandon() also drains the queue to
  // release a parked put, and this re-check ensures the writer then stops rather than
  // re-filling. Returns false if the partition was abandoned before the batch was accepted.
  // On a successful hand-off, records the batch's records and the time spent (including any
  // backpressure wait) against the write metrics; a dropped/abandoned batch counts nothing,
  // since those records are never shuffled out. `records` is the number of pairs in `batch`
  // (a full batch is `batchSize`, a trimmed tail is shorter; the end-of-stream marker is 0).
  private def putUnlessAbandoned(pid: Int, batch: AnyRef, records: Int): Boolean = {
    val q = ChannelShuffleRendezvous.queue(shuffleId, runEpoch, pid)
    val start = System.nanoTime()
    while (!ChannelShuffleRendezvous.isAbandoned(shuffleId, runEpoch, pid)) {
      // Wake on a task kill even without thread interruption. `isAbandoned` is set only by a
      // reduce task that actually STARTED (its completion listener); if the reader for pid never
      // started -- the group aborts while this producer is already filling queues (an
      // unserializable consumer task, an early failure of another member, a job cancel) -- the
      // mark never appears and this offer loop would park forever, pinning the executor slot
      // (spark.job.interruptOnCancel defaults to false, so the kill does not interrupt the
      // thread). Checking the TaskContext interrupt flag each cycle is the symmetric escape to
      // the reader's takeItem.
      Option(TaskContext.get()).foreach(_.killTaskIfInterrupted())
      if (q.offer(batch, 100, java.util.concurrent.TimeUnit.MILLISECONDS)) {
        // A successful offer can race abandon(): abandon does `add(mark)` then `q.clear()`, so if
        // it ran between the isAbandoned check above and this offer, our batch lands AFTER the
        // clear and would be stranded in the queue (no reader will ever drain it). Re-check and
        // clear it ourselves so nothing is left behind. The reader has departed, so discarding is
        // correct; and it keeps the queue empty for removeShuffle rather than pinning a batch.
        if (ChannelShuffleRendezvous.isAbandoned(shuffleId, runEpoch, pid)) {
          q.clear()
          return false
        }
        if (records > 0) {
          writeMetrics.incRecordsWritten(records.toLong)
          writeMetrics.incWriteTime(System.nanoTime() - start)
        }
        return true
      }
    }
    false
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // No stale-state reset is needed here: this run's queues and abandoned marks are keyed by
    // runEpoch (the jobId), so an EARLIER run of this shuffleId (a RangePartitioner sampling job
    // then the main job; executeTake batches; a re-executed classic plan) used a different epoch
    // and its leftovers are physically separate -- this run starts against empty per-epoch state.

    // One in-progress batch per reduce partition, plus its fill count. A partition's batch array
    // is allocated LAZILY, on its first record (batches(pid) starts null), so a map task pays for
    // only the partitions it actually writes -- a wide shuffle (thousands of partitions) or a
    // partial read (liveMask leaves most partitions dead) does not eagerly allocate
    // numPartitions * batchSize empty slots up front.
    val batches = new Array[Array[AnyRef]](numPartitions)
    val sizes = new Array[Int](numPartitions)
    // Records skipped because their reduce partition has no consumer (see liveMask).
    // Reported at the end of write().
    var droppedRecords = 0L

    while (records.hasNext) {
      val rec = records.next()
      val pid = partitioner.getPartition(rec._1)
      // Only accumulate for partitions a consumer reads (liveMask). Abandonment is not checked
      // here -- it is handled at hand-off in putUnlessAbandoned (see liveMask's comment).
      if (!liveMask(pid)) {
        // This record is routed to a reduce partition the driver said no consumer reads, so it is
        // dropped -- see liveMask. Count it: dropping is CORRECT only if the live set was computed
        // correctly, and everything else here fails loudly (a wrong width fails a require, a
        // reader-less live partition hangs the writer), while an under-approximated live set would
        // instead lose rows quietly. A non-zero count at the end of a job whose result looks wrong
        // is the thread to pull.
        droppedRecords += 1
      } else {
        if (batches(pid) == null) batches(pid) = new Array[AnyRef](batchSize)
        // Records must already be detached from the producer's reused row buffers by the time
        // they reach here (the producer reuses its output UnsafeRow across iterations, and the
        // consumer reads on another thread). The copy is done in the SQL layer's
        // ShuffleWriteProcessor for the pipelined path -- where InternalRow.copy() is available
        // -- rather than here, because this class lives in `core` and cannot reference SQL rows.
        // `rec` is already a detached (key, value) pair, so batch it directly rather than
        // re-wrapping it in a fresh Tuple2 -- one fewer allocation per record on the hot loop.
        // Product2 is typed as Any; the concrete record is always a reference (a Tuple2 of the
        // pid and the row), so store it as AnyRef. The reader casts each element back to Product2.
        batches(pid)(sizes(pid)) = rec.asInstanceOf[AnyRef]
        sizes(pid) += 1
        if (sizes(pid) == batchSize) {
          putUnlessAbandoned(pid, batches(pid), batchSize)
          // Hand off ownership of the filled array to the consumer; the next record for this
          // partition re-allocates lazily (so a partition whose last record just filled a batch
          // does not allocate a fresh array it never uses).
          batches(pid) = null
          sizes(pid) = 0
        }
      }
    }

    // Flush partial batches (trimmed so the reader can iterate array length directly), then
    // signal end-of-stream to every partition still wanted, so each live reader can count
    // this map task as done. Same thread, same queue: data always precedes the marker. A
    // partition that is dead (no reader) or abandoned (reader departed) gets neither -- its
    // queue is left for removeShuffle to drop.
    var p = 0
    while (p < numPartitions) {
      if (liveMask(p)) {
        if (sizes(p) > 0) {
          putUnlessAbandoned(p, Arrays.copyOf(batches(p), sizes(p)), sizes(p))
        }
        // Re-check: the reader may have departed while the trimmed batch was being put.
        if (!ChannelShuffleRendezvous.isAbandoned(shuffleId, runEpoch, p)) {
          putUnlessAbandoned(p, ChannelShuffleRendezvous.EndOfStream, records = 0)
        }
      }
      p += 1
    }

    // Report what this task dropped. Correct for a partial read (those partitions have no reader),
    // but the only quiet failure mode on this path, so leave a trace: a wrong live set shows up
    // here as drops on a job whose result is short. Not a metric -- shuffleWrite counters mean
    // "bytes/records that crossed the transport", which these did not.
    if (droppedRecords > 0) {
      logDebug(s"Pipelined shuffle $shuffleId map $mapId dropped $droppedRecords record(s) " +
        s"routed " +
        s"to reduce partitions with no consumer (live set: " +
        s"${liveReducePartitions.map(_.toArray.sorted.mkString(",")).getOrElse("all")})")
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    // A pipelined reducer never reads partition lengths, but the ShuffleWriter contract
    // still requires a MapStatus. Return an all-zero placeholder, mirroring the RPC
    // streaming writer.
    Some(MapStatus(
      SparkEnv.get.blockManager.shuffleServerId,
      Array.fill(numPartitions)(0L),
      mapId))
  }

  override def getPartitionLengths(): Array[Long] = Array.fill(numPartitions)(0L)
}

/**
 * Reduce-side of the in-process pipelined shuffle. Drains the shared queue for this reduce
 * partition batch by batch, handing rows to the consumer stage as the map tasks produce
 * them, until every map task has signalled end-of-stream.
 *
 * `numMaps` is the number of map tasks feeding this shuffle; the reader stops after it has
 * observed that many [[ChannelShuffleRendezvous.EndOfStream]] markers on its queue.
 *
 * ONE reduce partition per reader task ONLY: `endPartition - startPartition` must be 1. The
 * channel transport cannot serve a coalesced multi-partition range. The reader would have to
 * drain the range's queues in some order, but the map-side writer interleaves all partitions
 * on ONE thread and blocks on a full bounded queue; if the reader drains partition `start` to
 * completion before touching `start+1` while the writer has parked filling `start+1`, the two
 * deadlock with no timeout escape. The SQL layer keeps a coalesced spec from ever reaching a
 * pipelined dependency: `EnablePipelinedShuffle` / `AQEEnablePipelinedShuffle` refuse to
 * pipeline any shuffle read by a `CoalesceExec` (leaving it regular), and AQE also keeps a
 * pipelined exchange out of any ShuffleQueryStage so CoalesceShufflePartitions never coalesces
 * it -- so both the AQE and non-AQE readers use width-1 CoalescedPartitionSpec(i, i+1). The
 * `require` below makes that a hard, fail-loud invariant rather than a silent hang if a future
 * change ever lets a coalesced spec reach a pipelined dependency.
 */
private[spark] class ChannelShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    numMaps: Int,
    readMetrics: ShuffleReadMetricsReporter)
  extends ShuffleReader[K, C] {

  require(endPartition - startPartition == 1,
    s"ChannelShuffleReader supports exactly one reduce partition per task, got " +
      s"[$startPartition, $endPartition); the in-process channel transport does not support " +
      "coalesced multi-partition reads (see class doc).")

  // Per-run epoch (the jobId), the same value the writer of this gang reads, so both address the
  // same per-run queues in the process-wide rendezvous. See ChannelShuffleRendezvous.epochOf.
  private val runEpoch = ChannelShuffleRendezvous.epochOf(TaskContext.get())

  // On task completion (normal end, early stop like LIMIT, or failure) mark this reader's
  // partition as abandoned, so a writer still feeding it stops and does not wedge on its bounded
  // queue. Registered once here; fires whether or not the iterator was drained to the end.
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      var p = startPartition
      while (p < endPartition) {
        ChannelShuffleRendezvous.abandon(handle.shuffleId, runEpoch, p)
        p += 1
      }
    }
  }

  override def read(): Iterator[Product2[K, C]] =
    (startPartition until endPartition).iterator.flatMap(drainQueue)

  private def drainQueue(reducePartitionId: Int): Iterator[Product2[K, C]] = {
    val q = ChannelShuffleRendezvous.queue(handle.shuffleId, runEpoch, reducePartitionId)
    new Iterator[Product2[K, C]] {
      // The current batch being handed out, and the cursor into it. A null batch after
      // advance() means every map task has signalled end-of-stream: iteration is over.
      private var batch: Array[AnyRef] = _
      private var pos = 0
      private var endOfStreamSeen = 0
      advance()

      // Interruptibly wait for the next queue item. `LinkedBlockingQueue.take()` blocks
      // UNINTERRUPTIBLY here: if this reader's producers die (a map task throws before emitting
      // end-of-stream for this partition), the DAGScheduler aborts the group and kills this
      // reduce task, but the kill sets the interrupt flag on the TaskContext WITHOUT necessarily
      // interrupting the thread (spark.job.interruptOnCancel defaults to false), so a plain
      // take() would park forever and pin the executor slot for the app's life. Poll with a
      // short timeout and check the TaskContext's interrupt flag each cycle -- the symmetric
      // cooperative escape to the writer's putUnlessAbandoned. Time spent parked here is the
      // read-side backpressure signal (producer slower than consumer), so it is reported as
      // fetch-wait time.
      private def takeItem(): AnyRef = {
        var item: AnyRef = null
        // Time the WHOLE wait and convert once, the way ShuffleBlockFetcherIterator's
        // withFetchWaitTimeTracked does. Converting each poll separately truncated every
        // sub-millisecond wait to zero -- and a normal hand-off returns in microseconds -- so a
        // consumer that really was waiting reported ~0, inverting what this metric is for.
        val start = System.nanoTime()
        while (item == null) {
          Option(TaskContext.get()).foreach(_.killTaskIfInterrupted())
          item = q.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
        }
        readMetrics.incFetchWaitTime(
          java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
        item
      }

      // Blocking-drain until the next non-empty data batch, or until every map task has
      // signalled end-of-stream for this queue (then leave `batch` null to end iteration).
      private def advance(): Unit = {
        batch = null
        pos = 0
        // A producer with zero map tasks (numMaps == 0, e.g. a pipelined shuffle over an empty
        // RDD) enqueues nothing and no end-of-stream marker ever arrives; without this guard the
        // wait below would never terminate. Terminate immediately with an empty iterator. The
        // check is also correct for numMaps > 0 once every marker has been seen (advance is not
        // called again after batch stays null, but this keeps the invariant explicit).
        if (endOfStreamSeen >= numMaps) return
        var item = takeItem()
        while (item eq ChannelShuffleRendezvous.EndOfStream) {
          endOfStreamSeen += 1
          if (endOfStreamSeen >= numMaps) return
          item = takeItem()
        }
        batch = item.asInstanceOf[Array[AnyRef]]
        // Count the records handed to the consumer as this batch is fetched. Local, so this
        // is the read-side records metric; there is no remote fetch and no wire bytes.
        readMetrics.incRecordsRead(batch.length.toLong)
      }

      override def hasNext: Boolean = batch != null

      override def next(): Product2[K, C] = {
        if (batch == null) throw new NoSuchElementException
        val cur = batch(pos).asInstanceOf[Product2[K, C]]
        pos += 1
        // Writers only enqueue non-empty batches, so exhausting one means fetching the next.
        if (pos == batch.length) advance()
        cur
      }
    }
  }
}

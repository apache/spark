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
 * by `batchSize`, the same lesson as local-repartition v1's object-batch transport. A batch
 * array is handed off to the consumer and never touched again by the writer (a fresh array
 * is allocated after each put), so ownership transfer is clean across threads.
 */
private[spark] class ChannelShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Long,
    batchSize: Int,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V] {

  require(batchSize > 0, s"batchSize must be positive, got $batchSize")

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = handle.shuffleId

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

  // A partition is worth writing only while a consumer still wants its data: it must be in
  // the job's live set (Half 1: no-reader partitions), AND its reader must not have departed
  // (Half 2: a live reader that stopped early, e.g. LIMIT). `wants` is re-checked as data
  // flows because abandonment happens at runtime when the reader task completes.
  private def wants(pid: Int): Boolean =
    liveReducePartitions.forall(_.contains(pid)) &&
      !ChannelShuffleRendezvous.isAbandoned(shuffleId, pid)

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
    val q = ChannelShuffleRendezvous.queue(shuffleId, pid)
    val start = System.nanoTime()
    while (!ChannelShuffleRendezvous.isAbandoned(shuffleId, pid)) {
      if (q.offer(batch, 100, java.util.concurrent.TimeUnit.MILLISECONDS)) {
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
    // This is a fresh producer attempt. Clear any abandoned marks left on our partitions by
    // an EARLIER job that reused this shuffleId (RangePartitioner sampling job -> main job;
    // executeTake batches) so we do not mistake a prior reader's departure for our own
    // partitions being dead. Only abandonment happening during THIS attempt then counts.
    var pc = 0
    while (pc < numPartitions) {
      ChannelShuffleRendezvous.clearAbandoned(shuffleId, pc)
      pc += 1
    }

    // One in-progress batch per reduce partition, plus its fill count.
    val batches = Array.fill(numPartitions)(new Array[AnyRef](batchSize))
    val sizes = new Array[Int](numPartitions)

    while (records.hasNext) {
      val rec = records.next()
      val pid = partitioner.getPartition(rec._1)
      // Only accumulate for partitions a consumer still wants (see `wants`).
      if (wants(pid)) {
        // Records must already be detached from the producer's reused row buffers by the time
        // they reach here (the producer reuses its output UnsafeRow across iterations, and the
        // consumer reads on another thread). The copy is done in the SQL layer's
        // ShuffleWriteProcessor for the pipelined path -- where InternalRow.copy() is available
        // -- rather than here, because this class lives in `core` and cannot reference SQL rows,
        // and the UnsafeRow serializer offers no single-object copy. So batch the pair as-is.
        batches(pid)(sizes(pid)) = (rec._1, rec._2)
        sizes(pid) += 1
        if (sizes(pid) == batchSize) {
          putUnlessAbandoned(pid, batches(pid), batchSize)
          batches(pid) = new Array[AnyRef](batchSize)
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
      if (wants(p)) {
        if (sizes(p) > 0) {
          putUnlessAbandoned(p, Arrays.copyOf(batches(p), sizes(p)), sizes(p))
        }
        // Re-check: the reader may have departed while the trimmed batch was being put.
        if (!ChannelShuffleRendezvous.isAbandoned(shuffleId, p)) {
          putUnlessAbandoned(p, ChannelShuffleRendezvous.EndOfStream, records = 0)
        }
      }
      p += 1
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
 */
private[spark] class ChannelShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    numMaps: Int,
    readMetrics: ShuffleReadMetricsReporter)
  extends ShuffleReader[K, C] {

  // A reduce task may be asked to read a RANGE of reduce partitions in one go, not just one:
  // ShuffledRowRDD's CoalescedPartitionSpec(start, end) collapses several reduce partitions
  // into a single reader task. Drain every queue in [startPartition, endPartition); each
  // carries numMaps EndOfStream markers (the writer puts one per map task on every partition
  // queue), so each queue is drained independently to completion.
  //
  // On task completion (normal end, early stop like LIMIT, or failure) mark every partition
  // this reader owned as abandoned, so a writer still feeding them stops and does not wedge
  // on their bounded queues. Registered once here; fires whether or not the iterator was
  // drained to the end.
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      var p = startPartition
      while (p < endPartition) {
        ChannelShuffleRendezvous.abandon(handle.shuffleId, p)
        p += 1
      }
    }
  }

  override def read(): Iterator[Product2[K, C]] =
    (startPartition until endPartition).iterator.flatMap(drainQueue)

  private def drainQueue(reducePartitionId: Int): Iterator[Product2[K, C]] = {
    val q = ChannelShuffleRendezvous.queue(handle.shuffleId, reducePartitionId)
    new Iterator[Product2[K, C]] {
      // The current batch being handed out, and the cursor into it. A null batch after
      // advance() means every map task has signalled end-of-stream: iteration is over.
      private var batch: Array[AnyRef] = _
      private var pos = 0
      private var endOfStreamSeen = 0
      advance()

      // Blocking-drain until the next non-empty data batch, or until every map task has
      // signalled end-of-stream for this queue (then leave `batch` null to end iteration).
      private def advance(): Unit = {
        batch = null
        pos = 0
        var item = q.take()
        while (item eq ChannelShuffleRendezvous.EndOfStream) {
          endOfStreamSeen += 1
          if (endOfStreamSeen >= numMaps) return
          item = q.take()
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

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

import org.apache.spark.SparkEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader, ShuffleWriter}

/**
 * Map-side of the in-process pipelined shuffle. Each input record is routed to the reduce
 * partition its key hashes to and pushed onto that partition's shared queue (see
 * [[ChannelShuffleRendezvous]]) as it is produced -- the consumer stage, running
 * concurrently, reads it immediately. No serialization, no disk, no network.
 *
 * The pair is copied before being enqueued because upstream operators reuse their output
 * row/pair buffers across a stage, and the consumer reads on a different thread.
 */
private[spark] class ChannelShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Long)
  extends ShuffleWriter[K, V] {

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = handle.shuffleId

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    while (records.hasNext) {
      val rec = records.next()
      val pid = partitioner.getPartition(rec._1)
      // Records must already be detached from the producer's reused row buffers by the time
      // they reach here (the producer reuses its output UnsafeRow across iterations, and the
      // consumer reads on another thread). The copy is done in the SQL layer's
      // ShuffleWriteProcessor for the pipelined path -- where InternalRow.copy() is available
      // -- rather than here, because this class lives in `core` and cannot reference SQL rows,
      // and the UnsafeRow serializer offers no single-object copy. So enqueue as-is.
      ChannelShuffleRendezvous.queue(shuffleId, pid).put((rec._1, rec._2))
    }
    // Signal end-of-stream to every reduce partition so each reader can count this map
    // task as done.
    var p = 0
    while (p < numPartitions) {
      ChannelShuffleRendezvous.queue(shuffleId, p).put(ChannelShuffleRendezvous.EndOfStream)
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
 * partition, handing rows to the consumer stage as the map tasks produce them, until every
 * map task has signalled end-of-stream.
 *
 * `numMaps` is the number of map tasks feeding this shuffle; the reader stops after it has
 * observed that many [[ChannelShuffleRendezvous.EndOfStream]] markers on its queue.
 */
private[spark] class ChannelShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    numMaps: Int)
  extends ShuffleReader[K, C] {

  // A reduce task may be asked to read a RANGE of reduce partitions in one go, not just one:
  // ShuffledRowRDD's CoalescedPartitionSpec(start, end) collapses several reduce partitions
  // into a single reader task. Drain every queue in [startPartition, endPartition); each
  // carries numMaps EndOfStream markers (the writer puts one per map task on every partition
  // queue), so each queue is drained independently to completion.
  override def read(): Iterator[Product2[K, C]] =
    (startPartition until endPartition).iterator.flatMap(drainQueue)

  private def drainQueue(reducePartitionId: Int): Iterator[Product2[K, C]] = {
    val q = ChannelShuffleRendezvous.queue(handle.shuffleId, reducePartitionId)
    new Iterator[Product2[K, C]] {
      private var endOfStreamSeen = 0
      private var nextItem: AnyRef = advance()

      // Blocking-drain until the next data pair, or until every map task has signalled
      // end-of-stream for this queue (then return null to end iteration).
      private def advance(): AnyRef = {
        var item = q.take()
        while (item eq ChannelShuffleRendezvous.EndOfStream) {
          endOfStreamSeen += 1
          if (endOfStreamSeen >= numMaps) return null
          item = q.take()
        }
        item
      }

      override def hasNext: Boolean = nextItem != null

      override def next(): Product2[K, C] = {
        if (nextItem == null) throw new NoSuchElementException
        val cur = nextItem.asInstanceOf[Product2[K, C]]
        nextItem = advance()
        cur
      }
    }
  }
}

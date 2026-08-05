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

package org.apache.spark.shuffle.proto

// THROWAWAY PROTOTYPE (SPARK-57399 investigation, 2026-08-05). Not for merge.
// Proves that concurrent-stage pipelined scheduling can move data through an in-process
// bounded-channel transport (no Netty, single executor) instead of the RTM streaming
// shuffle's RPC transport. Does NOT touch any existing local-repartition code.
//
// See plans/LOCAL_REPARTITION_PIPELINED_SHUFFLE_FEASIBILITY.md for context. This file is
// meant to be deleted after the prototype question is answered.

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.streaming.StreamingShuffleManager

/**
 * JVM-static rendezvous: one bounded queue per (shuffleId, reducePartitionId). Valid on a
 * single executor (local[k]) because all map/reduce tasks are threads in one JVM. A
 * sentinel object marks end-of-stream per writer; the reducer stops once it has seen one
 * sentinel from every map task.
 */
private[proto] object ChannelRendezvous {
  val Sentinel: AnyRef = new AnyRef

  private val queues = new ConcurrentHashMap[(Int, Int), LinkedBlockingQueue[AnyRef]]()

  def queue(shuffleId: Int, reduceId: Int): LinkedBlockingQueue[AnyRef] =
    queues.computeIfAbsent((shuffleId, reduceId), _ => new LinkedBlockingQueue[AnyRef](64))

  def clear(): Unit = queues.clear()
}

private[proto] class ChannelShuffleWriter[K, V](
    handle: BaseShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext)
  extends ShuffleWriter[K, V] {

  private val dep = handle.dependency
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    while (records.hasNext) {
      val rec = records.next()
      val pid = partitioner.getPartition(rec._1)
      // Copy the pair defensively: upstream reuses row/pair buffers across a stage. Same
      // reason the real local-repartition sender copies.
      ChannelRendezvous.queue(handle.shuffleId, pid).put((rec._1, rec._2))
    }
    // End-of-stream sentinel to every reducer, so each reader knows this map task is done.
    var p = 0
    while (p < numPartitions) {
      ChannelRendezvous.queue(handle.shuffleId, p).put(ChannelRendezvous.Sentinel)
      p += 1
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    // Placeholder MapStatus: a pipelined reducer never reads partition lengths, but the
    // ShuffleWriter contract still requires a status. Mirror StreamingShuffleWriter.stop.
    Some(MapStatus(
      SparkEnv.get.blockManager.shuffleServerId,
      Array.fill(numPartitions)(0L),
      mapId))
  }

  override def getPartitionLengths(): Array[Long] = Array.fill(numPartitions)(0L)
}

private[proto] class ChannelShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    numMaps: Int)
  extends ShuffleReader[K, C] {

  override def read(): Iterator[Product2[K, C]] = {
    // ShuffledRDD reads exactly one reduce partition: [startPartition, startPartition+1).
    val q = ChannelRendezvous.queue(handle.shuffleId, startPartition)
    new Iterator[Product2[K, C]] {
      private var sentinelsSeen = 0
      private var nextItem: AnyRef = advance()

      private def advance(): AnyRef = {
        // Drain until a real element, or until every map task's sentinel has arrived.
        var item = q.take()
        while (item eq ChannelRendezvous.Sentinel) {
          sentinelsSeen += 1
          if (sentinelsSeen >= numMaps) return null
          item = q.take()
        }
        item
      }

      override def hasNext: Boolean = nextItem != null

      override def next(): Product2[K, C] = {
        val cur = nextItem.asInstanceOf[Product2[K, C]]
        nextItem = advance()
        cur
      }
    }
  }
}

/**
 * Subclass StreamingShuffleManager (not the bare PipelinedShuffleManager trait) so that
 * SparkEnv.initializeStreamingShuffleOutputTracker creates the tracker the pipelined
 * DAGScheduler path requires -- see the CAVEAT in the feasibility doc. Only the transport
 * (getWriter/getReader) is overridden; registration/tracker wiring is inherited.
 */
private[spark] class PipelinedChannelShuffleManager(conf: SparkConf)
  extends StreamingShuffleManager {

  // numMaps per shuffle, so the reducer knows how many end-of-stream sentinels to expect.
  private val mapCounts = new ConcurrentHashMap[Int, Int]()

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    mapCounts.put(shuffleId, dependency.rdd.partitions.length)
    new BaseShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    new ChannelShuffleWriter[K, V](
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val h = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    new ChannelShuffleReader[K, C](h, startPartition, endPartition, mapCounts.get(h.shuffleId))
  }
}

/** ShuffledRDD that emits a PipelinedShuffleDependency instead of a regular one. */
private[proto] class PipelinedShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends ShuffledRDD[K, V, C](prev, part) {

  override def getDependencies: Seq[org.apache.spark.Dependency[_]] = {
    val serializer: Serializer = SparkEnv.get.serializer
    List(new org.apache.spark.PipelinedShuffleDependency[K, V, C](
      prev.asInstanceOf[RDD[_ <: Product2[K, V]]], part, serializer))
  }

  // Pipelined shuffle is not registered with the MapOutputTracker; skip locality hints
  // (the inherited impl casts to MapOutputTrackerMaster for a shuffle that isn't there).
  override def getPreferredLocations(partition: org.apache.spark.Partition): Seq[String] = Nil
}

class ChannelPipelinedShufflePrototypeSuite extends SparkFunSuite {

  test("PROTOTYPE: repartition through an in-process channel-backed pipelined shuffle") {
    ChannelRendezvous.clear()
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("channel-pipelined-prototype")
      .set("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.proto.PipelinedChannelShuffleManager")
      // Keep the pipelined job admissible: no speculation / dynamic allocation / barrier.
      .set("spark.speculation", "false")
    val sc = new SparkContext(conf)
    try {
      val n = 1000
      val numOut = 4
      val keyed: RDD[(Int, Int)] =
        sc.parallelize(0 until n, 3).map(v => (v % numOut, v))
      val repartitioned =
        new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(numOut))

      // Collect (pid-by-index, value) to verify both correctness and correct routing.
      val out = repartitioned.mapPartitionsWithIndex { (idx, it) =>
        it.map { case (k, v) => (idx, k, v) }
      }.collect()

      // 1. No data lost or duplicated.
      assert(out.map(_._3).toSet === (0 until n).toSet,
        s"expected all $n values exactly once through the channel transport")
      // 2. Every row landed in the partition its key hashes to (routing correct).
      out.foreach { case (idx, k, _) =>
        assert(new HashPartitioner(numOut).getPartition(k) === idx,
          s"key $k routed to partition $idx, expected ${new HashPartitioner(numOut).getPartition(k)}")
      }
    } finally {
      sc.stop()
      ChannelRendezvous.clear()
    }
  }
}

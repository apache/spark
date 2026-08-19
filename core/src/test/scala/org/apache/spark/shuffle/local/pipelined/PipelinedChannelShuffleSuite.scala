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

import scala.reflect.ClassTag

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{HashPartitioner, Partitioner, PipelinedShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.BaseShuffleHandle

/**
 * A [[ShuffledRDD]] that emits a [[PipelinedShuffleDependency]] instead of a regular
 * `ShuffleDependency`, so its shuffle is served by the pipelined manager and its stages are
 * co-scheduled. Test-only: in production the pipelined boundary would be emitted by the SQL
 * planner (a pipelined exchange), not by a hand-built RDD.
 */
private class PipelinedShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends ShuffledRDD[K, V, C](prev, part) {

  override def getDependencies: Seq[org.apache.spark.Dependency[_]] =
    List(new PipelinedShuffleDependency[K, V, C](
      prev.asInstanceOf[RDD[_ <: Product2[K, V]]], part, SparkEnv.get.serializer))

  // A pipelined shuffle is not registered with the MapOutputTracker; skip locality hints,
  // whose inherited implementation casts to MapOutputTrackerMaster for a shuffle that is
  // not there.
  override def getPreferredLocations(partition: org.apache.spark.Partition): Seq[String] = Nil
}

class PipelinedChannelShuffleSuite extends SparkFunSuite {

  private def withPipelinedSparkContext(cores: Int)(body: SparkContext => Unit): Unit = {
    ChannelShuffleRendezvous.clearForTesting()
    val conf = new SparkConf()
      .setMaster(s"local[$cores]")
      .setAppName("pipelined-channel-shuffle-test")
      .set("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .set("spark.speculation", "false")
    val sc = new SparkContext(conf)
    try {
      body(sc)
    } finally {
      sc.stop()
      ChannelShuffleRendezvous.clearForTesting()
    }
  }

  /** Repartition `0 until n` to `numOut` partitions and return (partitionIndex, key, value). */
  private def repartition(
      sc: SparkContext, n: Int, numIn: Int, numOut: Int): Array[(Int, Int, Int)] = {
    val keyed: RDD[(Int, Int)] = sc.parallelize(0 until n, numIn).map(v => (v, v))
    val out = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(numOut))
    out.mapPartitionsWithIndex { (idx, it) =>
      it.map { case (k, v) => (idx, k, v) }
    }.collect()
  }

  test("repartition through the channel transport loses no rows and routes correctly") {
    withPipelinedSparkContext(cores = 8) { sc =>
      val n = 1000
      val numOut = 4
      val out = repartition(sc, n, numIn = 3, numOut = numOut)
      // No row lost or duplicated.
      assert(out.map(_._3).toSet === (0 until n).toSet)
      // Every row landed in the partition its key hashes to.
      val part = new HashPartitioner(numOut)
      out.foreach { case (idx, k, _) =>
        assert(part.getPartition(k) === idx, s"key $k routed to partition $idx")
      }
    }
  }

  test("result matches a regular shuffle of the same workload") {
    // Ground-truth grouping from a plain (non-pipelined) repartition.
    val expected = {
      ChannelShuffleRendezvous.clearForTesting()
      val sc = new SparkContext(
        new SparkConf().setMaster("local[8]").setAppName("baseline"))
      try {
        sc.parallelize(0 until 1000, 3).map(v => (v, v))
          .partitionBy(new HashPartitioner(4))
          .mapPartitionsWithIndex((idx, it) => it.map { case (k, _) => (idx, k) })
          .collect().toSet
      } finally {
        sc.stop()
      }
    }
    withPipelinedSparkContext(cores = 8) { sc =>
      val actual = repartition(sc, 1000, numIn = 3, numOut = 4)
        .map { case (idx, k, _) => (idx, k) }.toSet
      assert(actual === expected)
    }
  }

  test("fan-in whose whole-group demand fits the slots repartitions correctly") {
    // Whole-group demand = producer parts + consumer parts must be <= free slots, since the
    // pipelined stages run concurrently. On local[8]: 5 + 2 = 7 <= 8, admissible.
    withPipelinedSparkContext(cores = 8) { sc =>
      val out = repartition(sc, n = 2000, numIn = 5, numOut = 2)
      assert(out.map(_._3).toSet === (0 until 2000).toSet)
    }
  }

  test("a group whose demand exceeds the slots is rejected up front") {
    // This is the single-machine ceiling of the pipelined model: all stages of the group
    // must hold a slot at once. On local[8], numIn=8 + numOut=2 = 10 > 8, so the scheduler
    // rejects the job before any task runs rather than deadlocking. Documented constraint,
    // pinned here so a future scheduler change that silently relaxes it is caught.
    withPipelinedSparkContext(cores = 8) { sc =>
      val ex = intercept[org.apache.spark.SparkException] {
        repartition(sc, n = 100, numIn = 8, numOut = 2)
      }
      assert(ex.getMessage.contains("CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT") ||
        ex.getMessage.contains("concurrent task slots"),
        s"expected an insufficient-slot rejection, got: ${ex.getMessage}")
    }
  }

  test("unregisterShuffle drops the shuffle's rendezvous queues") {
    // Test the manager's cleanup contract directly rather than relying on the async
    // ContextCleaner + GC to fire (which would be flaky). Populate a couple of queues for
    // a shuffle id, then confirm unregisterShuffle removes exactly those.
    ChannelShuffleRendezvous.clearForTesting()
    try {
      ChannelShuffleRendezvous.queue(7, 0).put("a")
      ChannelShuffleRendezvous.queue(7, 1).put("b")
      ChannelShuffleRendezvous.queue(9, 0).put("c") // a different shuffle, must survive
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 3)

      ChannelShuffleRendezvous.removeShuffle(7)
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 1,
        "only shuffle 7's queues should be dropped")
      // Shuffle 9's queue is untouched.
      assert(ChannelShuffleRendezvous.queue(9, 0).peek() === "c")
    } finally {
      ChannelShuffleRendezvous.clearForTesting()
    }
  }

  test("ContextCleaner frees the channel's queues for a tracker-less pipelined shuffle") {
    // Regression for the production leak (C1): a channel-manager shuffle registers in NO output
    // tracker (usesStreamingShuffleOutputTracker = false), so ContextCleaner.doCleanupShuffle
    // finds it in neither the MapOutputTracker nor the StreamingShuffleOutputTracker and takes
    // its tracker-less branch. That branch must still reach the manager's unregisterShuffle (->
    // ChannelShuffleRendezvous.removeShuffle) or the process-wide queues leak for the JVM's life.
    // The SQL suite cannot catch this: it runs under spark.sql.classic.shuffleDependency.
    // fileCleanup.enabled, whose default is Utils.isTesting = true, so the SQL path proactively
    // removes the shuffle in tests and masks the leak. Here we drive doCleanupShuffle directly
    // (the production GC path, minus the flaky GC) and assert the queues are freed.
    withPipelinedSparkContext(cores = 8) { sc =>
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 1000, 3).map(v => (v, v))
      val rdd = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      val shuffleId = rdd.dependencies.head
        .asInstanceOf[org.apache.spark.ShuffleDependency[_, _, _]].shuffleId
      // Run it so the writer creates the rendezvous queues.
      rdd.collect()
      assert(ChannelShuffleRendezvous.numQueuesForTesting > 0,
        "the pipelined run should have created rendezvous queues")

      // The production cleanup path: ContextCleaner.doCleanupShuffle for this shuffle id. With the
      // fix, its tracker-less branch calls shuffleDriverComponents.removeShuffle, which routes to
      // the channel manager and frees the queues.
      sc.cleaner.get.doCleanupShuffle(shuffleId, blocking = true)
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 0,
        "doCleanupShuffle must free the channel's queues for a tracker-less pipelined shuffle")
    }
  }

  test("the channel manager refuses to construct outside local mode") {
    // The rendezvous is JVM-local; on a multi-executor master every reader would hang on
    // data written in another JVM. A cluster misconfiguration must fail loudly at startup.
    val conf = new SparkConf().set("spark.master", "spark://example.invalid:7077")
    val ex = intercept[IllegalArgumentException] {
      new PipelinedChannelShuffleManager(conf)
    }
    assert(ex.getMessage.contains("requires local mode"), ex.getMessage)
    // And constructs fine for local masters.
    new PipelinedChannelShuffleManager(new SparkConf().set("spark.master", "local[4]"))
  }

  test("materialized regular prefix + pipelined suffix runs end-to-end") {
    // The materialized-prefix mixed shape: a regular shuffle materialized by an earlier job,
    // then a pipelined shuffle over its output in a second job. Previously rejected as a
    // mixed job; with the prefix relaxation the second job runs, reading the materialized
    // regular output at the gang's leaves. Demand: 3 (producer, over the prefix's 3
    // partitions) + 4 (consumer) = 7 <= 8.
    withPipelinedSparkContext(cores = 8) { sc =>
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 1000, 2).map(v => (v % 10, v))
      val prefix = new ShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(3))
      assert(prefix.count() === 1000) // materialize the regular shuffle

      val suffix = new PipelinedShuffledRDD[Int, Int, Int](prefix, new HashPartitioner(4))
      val out = suffix.collect()
      assert(out.map(_._2).toSet === (0 until 1000).toSet,
        "no row lost or duplicated through the prefix + pipelined suffix")
    }
  }

  test("an unmaterialized regular prefix below a pipelined suffix is still rejected") {
    withPipelinedSparkContext(cores = 8) { sc =>
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 100, 2).map(v => (v % 10, v))
      val prefix = new ShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(3))
      // No action on `prefix`: its shuffle is not materialized when the mixed job submits.
      val suffix = new PipelinedShuffledRDD[Int, Int, Int](prefix, new HashPartitioner(4))
      val ex = intercept[org.apache.spark.SparkException] {
        suffix.collect()
      }
      assert(ex.getMessage.contains("fully-materialized prefix"),
        s"expected the unmaterialized-prefix rejection, got: ${ex.getMessage}")
    }
  }

  test("abandon marks partition and drains its queue") {
    // abandon() sets an abandoned mark and drains the queue. The key contract:
    // when a writer is blocked on put() due to a full queue, abandon() drains
    // the queue so the put() unblocks (freeing capacity). This test verifies both
    // the mark is set and the queue is drained.
    ChannelShuffleRendezvous.clearForTesting()
    try {
      val q = ChannelShuffleRendezvous.queue(42, 0)
      // Put a few batches into the queue.
      q.put("batch1")
      q.put("batch2")
      q.put("batch3")
      assert(!q.isEmpty, "queue should have elements")

      // Abandon the partition.
      ChannelShuffleRendezvous.abandon(42, 0)

      // Both conditions must hold after abandon:
      // 1. The partition is marked abandoned (prevents new writes).
      assert(ChannelShuffleRendezvous.isAbandoned(42, 0),
        "partition should be marked abandoned")
      // 2. The queue is drained (unblocks any parked put and cleans up).
      assert(q.isEmpty, "queue should be drained by abandon()")
    } finally {
      ChannelShuffleRendezvous.clearForTesting()
    }
  }

  test("clearAbandonedForShuffle resets a run's marks; other shuffles and queues survive") {
    ChannelShuffleRendezvous.clearForTesting()
    try {
      val shuffleId = 55
      val other = 56
      val pid1 = 0
      val pid2 = 1

      // Abandon two partitions of `shuffleId` and one of another shuffle.
      ChannelShuffleRendezvous.queue(shuffleId, pid1).put("data1")
      ChannelShuffleRendezvous.queue(shuffleId, pid2).put("data2")
      ChannelShuffleRendezvous.abandon(shuffleId, pid1)
      ChannelShuffleRendezvous.abandon(shuffleId, pid2)
      ChannelShuffleRendezvous.abandon(other, pid1)

      assert(ChannelShuffleRendezvous.isAbandoned(shuffleId, pid1))
      assert(ChannelShuffleRendezvous.isAbandoned(shuffleId, pid2))

      // clearAbandonedForShuffle resets ALL of a shuffle's marks (the whole-shuffle reset the
      // scheduler triggers when re-submitting a producer stage), leaving queues and OTHER
      // shuffles' marks intact.
      ChannelShuffleRendezvous.clearAbandonedForShuffle(shuffleId)
      assert(!ChannelShuffleRendezvous.isAbandoned(shuffleId, pid1),
        "the shuffle's marks must be reset")
      assert(!ChannelShuffleRendezvous.isAbandoned(shuffleId, pid2),
        "the shuffle's marks must be reset")
      assert(ChannelShuffleRendezvous.isAbandoned(other, pid1),
        "a different shuffle's marks must survive")
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 2,
        "clearAbandonedForShuffle must not drop queues")

      // removeShuffle drops both queues and all marks for the shuffle.
      ChannelShuffleRendezvous.abandon(shuffleId, pid1)
      ChannelShuffleRendezvous.removeShuffle(shuffleId)
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 0,
        "removeShuffle should drop all queues for this shuffle")
      assert(!ChannelShuffleRendezvous.isAbandoned(shuffleId, pid1),
        "removeShuffle should clear all abandoned marks for this shuffle")
    } finally {
      ChannelShuffleRendezvous.clearForTesting()
    }
  }

  test("reader stops after exactly numMaps EndOfStream markers") {
    // Drive the REAL ChannelShuffleReader.read() (not a reimplementation of its loop): pre-load
    // its partition queue with data batches interleaved with exactly numMaps EndOfStream markers,
    // then assert read() yields every data row IN ORDER and terminates -- it must not block
    // waiting for a further marker (would hang the test), nor stop early. The reader reads only
    // handle.shuffleId, so a mock handle suffices; TaskContext.get() is None here so no
    // completion listener is registered. TempShuffleReadMetrics is a no-op reporter.
    ChannelShuffleRendezvous.clearForTesting()
    try {
      val shuffleId = 11
      val pid = 0
      val numMaps = 3
      val q = ChannelShuffleRendezvous.queue(shuffleId, pid)
      // batch1, EOS, batch2, EOS, batch3, EOS (3 markers = numMaps). Rows are (k, v) pairs.
      q.put(Array[AnyRef]((1, 1), (2, 2)))
      q.put(ChannelShuffleRendezvous.EndOfStream)
      q.put(Array[AnyRef]((3, 3)))
      q.put(ChannelShuffleRendezvous.EndOfStream)
      q.put(Array[AnyRef]((4, 4), (5, 5), (6, 6)))
      q.put(ChannelShuffleRendezvous.EndOfStream)

      val handle = mock(classOf[BaseShuffleHandle[Int, Int, Int]])
      when(handle.shuffleId).thenReturn(shuffleId)
      val reader = new ChannelShuffleReader[Int, Int](
        handle, startPartition = pid, endPartition = pid + 1, numMaps = numMaps,
        readMetrics = new TempShuffleReadMetrics)

      val values = reader.read().map(_._2).toSeq
      assert(values === Seq(1, 2, 3, 4, 5, 6),
        s"reader must yield every data row in order and then stop, got $values")
      assert(q.isEmpty, "the reader should have drained the queue to empty")
    } finally {
      ChannelShuffleRendezvous.clearForTesting()
    }
  }
}

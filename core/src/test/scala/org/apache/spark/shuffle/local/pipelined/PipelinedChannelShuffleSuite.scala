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
    ChannelShuffleRendezvous.clear()
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
      ChannelShuffleRendezvous.clear()
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
      ChannelShuffleRendezvous.clear()
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

  test("unregisterShuffle drops the shuffle's rendezvous queues across all epochs") {
    // Test the manager's cleanup contract directly rather than relying on the async
    // ContextCleaner + GC to fire (which would be flaky). Populate queues for a shuffle id under
    // TWO different epochs (a re-run), plus a different shuffle, then confirm unregisterShuffle
    // removes every epoch of shuffle 7 and leaves shuffle 9 alone.
    ChannelShuffleRendezvous.clear()
    try {
      ChannelShuffleRendezvous.queue(7, epoch = 1, 0).put("a")
      ChannelShuffleRendezvous.queue(7, epoch = 1, 1).put("b")
      ChannelShuffleRendezvous.queue(7, epoch = 2, 0).put("a2") // a re-run of shuffle 7
      ChannelShuffleRendezvous.queue(9, epoch = 1, 0).put("c")  // a different shuffle, must survive
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 4)

      ChannelShuffleRendezvous.removeShuffle(7)
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 1,
        "every epoch of shuffle 7's queues should be dropped")
      // Shuffle 9's queue is untouched.
      assert(ChannelShuffleRendezvous.queue(9, epoch = 1, 0).peek() === "c")
    } finally {
      ChannelShuffleRendezvous.clear()
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

  test("the tracker-less cleanup arm is scoped to shuffles the rendezvous actually holds") {
    // The tracker-less arm of doCleanupShuffle must fire only for a shuffle the channel rendezvous
    // holds state for, not for any shuffle that happens to be in neither tracker while the manager
    // is active. The gating signal is ChannelShuffleRendezvous.holdsShuffle (NOT the manager's
    // registration), because a channel shuffle unregistered before its job runs recreates its
    // queues lazily even though the manager no longer lists it -- keying off the rendezvous frees
    // those recreated queues while staying false for a regular shuffle (never present here).
    withPipelinedSparkContext(cores = 8) { sc =>
      // A channel (pipelined) shuffle: after it runs, the rendezvous holds its queues...
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 1000, 3).map(v => (v, v))
      val rdd = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      val pipelinedId = rdd.dependencies.head
        .asInstanceOf[org.apache.spark.ShuffleDependency[_, _, _]].shuffleId
      rdd.collect()
      assert(ChannelShuffleRendezvous.holdsShuffle(pipelinedId),
        "the rendezvous must hold a channel shuffle that has run")

      // A REGULAR shuffle in the same feature-on context routes to the DEFAULT manager and never
      // touches the rendezvous, so holdsShuffle is false -- the arm skips it (no duplicate RPC).
      val regular = sc.parallelize(0 until 1000, 3).map(v => (v, v)).reduceByKey(_ + _)
      val regularId = regular.dependencies.head
        .asInstanceOf[org.apache.spark.ShuffleDependency[_, _, _]].shuffleId
      regular.collect()
      assert(!ChannelShuffleRendezvous.holdsShuffle(regularId),
        "the rendezvous must NOT hold a regular shuffle")

      // Unregister frees the rendezvous state, so a later GC-time re-clean is a no-op.
      ChannelShuffleRendezvous.removeShuffle(pipelinedId)
      assert(!ChannelShuffleRendezvous.holdsShuffle(pipelinedId),
        "removeShuffle must clear the rendezvous state for the shuffle")
    }
  }

  test("cleanup frees a channel shuffle unregistered BEFORE it ran (re-run recreated its queues)") {
    // Regression for the unregister-before-run leak: a shuffle whose SQL scope ended (Dataset.rdd
    // under fileCleanup) unregisters it before any job runs, then the job runs and recreates the
    // rendezvous queues lazily. An earlier fix keyed cleanup off the manager's registration, which
    // was dropped at that unregister and never re-added, so holdsShuffle stayed false forever and
    // the recreated queues leaked. Keying off the rendezvous fixes that: the recreated queues make
    // holdsShuffle true again, so doCleanupShuffle's arm frees them.
    withPipelinedSparkContext(cores = 8) { sc =>
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 1000, 3).map(v => (v, v))
      val rdd = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      val shuffleId = rdd.dependencies.head
        .asInstanceOf[org.apache.spark.ShuffleDependency[_, _, _]].shuffleId

      // Simulate the unregister-before-run: clear any state, then run so the writer/reader recreate
      // the queues -- WITHOUT any manager re-registration in between.
      ChannelShuffleRendezvous.removeShuffle(shuffleId)
      rdd.collect()
      assert(ChannelShuffleRendezvous.holdsShuffle(shuffleId),
        "running the shuffle must recreate its rendezvous queues")

      // The cleanup arm must now free them (it keys off the rendezvous, not a stale registry).
      sc.cleaner.get.doCleanupShuffle(shuffleId, blocking = true)
      assert(!ChannelShuffleRendezvous.holdsShuffle(shuffleId),
        "doCleanupShuffle must free the recreated queues for a re-run channel shuffle")
    }
  }

  /**
   * Run a job over only SOME of `rdd`'s partitions -- a PARTIAL READ, the shape that makes the
   * DAGScheduler stamp a live reduce-partition set on a pipelined producer (see
   * DAGScheduler.liveReduceSet). Returns the rows the read partitions produced, under a deadline:
   * if the live set is mapped wrongly the producer keeps feeding a partition no reader will drain,
   * fills its bounded queue and parks forever, so a regression surfaces as a timeout, not a hang.
   *
   * These are RDD-level on purpose. The SQL LIMIT shapes that used to cover this now fall back to
   * a regular shuffle (a LIMIT operator builds a hidden regular shuffle in doExecute, so the
   * enabling rules refuse to pipeline a plan containing one), which would make a SQL-level partial
   * read test vacuous -- it would pass while exercising nothing.
   */
  private def partialReadWithin(
      sc: SparkContext, seconds: Int, rdd: RDD[_], partitions: Seq[Int]): Option[Long] = {
    val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
    val fut = pool.submit(new java.util.concurrent.Callable[Long] {
      override def call(): Long = {
        // Count rows per read partition. `PipelinedChannelShuffleSuite.countRows` is a static
        // (companion) function so the job closure captures nothing from the suite instance --
        // a closure capturing the suite is not serializable.
        sc.runJob(rdd, PipelinedChannelShuffleSuite.countRows _, partitions).sum
      }
    })
    try {
      Some(fut.get(seconds.toLong, java.util.concurrent.TimeUnit.SECONDS))
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        None
    } finally {
      pool.shutdownNow()
    }
  }

  test("partial read of a pipelined shuffle completes (identity narrow chain)") {
    // The result RDD reads the shuffle through a 1:1 chain, so the live reduce set is the read
    // partition set itself. Only partition 0 is read; the writer must drop 1..3 rather than fill
    // their queues and park.
    withPipelinedSparkContext(cores = 8) { sc =>
      val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 400000, 4).map(v => (v, v))
      val shuffled = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      // A 1:1 map keeps the chain narrow and same-index (OneToOneDependency).
      val mapped = shuffled.map { case (k, v) => (k, v) }
      // Read a NON-ZERO partition on purpose: a live-set bug that collapses to {0} (or to the
      // result-partition ids when they differ from the reduce ids) would starve this reader and
      // hang, instead of accidentally being right.
      val got = partialReadWithin(sc, 90, mapped, Seq(2))
      assert(got.isDefined,
        "partial read of a pipelined shuffle hung: the producer kept feeding partitions that " +
          "have no reader (the live reduce set was mapped wrongly)")
      assert(got.get > 0, "the read partition should have produced rows")
    }
  }

  test("partial read through a union of two pipelined shuffles completes") {
    // union() reaches each shuffle through a RangeDependency whose getParents applies a per-branch
    // offset, so liveReduceSet must map the read partition set through both branches' offsets.
    withPipelinedSparkContext(cores = 12) { sc =>
      def branch(): RDD[(Int, Int)] = {
        val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 200000, 4).map(v => (v, v))
        new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      }
      // Both branches share a HashPartitioner, so union() yields a PartitionerAwareUnionRDD whose
      // partition i draws from partition i of BOTH branches (4 partitions total, not 8). That is
      // exactly the fan-in shape under test: one read partition must contribute a live reduce
      // partition to EACH branch's shuffle.
      val unioned = branch().union(branch())
      val got = partialReadWithin(sc, 90, unioned, Seq(2))
      assert(got.isDefined,
        "partial read through a union of pipelined shuffles hung: the live set was not mapped " +
          "through the union's per-branch offsets")
    }
  }

  test("partial read through a zip of two pipelined shuffles completes") {
    // zip() fans in over a ZippedPartitionsRDD: one narrow branch per side down to that side's
    // shuffle, so liveReduceSet must contribute the live set from EVERY branch reaching a shuffle.
    withPipelinedSparkContext(cores = 12) { sc =>
      def branch(): RDD[(Int, Int)] = {
        val keyed: RDD[(Int, Int)] = sc.parallelize(0 until 200000, 4).map(v => (v, v))
        new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
      }
      val zipped = branch().zipPartitions(branch())((a, b) => a.zip(b))
      val got = partialReadWithin(sc, 90, zipped, Seq(2))
      assert(got.isDefined,
        "partial read through a zip of pipelined shuffles hung: the live set was not contributed " +
          "from both zip branches")
    }
  }

  test("a failing producer map task fails the job promptly, without parking the reader forever") {
    // If a producer map task throws before emitting end-of-stream, the reduce task for a
    // partition it never fed would park in the queue wait. The reader polls interruptibly and
    // checks the TaskContext interrupt flag (set when the group is aborted), so it wakes and the
    // job fails fast with the producer's error instead of pinning the executor slot forever.
    // Run on a background thread under a deadline: a regression (uninterruptible take) would hang
    // here rather than fail, and the deadline turns that into a test failure, not a stuck suite.
    val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withPipelinedSparkContext(cores = 8) { sc =>
        val keyed = sc.parallelize(0 until 1000, 3).map { v =>
          if (v == 500) throw new RuntimeException("boom in producer map task")
          (v, v)
        }
        val out = new PipelinedShuffledRDD[Int, Int, Int](keyed, new HashPartitioner(4))
        // The job must FAIL (the producer threw), not hang. intercept confirms it returned.
        intercept[org.apache.spark.SparkException](out.collect())
      }
    })
    try {
      fut.get(90, java.util.concurrent.TimeUnit.SECONDS)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        fail("a failing producer hung the job: the reader parked in an uninterruptible wait")
    } finally {
      pool.shutdownNow()
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
    ChannelShuffleRendezvous.clear()
    try {
      val q = ChannelShuffleRendezvous.queue(42, epoch = 1, 0)
      // Put a few batches into the queue.
      q.put("batch1")
      q.put("batch2")
      q.put("batch3")
      assert(!q.isEmpty, "queue should have elements")

      // Abandon the partition.
      ChannelShuffleRendezvous.abandon(42, epoch = 1, 0)

      // Both conditions must hold after abandon:
      // 1. The partition is marked abandoned (prevents new writes).
      assert(ChannelShuffleRendezvous.isAbandoned(42, epoch = 1, 0),
        "partition should be marked abandoned")
      // 2. The queue is drained (unblocks any parked put and cleans up).
      assert(q.isEmpty, "queue should be drained by abandon()")
    } finally {
      ChannelShuffleRendezvous.clear()
    }
  }

  test("a re-run's epoch isolates it from a prior run's leftover queue and marks") {
    // Finding B: a re-run of the same shuffleId must not see the previous run's leftovers. Two
    // runs of one shuffleId are two jobs, so two epochs; keying the rendezvous by epoch makes the
    // second run's queue and marks physically separate from the first's. This is the property
    // that prevents a new reader from draining stale batches / counting stale EndOfStream markers
    // (a silent wrong result) and a straggler writer of the aborted run from touching the new
    // run's queue.
    ChannelShuffleRendezvous.clear()
    try {
      val shuffleId = 55
      val pid = 0

      // Run epoch=1 leaves a batch, an EndOfStream, and an abandoned mark on pid (its reader
      // never fully drained -- e.g. the group aborted).
      ChannelShuffleRendezvous.queue(shuffleId, epoch = 1, pid).put("stale-batch")
      ChannelShuffleRendezvous.queue(shuffleId, epoch = 1, pid).put(
        ChannelShuffleRendezvous.EndOfStream)
      ChannelShuffleRendezvous.abandon(shuffleId, epoch = 1, pid)
      // abandon drains the queue, so re-add a leftover to model a partition whose reader never
      // started (no abandon, queue keeps its contents).
      val pid2 = 1
      ChannelShuffleRendezvous.queue(shuffleId, epoch = 1, pid2).put("stale-unread")

      // Run epoch=2 (the re-run) sees FRESH state for the same (shuffleId, pid): a new empty
      // queue and no abandoned mark, regardless of what epoch 1 left behind.
      assert(ChannelShuffleRendezvous.queue(shuffleId, epoch = 2, pid).isEmpty,
        "the re-run's queue must be empty, not the prior run's leftovers")
      assert(ChannelShuffleRendezvous.queue(shuffleId, epoch = 2, pid2).isEmpty,
        "the re-run's queue must be empty for an unread prior-run partition too")
      assert(!ChannelShuffleRendezvous.isAbandoned(shuffleId, epoch = 2, pid),
        "the re-run must not inherit the prior run's abandoned mark")

      // removeShuffle at cleanup drops every epoch for the shuffle.
      ChannelShuffleRendezvous.removeShuffle(shuffleId)
      assert(ChannelShuffleRendezvous.numQueuesForTesting === 0,
        "removeShuffle should drop queues of every epoch for this shuffle")
      assert(!ChannelShuffleRendezvous.isAbandoned(shuffleId, epoch = 1, pid),
        "removeShuffle should clear abandoned marks of every epoch")
    } finally {
      ChannelShuffleRendezvous.clear()
    }
  }

  test("reader stops after exactly numMaps EndOfStream markers") {
    // Drive the REAL ChannelShuffleReader.read() (not a reimplementation of its loop): pre-load
    // its partition queue with data batches interleaved with exactly numMaps EndOfStream markers,
    // then assert read() yields every data row IN ORDER and terminates -- it must not block
    // waiting for a further marker (would hang the test), nor stop early. The reader reads only
    // handle.shuffleId, so a mock handle suffices; TaskContext.get() is None here so no
    // completion listener is registered and the reader's epoch defaults to 0 -- so pre-load the
    // queue at epoch 0 to match. TempShuffleReadMetrics is a no-op reporter.
    ChannelShuffleRendezvous.clear()
    try {
      val shuffleId = 11
      val pid = 0
      val numMaps = 3
      val q = ChannelShuffleRendezvous.queue(shuffleId, epoch = 0, pid)
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
      ChannelShuffleRendezvous.clear()
    }
  }
}

private object PipelinedChannelShuffleSuite {
  /** Row count of one partition; a top-level function so a job closure captures no suite state. */
  def countRows(it: Iterator[_]): Long = it.size.toLong
}

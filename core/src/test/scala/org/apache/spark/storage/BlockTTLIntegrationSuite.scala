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

package org.apache.spark.storage

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import org.apache.spark._
import org.apache.spark.internal.config

/**
 * End-to-end tests for the block TTL cleaners: that the right things get tracked, that a reap
 * reclaims what it should, and that the hooks `SparkContext` wires are the ones that run.
 *
 * The smallest TTL the configs accept is 10 minutes, so these tests do not wait for an expiry.
 * Instead they backdate an access time and call `BlockTTLCleaner.sweep()` directly, which is both
 * faster and deterministic. The background sweep threads are stopped for the same reason -- their
 * scheduling is `BlockTTLCleanerSuite`'s business, not this suite's.
 */
class BlockTTLIntegrationSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  // Only the cluster-wide block removals are asynchronous now, so the budget can be modest. The
  // interval is deliberately not tight: some `eventually` bodies do an RPC fan-out to every
  // executor, and polling that every few millis loads the dispatcher being measured.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(200, Millis)))

  private val ttlSeconds = 600L
  private val numParts = 3

  /** Starts a two-executor cluster, enabling either cleaner. */
  private def startCluster(rddTTL: Boolean, shuffleTTL: Boolean): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local-cluster[2, 1, 1024]")
    if (rddTTL) conf.set(config.CLEANER_TTL_RDD, ttlSeconds)
    if (shuffleTTL) conf.set(config.CLEANER_TTL_SHUFFLE, ttlSeconds)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    // Sweeps are driven explicitly below; stop the threads so they cannot race the assertions.
    endpoint.ttlCleaner.foreach(_.stop())
    tracker.ttlCleaner.foreach(_.stop())
  }

  // The driver retains its endpoint instance, so no reflection is needed to reach it.
  private def endpoint: BlockManagerMasterEndpoint = sc.env.blockManagerMasterEndpoint.get

  private def tracker: MapOutputTrackerMaster =
    sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  /** Backdates an access time well past the TTL, so the next sweep treats the id as expired. */
  private def expire(accessTimes: ConcurrentHashMap[Int, Long], id: Int): Unit = {
    assert(accessTimes.containsKey(id), s"$id should be TTL-tracked before being expired")
    accessTimes.put(id, System.currentTimeMillis() - (ttlSeconds * 1000 * 2))
  }

  private def sweepRdds(): Unit =
    endpoint.ttlCleaner.getOrElse(fail("the RDD TTL cleaner should be enabled")).sweep()

  private def sweepShuffles(): Unit =
    tracker.ttlCleaner.getOrElse(fail("the shuffle TTL cleaner should be enabled")).sweep()

  test("a shuffle's map output is reclaimed by a sweep") {
    startCluster(rddTTL = true, shuffleTTL = true)
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleAccessTime.isEmpty)
    val shuffled = sc.parallelize(1.to(100)).groupBy(_ % 10)
    shuffled.count()
    val shuffleId = tracker.shuffleStatuses.keys.head
    val epochBefore = tracker.getEpoch
    eventually { assert(tracker.shuffleAccessTime.containsKey(shuffleId)) }
    // An uncached shuffle is tracked by the map output tracker only, never as an RDD.
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleStatuses.get(shuffleId).exists(_.numAvailableMapOutputs > 0))

    expire(tracker.shuffleAccessTime, shuffleId)
    sweepShuffles()

    // Assert on the map output itself, not just on the tracking map: the sweep drops the access
    // time *before* it reaps, so an access-time assertion alone passes even with the reap wiring
    // removed.
    assert(!tracker.shuffleStatuses.get(shuffleId).exists(_.numAvailableMapOutputs > 0),
      "the reaped shuffle's map output should be gone")
    assert(tracker.getEpoch > epochBefore,
      "the reap must bump the epoch, or executors keep fetching the files it deleted")
    assert(tracker.shuffleAccessTime.isEmpty)
    // Last use of `shuffled`, keeping it reachable throughout: the JVM collects by liveness, not by
    // scope, and a collected ShuffleDependency would let the ContextCleaner do the reaping instead.
    assert(shuffled.getNumPartitions > 0)
  }

  test("the shuffle reap deletes files before it drops the driver's map output") {
    // removeShuffle reads the live MapStatuses and push-merger locations to find what the external
    // shuffle service must delete, so emptying the ShuffleStatus first silently reclaims no ESS
    // disk. Observe the ordering directly rather than standing up a shuffle service.
    startCluster(rddTTL = false, shuffleTTL = true)
    // -2 = never invoked, -1 = invoked with the ShuffleStatus already gone.
    val outputsWhenRemoved = new AtomicInteger(-2)
    val wiredRemover = tracker.shuffleFileRemover.getOrElse(
      fail("SparkContext should have wired the shuffle file remover"))
    tracker.shuffleFileRemover = Some { shuffleId =>
      outputsWhenRemoved.compareAndSet(-2,
        tracker.shuffleStatuses.get(shuffleId).map(_.numAvailableMapOutputs).getOrElse(-1))
      wiredRemover(shuffleId)
    }
    val shuffled = sc.parallelize(1.to(100)).groupBy(_ % 10)
    shuffled.count()
    val shuffleId = tracker.shuffleStatuses.keys.head
    eventually { assert(tracker.shuffleAccessTime.containsKey(shuffleId)) }

    expire(tracker.shuffleAccessTime, shuffleId)
    sweepShuffles()

    assert(outputsWhenRemoved.get() !== -2, "the sweep should have reaped the shuffle")
    assert(outputsWhenRemoved.get() > 0,
      "the shuffle's map output must still be registered when its files are deleted, or the " +
        "external shuffle service is never told which blocks to remove")
    assert(shuffled.getNumPartitions > 0)
  }

  test("re-reading a cached RDD in a new job refreshes its access time") {
    // This pins down the assumption behind the RDD TTL: an actively reused cached RDD is not
    // reaped, because every job that reads it re-resolves cache locations at the driver
    // (DAGScheduler.getCacheLocs -> BlockManagerMaster.getLocations -> updateBlockAtime), whether
    // or not the block read itself is served locally on the executor.
    startCluster(rddTTL = true, shuffleTTL = false)
    assert(endpoint.rddAccessTime.isEmpty)
    val input = sc.parallelize(1.to(100), numParts).cache()
    input.count()
    // eventually because the executors' UpdateBlockInfo reports can land after count() returns.
    eventually { assert(endpoint.rddAccessTime.containsKey(input.id)) }
    // Read via Option: a ConcurrentHashMap[Int, Long] miss unboxes null to 0L rather than throwing,
    // which would let the assertion below pass vacuously.
    def atimeOf(rddId: Int): Option[Long] =
      Option(endpoint.rddAccessTime.get(rddId)).map(_.longValue)
    val firstAtime = atimeOf(input.id).getOrElse(fail("the cached RDD should be TTL-tracked"))
    // Retried because the clock may not have ticked past firstAtime yet.
    eventually {
      input.count()
      assert(atimeOf(input.id).exists(_ > firstAtime),
        s"a new job reading the cached RDD should refresh its atime (was $firstAtime)")
    }
  }

  test("RDD and shuffle blocks are both reclaimed, and come back") {
    startCluster(rddTTL = true, shuffleTTL = true)
    assert(endpoint.rddAccessTime.isEmpty)
    val input = sc.parallelize(1.to(100)).groupBy(_ % 10).cache()
    input.count()
    eventually { assert(endpoint.rddAccessTime.containsKey(input.id)) }
    val shuffleId = tracker.shuffleStatuses.keys.head
    eventually { assert(tracker.shuffleAccessTime.containsKey(shuffleId)) }

    expire(endpoint.rddAccessTime, input.id)
    expire(tracker.shuffleAccessTime, shuffleId)
    sweepRdds()
    sweepShuffles()

    assert(tracker.shuffleAccessTime.isEmpty, "the reaped shuffle should no longer be tracked")
    assert(endpoint.rddAccessTime.isEmpty, "the reaped RDD should no longer be tracked")
    // Both are evictions, not unpersists: the job runs again and both come back tracked.
    input.count()
    eventually {
      assert(endpoint.rddAccessTime.containsKey(input.id))
      assert(!tracker.shuffleAccessTime.isEmpty)
    }
  }

  test("a locally-checkpointed RDD is never reaped by the TTL cleaner") {
    // localCheckpoint truncates lineage, so the cache blocks are the only copy of the data and
    // losing them is unrecoverable (LocalCheckpointRDD.compute always throws).
    startCluster(rddTTL = true, shuffleTTL = false)
    val checkpointed = sc.parallelize(1.to(100), numParts)
    checkpointed.localCheckpoint()
    assert(checkpointed.count() === 100)
    eventually { assert(endpoint.rddAccessTime.containsKey(checkpointed.id)) }
    assert(endpoint.rddReapable(checkpointed.id) === false,
      "the TTL cleaner must refuse to reap a locally-checkpointed RDD")

    // Expire it and sweep anyway: the veto, not the access time, is what has to save it.
    expire(endpoint.rddAccessTime, checkpointed.id)
    sweepRdds()

    assert(checkpointed.count() === 100,
      "a locally-checkpointed RDD must survive a sweep: its blocks are the only copy")
    assert(endpoint.rddAccessTime.containsKey(checkpointed.id),
      "a vetoed RDD must stay tracked, or it could never be reaped once the veto lifts")

    // Positive control: `rddReapable`'s unwired default is `_ => false`, so the assertion above
    // passes just as well when SparkContext never wired the veto at all. Pin down that the wired
    // veto discriminates rather than refusing everything.
    val plainCached = sc.parallelize(1.to(10)).cache()
    assert(plainCached.count() === 10)
    assert(endpoint.rddReapable(plainCached.id),
      "the veto must be specific to locally-checkpointed RDDs, not a blanket refusal")
  }

  test("reaping an RDD removes every partition and leaves the RDD usable") {
    // Access times are recorded per RDD id but stamped by individual block accesses, and a reap
    // removes the whole RDD. Pin down both halves of that.
    startCluster(rddTTL = true, shuffleTTL = false)
    val input = sc.parallelize(1.to(100), numParts).cache()
    assert(input.count() === 100)

    def cachedPartitionsOf(rddId: Int): Int =
      sc.env.blockManager.master.getMatchingBlockIds({
        case RDDBlockId(id, _) => id == rddId
        case _ => false
      }, askStorageEndpoints = true).size

    eventually {
      assert(endpoint.rddAccessTime.containsKey(input.id))
      assert(cachedPartitionsOf(input.id) >= numParts, s"expected all $numParts partitions cached")
    }

    expire(endpoint.rddAccessTime, input.id)
    sweepRdds()

    assert(!endpoint.rddAccessTime.containsKey(input.id),
      "the reaped RDD should no longer be TTL-tracked")
    // eventually: the reap frees the driver's metadata synchronously but the executors are told
    // asynchronously (removeRdd with blocking = false).
    eventually {
      assert(cachedPartitionsOf(input.id) === 0,
        "every partition of the reaped RDD should be removed, not just the idle ones")
    }
    // An eviction, not an unpersist: the RDD still computes and comes back tracked.
    assert(input.count() === 100, "a reaped RDD must still be usable (recomputed)")
    eventually { assert(endpoint.rddAccessTime.containsKey(input.id)) }
  }

  test("nothing is tracked when the TTLs are not set") {
    startCluster(rddTTL = false, shuffleTTL = false)
    sc.parallelize(1.to(100)).groupBy(_ % 10).cache().count()
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleAccessTime.isEmpty)
    assert(endpoint.ttlCleaner.isEmpty, "no RDD TTL means no cleaner at all")
    assert(tracker.ttlCleaner.isEmpty, "no shuffle TTL means no cleaner at all")
  }

  test("a TTL below the ten minute minimum is rejected") {
    val conf = new SparkConf().set(config.CLEANER_TTL_RDD.key, "60s")
    val e = intercept[IllegalArgumentException](conf.get(config.CLEANER_TTL_RDD))
    assert(e.getMessage.contains("at least 10 minutes"))
    val shuffleConf = new SparkConf().set(config.CLEANER_TTL_SHUFFLE.key, "5s")
    val se = intercept[IllegalArgumentException](shuffleConf.get(config.CLEANER_TTL_SHUFFLE))
    assert(se.getMessage.contains("at least 10 minutes"))
    // A bare number is seconds, matching the pre-2.0 spark.cleaner.ttl this restores, so 600 is
    // exactly the minimum and must be accepted.
    assert(new SparkConf().set(config.CLEANER_TTL_RDD.key, "600")
      .get(config.CLEANER_TTL_RDD).contains(600L))
  }
}

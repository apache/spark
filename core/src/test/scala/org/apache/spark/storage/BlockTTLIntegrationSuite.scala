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

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import org.apache.spark._
import org.apache.spark.internal.config

class BlockTTLIntegrationSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  // The interval is not tight on purpose: several of these `eventually` bodies do a cluster-wide
  // RPC fan-out (getMatchingBlockIds with askStorageEndpoints), and polling those every few millis
  // for up to 20s loads the very dispatcher the assertions are measuring.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(200, Millis)))

  private val blockTTL = 5000L
  // Long enough that nothing is reaped mid-test, for the tests that only check tracking.
  private val longTTL = 60000L
  private val numParts = 3

  /** Starts a two-executor cluster; a TTL of None leaves that cleaner disabled. */
  private def startCluster(rddTTL: Option[Long], shuffleTTL: Option[Long]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local-cluster[2, 1, 1024]")
    rddTTL.foreach(ttl => conf.set(config.CLEANER_TTL_RDD, ttl))
    shuffleTTL.foreach(ttl => conf.set(config.CLEANER_TTL_SHUFFLE, ttl))
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
  }

  // The driver retains its endpoint instance, so no reflection is needed to reach it.
  private def endpoint: BlockManagerMasterEndpoint = sc.env.blockManagerMasterEndpoint.get

  private def tracker: MapOutputTrackerMaster =
    sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  test("a shuffle's map output is reclaimed after the TTL") {
    startCluster(Some(blockTTL), Some(blockTTL))
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleAccessTime.isEmpty)
    assert(tracker.ttlCleaner.isDefined)
    // Held in scope so the ContextCleaner cannot collect the ShuffleDependency first: its path does
    // not bump the epoch, so the epoch assertion below would never come true.
    val shuffled = sc.parallelize(1.to(100)).groupBy(_ % 10)
    shuffled.count()
    val shuffleId = tracker.shuffleStatuses.keys.head
    val epochBefore = tracker.getEpoch
    eventually { assert(tracker.shuffleAccessTime.containsKey(shuffleId)) }
    // An uncached shuffle is tracked by the map output tracker only, never as an RDD.
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleStatuses.get(shuffleId).exists(_.numAvailableMapOutputs > 0))
    // Assert on the map output itself, not just on the tracking map: the cleaner drops the atime
    // *before* it reaps, so an atime assertion alone passes even with the reap wiring removed.
    eventually {
      assert(!tracker.shuffleStatuses.get(shuffleId).exists(_.numAvailableMapOutputs > 0),
        s"the shuffle's map output should be gone ${blockTTL}ms after its last access")
      assert(tracker.getEpoch > epochBefore,
        "the reap must bump the epoch, or executors keep fetching the files it deleted")
      assert(tracker.shuffleAccessTime.isEmpty)
    }
    // Last use of `shuffled`, keeping it reachable throughout: the JVM collects by liveness, not
    // by scope.
    assert(shuffled.getNumPartitions > 0)
  }

  test("the shuffle reap deletes files before it drops the driver's map output") {
    // removeShuffle reads the live MapStatuses and push-merger locations to find what the external
    // shuffle service must delete, so emptying the ShuffleStatus first silently reclaims no ESS
    // disk. Observe the ordering directly rather than standing up a shuffle service.
    startCluster(Some(blockTTL), Some(blockTTL))
    // -2 = never invoked, -1 = invoked with the ShuffleStatus already gone.
    val outputsWhenRemoved = new AtomicInteger(-2)
    val wiredRemover = tracker.shuffleFileRemover.getOrElse(
      fail("SparkContext should have wired the shuffle file remover"))
    tracker.shuffleFileRemover = Some { shuffleId =>
      outputsWhenRemoved.compareAndSet(-2,
        tracker.shuffleStatuses.get(shuffleId).map(_.numAvailableMapOutputs).getOrElse(-1))
      wiredRemover(shuffleId)
    }
    // Held in scope so the ContextCleaner cannot collect the ShuffleDependency mid-test; the TTL
    // cleaner must be the only thing doing the reaping here.
    val shuffled = sc.parallelize(1.to(100)).groupBy(_ % 10)
    shuffled.count()
    eventually {
      assert(outputsWhenRemoved.get() !== -2, "the TTL should have reaped the shuffle by now")
    }
    assert(outputsWhenRemoved.get() > 0,
      "the shuffle's map output must still be registered when its files are deleted, or the " +
        "external shuffle service is never told which blocks to remove")
    // Last use of `shuffled`, keeping it reachable throughout: the JVM collects by liveness, not
    // by scope.
    assert(shuffled.getNumPartitions > 0)
  }

  test("re-reading a cached RDD in a new job refreshes its access time") {
    // This pins down the assumption behind the RDD TTL: an actively reused cached RDD is not
    // reaped, because every job that reads it re-resolves cache locations at the driver
    // (DAGScheduler.getCacheLocs -> BlockManagerMaster.getLocations -> updateBlockAtime), whether
    // or not the block read itself is served locally on the executor.
    startCluster(Some(longTTL), Some(longTTL))
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

  test("RDD and shuffle blocks are both removed after the TTL, and come back") {
    startCluster(Some(blockTTL), Some(blockTTL))
    assert(endpoint.rddAccessTime.isEmpty)
    val input = sc.parallelize(1.to(100)).groupBy(_ % 10).cache()
    input.count()
    eventually { assert(!endpoint.rddAccessTime.isEmpty) }
    eventually { assert(!tracker.shuffleAccessTime.isEmpty) }
    eventually {
      assert(tracker.shuffleAccessTime.isEmpty, s"the shuffle should be gone after ${blockTTL}ms")
      assert(endpoint.rddAccessTime.isEmpty, s"the RDD should be gone after ${blockTTL}ms")
    }
    input.count()
    eventually {
      assert(!endpoint.rddAccessTime.isEmpty)
      assert(!tracker.shuffleAccessTime.isEmpty)
    }
  }

  test("a locally-checkpointed RDD is never reaped by the TTL cleaner") {
    // localCheckpoint truncates lineage, so the cache blocks are the only copy of the data and
    // losing them is unrecoverable (LocalCheckpointRDD.compute always throws).
    startCluster(Some(blockTTL), Some(blockTTL))
    val checkpointed = sc.parallelize(1.to(100), numParts)
    checkpointed.localCheckpoint()
    assert(checkpointed.count() === 100)
    // Sit idle for longer than the TTL; a plain cached RDD would be reaped in this window.
    Thread.sleep(blockTTL * 2)
    assert(checkpointed.count() === 100,
      "a locally-checkpointed RDD must survive the TTL: its blocks are the only copy")
    assert(endpoint.rddReapable(checkpointed.id) === false,
      "the TTL cleaner must refuse to reap a locally-checkpointed RDD")
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
    startCluster(Some(blockTTL), Some(longTTL))
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
    eventually {
      assert(!endpoint.rddAccessTime.containsKey(input.id),
        "the reaped RDD should no longer be TTL-tracked")
      assert(cachedPartitionsOf(input.id) === 0,
        "every partition of the reaped RDD should be removed, not just the idle ones")
    }
    // An eviction, not an unpersist: the RDD still computes and comes back tracked.
    assert(input.count() === 100, "a reaped RDD must still be usable (recomputed)")
    eventually { assert(endpoint.rddAccessTime.containsKey(input.id)) }
  }

  test("nothing is tracked when the TTLs are not set") {
    startCluster(None, None)
    sc.parallelize(1.to(100)).groupBy(_ % 10).cache().count()
    assert(endpoint.rddAccessTime.isEmpty)
    assert(tracker.shuffleAccessTime.isEmpty)
  }
}

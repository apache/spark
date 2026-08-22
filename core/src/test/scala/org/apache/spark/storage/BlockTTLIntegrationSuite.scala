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

import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import org.apache.spark._
import org.apache.spark.internal.config

class BlockTTLIntegrationSuite extends SparkFunSuite with LocalSparkContext
    with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(5, Millis)))

  val blockTTL = 5000L
  // Long enough that the cleaner won't reap mid-test; used by the tests that only check that
  // tracking happens / atime is refreshed (not the removal-after-TTL tests).
  val longTTL = 60000L

  val numParts = 3

  private def lookupBlockManagerMasterEndpoint(sc: SparkContext): BlockManagerMasterEndpoint = {
    // The driver retains its endpoint instance precisely so this is reachable without reflection.
    sc.env.blockManagerMasterEndpoint.get
  }

  private def lookupMapOutputTrackerMaster(sc: SparkContext): MapOutputTrackerMaster = {
    // On the driver the tracker is always a MapOutputTrackerMaster.
    sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
  }

  test("Test that cache blocks are recorded.") {
    val conf = new SparkConf()
      .setAppName("test-blockmanager-decommissioner")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, longTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, longTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    // Make some cache blocks
    val input = sc.parallelize(1.to(100)).cache()
    input.count()
    // Check that the blocks were registered with the TTL tracker. Wrapped in eventually because the
    // executors' UpdateBlockInfo reports can land just after count() returns.
    eventually { assert(managerMasterEndpoint.rddAccessTime.containsKey(input.id)) }
  }

  test("Test that re-reading a cached RDD in a new job refreshes its access time") {
    // This pins down the assumption behind the RDD TTL: an actively-reused cached RDD is not
    // reaped, because every job that reads it re-resolves cache locations at the driver
    // (DAGScheduler.clearCacheLocs -> getCacheLocs -> BlockManagerMaster.getLocations ->
    // updateBlockAtime), refreshing the atime -- independent of whether the block read itself is
    // served locally on the executor.
    val conf = new SparkConf()
      .setAppName("test-blockmanager-ttls-rdd-refresh")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, longTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, longTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    val input = sc.parallelize(1.to(100), numParts).cache()
    input.count()
    // The cached blocks are tracked, keyed by RDD id.
    eventually { assert(managerMasterEndpoint.rddAccessTime.containsKey(input.id)) }
    // Read via Option, not get: a ConcurrentHashMap[Int, Long] miss unboxes null to 0L rather than
    // throwing, which would let the "atime advanced" assertion below pass vacuously.
    def atimeOf(rddId: Int): Option[Long] =
      Option(managerMasterEndpoint.rddAccessTime.get(rddId)).map(_.longValue)
    val firstAtime = atimeOf(input.id).getOrElse(
      fail("the cached RDD should be TTL-tracked before we test the refresh"))
    // Re-reading the cached RDD in a new job must refresh (advance) its access time. Re-running
    // inside eventually guards against the clock not having ticked past firstAtime yet; if the
    // cleaner had already reaped it, count() re-materializes it and the atime still advances.
    eventually {
      input.count()
      assert(atimeOf(input.id).exists(_ > firstAtime),
        s"a new job reading the cached RDD should refresh its atime (was $firstAtime)")
    }
  }

  test("Test that shuffle blocks are tracked properly and removed after TTL") {
    val conf = new SparkConf()
      .setAppName("test-blockmanager-ttls-shuffle-only")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, blockTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, blockTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    val mapOutputTracker = lookupMapOutputTrackerMaster(sc)
    // Make sure it's empty at the start
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    assert(mapOutputTracker.shuffleAccessTime.isEmpty)
    // Make some cache blocks
    val input = sc.parallelize(1.to(100)).groupBy(_ % 10)
    input.count()
    // Make sure we've got the tracker threads defined
    assert(mapOutputTracker.cleanerThreadpool.isDefined)
    // Check that the shuffle blocks were NOT registered with the RDD TTL tracker.
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    // Check that the shuffle blocks are registered with the map output TTL
    eventually { assert(!mapOutputTracker.shuffleAccessTime.isEmpty) }
    // It should be expired!
    eventually {
      val t = System.currentTimeMillis()
      assert(
      mapOutputTracker.shuffleAccessTime.isEmpty,
      s"We should have no blocks since we are now at time ${t} with ttl of ${blockTTL}")
    }
  }


  test(s"Test that all blocks are tracked properly and removed after TTL") {
    val conf = new SparkConf()
      .setAppName("test-blockmanager-ttls-enabled")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, blockTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, blockTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    val mapOutputTracker = lookupMapOutputTrackerMaster(sc)
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    // Make some cache blocks
    val input = sc.parallelize(1.to(100)).groupBy(_ % 10)
    val cachedInput = input.cache()
    cachedInput.count()
    // Check that we have both shuffle & RDD blocks registered
    eventually { assert(!managerMasterEndpoint.rddAccessTime.isEmpty) }
    eventually { assert(!mapOutputTracker.shuffleAccessTime.isEmpty) }
    // Both should be expired!
    eventually {
      val t = System.currentTimeMillis()
      assert(mapOutputTracker.shuffleAccessTime.isEmpty,
        s"We should have no blocks since we are now at time ${t} with ttl of ${blockTTL}")
      assert(managerMasterEndpoint.rddAccessTime.isEmpty,
        s"We should have no blocks since we are now at time ${t} with ttl of ${blockTTL}")
    }
    // And redoing the count should work and everything should come back.
    input.count()
    eventually {
      assert(!managerMasterEndpoint.rddAccessTime.isEmpty)
      assert(!mapOutputTracker.shuffleAccessTime.isEmpty)
    }
  }

  test("Test that a locally-checkpointed RDD is never reaped by the TTL cleaner") {
    // localCheckpoint truncates lineage, so the cache blocks are the only copy of the data: reaping
    // them loses it unrecoverably (LocalCheckpointRDD.compute always throws). The cleaner must skip
    // such RDDs no matter how long they sit idle.
    val conf = new SparkConf()
      .setAppName("test-blockmanager-ttls-local-checkpoint")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, blockTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, blockTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    val checkpointed = sc.parallelize(1.to(100), numParts)
    checkpointed.localCheckpoint()
    assert(checkpointed.count() === 100)
    // Sit idle for longer than the TTL; a plain cached RDD would be reaped in this window.
    Thread.sleep(blockTTL * 2)
    // The data must still be readable -- this is the assertion that would fail on data loss.
    assert(checkpointed.count() === 100,
      "a locally-checkpointed RDD must survive the TTL: its blocks are the only copy")
    assert(managerMasterEndpoint.rddReapable(checkpointed.id) === false,
      "the TTL cleaner must refuse to reap a locally-checkpointed RDD")
  }

  test("Test that reaping an RDD removes every partition and leaves the RDD usable") {
    // Access times are recorded per RDD id but stamped by individual block accesses, and a reap
    // removes the whole RDD. Pin both halves of that: every partition's block goes away, and since
    // the reap frees blocks without resetting the RDD's storage level it is an eviction, not an
    // unpersist -- the RDD still computes the right answer and re-caches on the next action.
    val conf = new SparkConf()
      .setAppName("test-blockmanager-ttls-full-rdd-removal")
      .setMaster("local-cluster[2, 1, 1024]")
      .set(config.SPARK_TTL_RDD_CLEANER, blockTTL)
      .set(config.SPARK_TTL_SHUFFLE_BLOCK_CLEANER, longTTL)
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    val input = sc.parallelize(1.to(100), numParts).cache()
    assert(input.count() === 100)

    def cachedPartitionsOf(rddId: Int): Int =
      sc.env.blockManager.master.getMatchingBlockIds({
        case RDDBlockId(id, _) => id == rddId
        case _ => false
      }, askStorageEndpoints = true).size

    // All partitions are cached and the RDD is tracked.
    eventually {
      assert(managerMasterEndpoint.rddAccessTime.containsKey(input.id))
      assert(cachedPartitionsOf(input.id) >= numParts,
        s"expected all $numParts partitions cached")
    }

    // After the TTL every one of this RDD's blocks is gone from the master's directory, and it is
    // no longer tracked.
    eventually {
      assert(!managerMasterEndpoint.rddAccessTime.containsKey(input.id),
        "the reaped RDD should no longer be TTL-tracked")
      assert(cachedPartitionsOf(input.id) === 0,
        "every partition of the reaped RDD should be removed, not just the idle ones")
    }

    // Eviction, not unpersist: the RDD still produces the right answer and comes back tracked.
    assert(input.count() === 100, "a reaped RDD must still be usable (recomputed)")
    eventually { assert(managerMasterEndpoint.rddAccessTime.containsKey(input.id)) }
  }

  test("Test that blocks TTLS are not tracked when not enabled") {
    val conf = new SparkConf()
      .setAppName("test-blockmanager-decommissioner")
      .setMaster("local-cluster[2, 1, 1024]")
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
    val managerMasterEndpoint = lookupBlockManagerMasterEndpoint(sc)
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    // Make some cache blocks
    val input = sc.parallelize(1.to(100)).groupBy(_ % 10).cache()
    input.count()
    // Check that no RDD blocks are tracked
    assert(managerMasterEndpoint.rddAccessTime.isEmpty)
    // Check that the no shuffle blocks are tracked.
    val mapOutputTracker = lookupMapOutputTrackerMaster(sc)
    assert(mapOutputTracker.shuffleAccessTime.isEmpty)
  }
}

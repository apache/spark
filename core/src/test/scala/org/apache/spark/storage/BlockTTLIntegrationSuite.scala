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

import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.util.ResetSystemProperties

class BlockTTLIntegrationSuite extends SparkFunSuite with LocalSparkContext
    with ResetSystemProperties with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(5, Millis)))

  val blockTTL = 5000L
  // Long enough that the cleaner won't reap mid-test; used by the tests that only check that
  // tracking happens / atime is refreshed (not the removal-after-TTL tests).
  val longTTL = 60000L

  val numParts = 3

  // TODO(holden): This is shared with MapOutputTrackerSuite move to a BlockTestUtils or similar.
  private def fetchDeclaredField(value: AnyRef, fieldName: String): AnyRef = {
    val field = value.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(value)
  }

  private def lookupBlockManagerMasterEndpoint(sc: SparkContext): BlockManagerMasterEndpoint = {
    val rpcEnv = sc.env.rpcEnv
    val dispatcher = fetchDeclaredField(rpcEnv, "dispatcher")
    fetchDeclaredField(dispatcher, "endpointRefs").
      asInstanceOf[java.util.Map[RpcEndpoint, RpcEndpointRef]].asScala.
      filter(_._1.isInstanceOf[BlockManagerMasterEndpoint]).
      head._1.asInstanceOf[BlockManagerMasterEndpoint]
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
    // Check that the blocks were registered with the TTL tracker
    assert(!managerMasterEndpoint.rddAccessTime.isEmpty)
    val trackedRDDBlocks = managerMasterEndpoint.rddAccessTime.asScala.keys
    assert(!trackedRDDBlocks.isEmpty)
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
    val firstAtime = managerMasterEndpoint.rddAccessTime.get(input.id)
    // Re-reading the cached RDD in a new job must refresh (advance) its access time. Re-running
    // inside eventually guards against the clock not having ticked past firstAtime yet; if the
    // cleaner had already reaped it, count() re-materializes it and the atime still advances.
    eventually {
      input.count()
      assert(managerMasterEndpoint.rddAccessTime.containsKey(input.id),
        "cached RDD should stay tracked while it is being reused")
      assert(managerMasterEndpoint.rddAccessTime.get(input.id) > firstAtime,
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
    val idleUntil = System.currentTimeMillis() + (blockTTL * 2)
    eventually(timeout(Span(blockTTL * 4, Millis)), interval(Span(200, Millis))) {
      assert(System.currentTimeMillis() > idleUntil)
    }
    // The data must still be readable -- this is the assertion that would fail on data loss.
    assert(checkpointed.count() === 100,
      "a locally-checkpointed RDD must survive the TTL: its blocks are the only copy")
    assert(managerMasterEndpoint.rddReapable(checkpointed.id) === false,
      "the TTL cleaner must refuse to reap a locally-checkpointed RDD")
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

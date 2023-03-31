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

package org.apache.spark.broadcast

import java.util.Locale

import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.SparkFunSuite
import org.apache.spark.TestUtils

class SmallBroadcastSuite extends SparkFunSuite with LocalSparkContext {

  test("Using SmallBroadcast locally") {
    sc = new SparkContext("local", "test")
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.smallBroadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing SmallBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test")
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing SmallBroadcast variables in a local cluster") {
    val numWorkers = 4
    sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numWorkers), "test")
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numWorkers).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numWorkers).map(x => (x, 10)).toSet)
  }

  /**
   * Verify the persistence of state associated with a SmallBroadcast in a local-cluster.
   *
   * This test creates a broadcast variable, uses it on all executors, and then destroy it.
   * In between each step, this test verifies that the broadcast object are present only on the
   * expected nodes.
   */
  test("Unpersisting SmallBroadcast") {
    val numWorkers = 2

    // Verify that blocks are persisted only on the driver
    def afterCreation(broadcastId: Long): Unit = {
      val trackerMaster = sc.env.broadcastManager.smallBroadcastTracker
        .asInstanceOf[SmallBroadcastTrackerMaster]
      assert(trackerMaster.get(broadcastId).isDefined)
      assert(trackerMaster.getSerialized(broadcastId).isDefined)
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(broadcastId: Long): Unit = {
      val trackerMaster = sc.env.broadcastManager.smallBroadcastTracker
        .asInstanceOf[SmallBroadcastTrackerMaster]
      assert(trackerMaster.get(broadcastId).isDefined)
      assert(trackerMaster.getSerialized(broadcastId).isDefined)
      sc.range(0, numWorkers, 1, 2).mapPartitions(_ =>
        Iterator(Seq(SparkEnv.get.broadcastManager.smallBroadcastTracker
          .asInstanceOf[SmallBroadcastTrackerWorker].cache.size()))).collect()
        .foreach(value => assert(value.head > 0))
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true.
    def afterUnpersist(broadcastId: Long): Unit = {
      val trackerMaster = sc.env.broadcastManager.smallBroadcastTracker
        .asInstanceOf[SmallBroadcastTrackerMaster]
      assert(trackerMaster.get(broadcastId).isEmpty)
      assert(trackerMaster.getSerialized(broadcastId).isEmpty)
    }

    testUnpersistBroadcast(numWorkers, afterCreation, afterUsingBroadcast, afterUnpersist)
  }

  test("Using broadcast after destroy is not allowed") {
    sc = new SparkContext("local", "test")
    val broadcast = sc.smallBroadcast(Array(1, 2, 3, 4))
    broadcast.destroy(blocking = true)
    intercept[SparkException] {
      broadcast.value
    }
  }

  test("Broadcast variables cannot be created after SparkContext is stopped (SPARK-5065)") {
    sc = new SparkContext("local", "test")
    sc.stop()
    val thrown = intercept[IllegalStateException] {
      sc.smallBroadcast(Seq(1, 2, 3))
    }
    assert(thrown.getMessage.toLowerCase(Locale.ROOT).contains("stopped"))
  }

  test("Forbid broadcasting RDD directly") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val rdd = sc.parallelize(1 to 4)
    intercept[IllegalArgumentException] {
      sc.smallBroadcast(rdd)
    }
    sc.stop()
  }

  test("One small broadcast value instance per executor") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.smallBroadcast(list)
    val instances = sc.parallelize(1 to 10)
      .map(_ => System.identityHashCode(broadcast.value))
      .collect()
      .toSet

    assert(instances.size === 1)
  }

  /**
   * This test runs in 4 steps:
   *
   * 1) Create broadcast variable, and verify that all state is persisted on the driver.
   * 2) Use the broadcast variable on all executors, and verify that all state is persisted
   * on both the driver and the executors.
   * 3) Unpersist the broadcast, and verify that all state is removed where they should be.
   * 4) [Optional] If removeFromDriver is false, we verify that the broadcast is re-usable.
   */
  private def testUnpersistBroadcast(
    numWorkers: Int, // used only when distributed = true
    afterCreation: Long => Unit,
    afterUsingBroadcast: Long => Unit,
    afterUnpersist: Long => Unit): Unit = {

    sc = {
      val _sc =
        new SparkContext("local-cluster[%d, 1, 1024]".format(numWorkers), "test")
      // Wait until all salves are up
      try {
        TestUtils.waitUntilExecutorsUp(_sc, numWorkers, 60000)
        _sc
      } catch {
        case e: Throwable =>
          _sc.stop()
          throw e
      }
    }
    val list = List[Int](1, 2, 3, 4)

    // Create broadcast variable
    val broadcast = sc.smallBroadcast(list)
    afterCreation(broadcast.id)

    // Use broadcast variable on all executors
    val partitions = 10
    assert(partitions > numWorkers)
    val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    afterUsingBroadcast(broadcast.id)

    // destroy broadcast
    broadcast.destroy(blocking = true)

    afterUnpersist(broadcast.id)

    // After destroy, all subsequent uses of the broadcast variable should throw SparkExceptions.
    intercept[SparkException] {
      broadcast.value
    }
    intercept[SparkException] {
      broadcast.unpersist(blocking = true)
    }
    intercept[SparkException] {
      broadcast.destroy(blocking = true)
    }
  }
}

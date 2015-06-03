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

import scala.util.Random

import org.scalatest.Assertions
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._

// Dummy class that creates a broadcast variable but doesn't use it
class DummyBroadcastClass(rdd: RDD[Int]) extends Serializable {
  @transient val list = List(1, 2, 3, 4)
  val broadcast = rdd.context.broadcast(list)
  val bid = broadcast.id

  def doSomething(): Set[(Int, Boolean)] = {
    rdd.map { x =>
      val bm = SparkEnv.get.blockManager
      // Check if broadcast block was fetched
      val isFound = bm.getLocal(BroadcastBlockId(bid)).isDefined
      (x, isFound)
    }.collect().toSet
  }
}

class BroadcastSuite extends SparkFunSuite with LocalSparkContext {

  private val httpConf = broadcastConf("HttpBroadcastFactory")
  private val torrentConf = broadcastConf("TorrentBroadcastFactory")

  test("Using HttpBroadcast locally") {
    sc = new SparkContext("local", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing HttpBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing HttpBroadcast variables in a local cluster") {
    val numSlaves = 4
    val conf = httpConf.clone
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.broadcast.compress", "true")
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  test("Using TorrentBroadcast locally") {
    sc = new SparkContext("local", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing TorrentBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing TorrentBroadcast variables in a local cluster") {
    val numSlaves = 4
    val conf = torrentConf.clone
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.broadcast.compress", "true")
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  test("TorrentBroadcast's blockifyObject and unblockifyObject are inverses") {
    import org.apache.spark.broadcast.TorrentBroadcast._
    val blockSize = 1024
    val conf = new SparkConf()
    val compressionCodec = Some(new SnappyCompressionCodec(conf))
    val serializer = new JavaSerializer(conf)
    val seed = 42
    val rand = new Random(seed)
    for (trial <- 1 to 100) {
      val size = 1 + rand.nextInt(1024 * 10)
      val data: Array[Byte] = new Array[Byte](size)
      rand.nextBytes(data)
      val blocks = blockifyObject(data, blockSize, serializer, compressionCodec)
      val unblockified = unBlockifyObject[Array[Byte]](blocks, serializer, compressionCodec)
      assert(unblockified === data)
    }
  }

  test("Test Lazy Broadcast variables with TorrentBroadcast") {
    val numSlaves = 2
    val conf = torrentConf.clone
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", conf)
    val rdd = sc.parallelize(1 to numSlaves)

    val results = new DummyBroadcastClass(rdd).doSomething()

    assert(results.toSet === (1 to numSlaves).map(x => (x, false)).toSet)
  }

  test("Unpersisting HttpBroadcast on executors only in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = false)
  }

  test("Unpersisting HttpBroadcast on executors and driver in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = true)
  }

  test("Unpersisting HttpBroadcast on executors only in distributed mode") {
    testUnpersistHttpBroadcast(distributed = true, removeFromDriver = false)
  }

  test("Unpersisting HttpBroadcast on executors and driver in distributed mode") {
    testUnpersistHttpBroadcast(distributed = true, removeFromDriver = true)
  }

  test("Unpersisting TorrentBroadcast on executors only in local mode") {
    testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = false)
  }

  test("Unpersisting TorrentBroadcast on executors and driver in local mode") {
    testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = true)
  }

  test("Unpersisting TorrentBroadcast on executors only in distributed mode") {
    testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = false)
  }

  test("Unpersisting TorrentBroadcast on executors and driver in distributed mode") {
    testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = true)
  }

  test("Using broadcast after destroy prints callsite") {
    sc = new SparkContext("local", "test")
    testPackage.runCallSiteTest(sc)
  }

  test("Broadcast variables cannot be created after SparkContext is stopped (SPARK-5065)") {
    sc = new SparkContext("local", "test")
    sc.stop()
    val thrown = intercept[IllegalStateException] {
      sc.broadcast(Seq(1, 2, 3))
    }
    assert(thrown.getMessage.toLowerCase.contains("stopped"))
  }

  /**
   * Verify the persistence of state associated with an HttpBroadcast in either local mode or
   * local-cluster mode (when distributed = true).
   *
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * In between each step, this test verifies that the broadcast blocks and the broadcast file
   * are present only on the expected nodes.
   */
  private def testUnpersistHttpBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0

    // Verify that the broadcast file is created, and blocks are persisted only on the driver
    def afterCreation(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)
      statuses.head match { case (bm, status) =>
        assert(bm.isDriver, "Block should only be on the driver")
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
        assert(status.memSize > 0, "Block should be in memory store on the driver")
        assert(status.diskSize === 0, "Block should not be in disk store on the driver")
      }
      if (distributed) {
        // this file is only generated in distributed mode
        assert(HttpBroadcast.getFile(blockId.broadcastId).exists, "Broadcast file not found!")
      }
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === numSlaves + 1)
      statuses.foreach { case (_, status) =>
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
        assert(status.memSize > 0, "Block should be in memory store")
        assert(status.diskSize === 0, "Block should not be in disk store")
      }
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true. In the latter case, also verify that the broadcast file is deleted on the driver.
    def afterUnpersist(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      val expectedNumBlocks = if (removeFromDriver) 0 else 1
      val possiblyNot = if (removeFromDriver) "" else " not"
      assert(statuses.size === expectedNumBlocks,
        "Block should%s be unpersisted on the driver".format(possiblyNot))
      if (distributed && removeFromDriver) {
        // this file is only generated in distributed mode
        assert(!HttpBroadcast.getFile(blockId.broadcastId).exists,
          "Broadcast file should%s be deleted".format(possiblyNot))
      }
    }

    testUnpersistBroadcast(distributed, numSlaves, httpConf, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * Verify the persistence of state associated with an TorrentBroadcast in a local-cluster.
   *
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * In between each step, this test verifies that the broadcast blocks are present only on the
   * expected nodes.
   */
  private def testUnpersistTorrentBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0

    // Verify that blocks are persisted only on the driver
    def afterCreation(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      var statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === numSlaves + 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      assert(statuses.size === numSlaves + 1)
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true.
    def afterUnpersist(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      var expectedNumBlocks = if (removeFromDriver) 0 else 1
      var statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === expectedNumBlocks)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      expectedNumBlocks = if (removeFromDriver) 0 else 1
      statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === expectedNumBlocks)
    }

    testUnpersistBroadcast(distributed, numSlaves, torrentConf, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * This test runs in 4 steps:
   *
   * 1) Create broadcast variable, and verify that all state is persisted on the driver.
   * 2) Use the broadcast variable on all executors, and verify that all state is persisted
   *    on both the driver and the executors.
   * 3) Unpersist the broadcast, and verify that all state is removed where they should be.
   * 4) [Optional] If removeFromDriver is false, we verify that the broadcast is re-usable.
   */
  private def testUnpersistBroadcast(
      distributed: Boolean,
      numSlaves: Int,  // used only when distributed = true
      broadcastConf: SparkConf,
      afterCreation: (Long, BlockManagerMaster) => Unit,
      afterUsingBroadcast: (Long, BlockManagerMaster) => Unit,
      afterUnpersist: (Long, BlockManagerMaster) => Unit,
      removeFromDriver: Boolean) {

    sc = if (distributed) {
      val _sc =
        new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", broadcastConf)
      // Wait until all salves are up
      _sc.jobProgressListener.waitUntilExecutorsUp(numSlaves, 10000)
      _sc
    } else {
      new SparkContext("local", "test", broadcastConf)
    }
    val blockManagerMaster = sc.env.blockManager.master
    val list = List[Int](1, 2, 3, 4)

    // Create broadcast variable
    val broadcast = sc.broadcast(list)
    afterCreation(broadcast.id, blockManagerMaster)

    // Use broadcast variable on all executors
    val partitions = 10
    assert(partitions > numSlaves)
    val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    afterUsingBroadcast(broadcast.id, blockManagerMaster)

    // Unpersist broadcast
    if (removeFromDriver) {
      broadcast.destroy(blocking = true)
    } else {
      broadcast.unpersist(blocking = true)
    }
    afterUnpersist(broadcast.id, blockManagerMaster)

    // If the broadcast is removed from driver, all subsequent uses of the broadcast variable
    // should throw SparkExceptions. Otherwise, the result should be the same as before.
    if (removeFromDriver) {
      // Using this variable on the executors crashes them, which hangs the test.
      // Instead, crash the driver by directly accessing the broadcast value.
      intercept[SparkException] { broadcast.value }
      intercept[SparkException] { broadcast.unpersist() }
      intercept[SparkException] { broadcast.destroy(blocking = true) }
    } else {
      val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
      assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    }
  }

  /** Helper method to create a SparkConf that uses the given broadcast factory. */
  private def broadcastConf(factoryName: String): SparkConf = {
    val conf = new SparkConf
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.%s".format(factoryName))
    conf
  }
}

package object testPackage extends Assertions {

  def runCallSiteTest(sc: SparkContext) {
    val broadcast = sc.broadcast(Array(1, 2, 3, 4))
    broadcast.destroy()
    val thrown = intercept[SparkException] { broadcast.value }
    assert(thrown.getMessage.contains("BroadcastSuite.scala"))
  }

}

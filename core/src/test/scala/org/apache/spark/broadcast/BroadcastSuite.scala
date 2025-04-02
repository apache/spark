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

import scala.util.Random

import org.scalatest.Assertions

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.SERIALIZER
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.security.EncryptionFunSuite
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._
import org.apache.spark.util.io.ChunkedByteBuffer

// Dummy class that creates a broadcast variable but doesn't use it
class DummyBroadcastClass(rdd: RDD[Int]) extends Serializable {
  @transient val list = List(1, 2, 3, 4)
  val broadcast = rdd.context.broadcast(list)
  val bid = broadcast.id

  def doSomething(): Set[(Int, Boolean)] = {
    rdd.map { x =>
      val bm = SparkEnv.get.blockManager
      // Check if broadcast block was fetched
      val isFound = bm.getLocalValues(BroadcastBlockId(bid)).isDefined
      (x, isFound)
    }.collect().toSet
  }
}

class BroadcastSuite extends SparkFunSuite with LocalSparkContext with EncryptionFunSuite {

  test("Using TorrentBroadcast locally") {
    sc = new SparkContext("local", "test")
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing TorrentBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test")
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  encryptionTest("Accessing TorrentBroadcast variables in a local cluster") { conf =>
    val numWorkers = 4
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(config.BROADCAST_COMPRESS, true)
    sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numWorkers), "test", conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numWorkers).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numWorkers).map(x => (x, 10)).toSet)
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
      val blocks = blockifyObject(data, blockSize, serializer, compressionCodec).map { b =>
        new ChunkedByteBuffer(b).toInputStream(dispose = true)
      }
      val unblockified = unBlockifyObject[Array[Byte]](blocks, serializer, compressionCodec)
      assert(unblockified === data)
    }
  }

  test("Test Lazy Broadcast variables with TorrentBroadcast") {
    val numWorkers = 2
    sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numWorkers), "test")
    val rdd = sc.parallelize(1 to numWorkers)
    val results = new DummyBroadcastClass(rdd).doSomething()

    assert(results.toSet === (1 to numWorkers).map(x => (x, false)).toSet)
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
    assert(thrown.getMessage.toLowerCase(Locale.ROOT).contains("stopped"))
  }

  test("Forbid broadcasting RDD directly") {
    sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val rdd = sc.parallelize(1 to 4)
    intercept[IllegalArgumentException] { sc.broadcast(rdd) }
    sc.stop()
  }

  encryptionTest("Cache broadcast to disk") { conf =>
    conf.setMaster("local")
      .setAppName("test")
      .set(config.MEMORY_STORAGE_FRACTION, 0.0)
    sc = new SparkContext(conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    assert(broadcast.value.sum === 10)
  }

  test("One broadcast value instance per executor") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val instances = sc.parallelize(1 to 10)
      .map(x => System.identityHashCode(broadcast.value))
      .collect()
      .toSet

    assert(instances.size === 1)
  }

  test("One broadcast value instance per executor when memory is constrained") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
      .set(config.MEMORY_STORAGE_FRACTION, 0.0)

    sc = new SparkContext(conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val instances = sc.parallelize(1 to 10)
      .map(x => System.identityHashCode(broadcast.value))
      .collect()
      .toSet

    assert(instances.size === 1)
  }

  test("SPARK-39983 - Broadcasted value not cached on driver") {
    // Use distributed cluster as in local mode the broabcast value is actually cached.
    val conf = new SparkConf()
      .setMaster("local-cluster[2,1,1024]")
      .setAppName("test")
    sc = new SparkContext(conf)

    sc.broadcastInternal(value = 1234, serializedOnly = false) match {
      case tb: TorrentBroadcast[Int] =>
        assert(tb.hasCachedValue)
        assert(1234 === tb.value)
    }
    sc.broadcastInternal(value = 1234, serializedOnly = true) match {
      case tb: TorrentBroadcast[Int] =>
        assert(!tb.hasCachedValue)
        assert(1234 === tb.value)
    }
  }

  /**
   * Verify the persistence of state associated with a TorrentBroadcast in a local-cluster.
   *
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * In between each step, this test verifies that the broadcast blocks are present only on the
   * expected nodes.
   */
  private def testUnpersistTorrentBroadcast(distributed: Boolean,
      removeFromDriver: Boolean): Unit = {
    val numWorkers = if (distributed) 2 else 0

    // Verify that blocks are persisted only on the driver
    def afterCreation(broadcastId: Long, bmm: BlockManagerMaster): Unit = {
      var blockId = BroadcastBlockId(broadcastId)
      var statuses = bmm.getBlockStatus(blockId, askStorageEndpoints = true)
      assert(statuses.size === 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      statuses = bmm.getBlockStatus(blockId, askStorageEndpoints = true)
      assert(statuses.size === 1)
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(broadcastId: Long, bmm: BlockManagerMaster): Unit = {
      var blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askStorageEndpoints = true)
      assert(statuses.size === numWorkers + 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      assert(statuses.size === numWorkers + 1)
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true.
    def afterUnpersist(broadcastId: Long, bmm: BlockManagerMaster): Unit = {
      var blockId = BroadcastBlockId(broadcastId)
      var expectedNumBlocks = if (removeFromDriver) 0 else 1
      var statuses = bmm.getBlockStatus(blockId, askStorageEndpoints = true)
      assert(statuses.size === expectedNumBlocks)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      expectedNumBlocks = if (removeFromDriver) 0 else 1
      statuses = bmm.getBlockStatus(blockId, askStorageEndpoints = true)
      assert(statuses.size === expectedNumBlocks)
    }

    testUnpersistBroadcast(distributed, numWorkers, afterCreation,
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
      numWorkers: Int,  // used only when distributed = true
      afterCreation: (Long, BlockManagerMaster) => Unit,
      afterUsingBroadcast: (Long, BlockManagerMaster) => Unit,
      afterUnpersist: (Long, BlockManagerMaster) => Unit,
      removeFromDriver: Boolean): Unit = {

    sc = if (distributed) {
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
    } else {
      new SparkContext("local", "test")
    }
    val blockManagerMaster = sc.env.blockManager.master
    val list = List[Int](1, 2, 3, 4)

    // Create broadcast variable
    val broadcast = sc.broadcast(list)
    afterCreation(broadcast.id, blockManagerMaster)

    // Use broadcast variable on all executors
    val partitions = 10
    assert(partitions > numWorkers)
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
      val e1 = intercept[SparkException] { broadcast.value }
      assert(e1.isInternalError)
      assert(e1.getCondition == "INTERNAL_ERROR_BROADCAST")
      val e2 = intercept[SparkException] { broadcast.unpersist(blocking = true) }
      assert(e2.isInternalError)
      assert(e2.getCondition == "INTERNAL_ERROR_BROADCAST")
      val e3 = intercept[SparkException] { broadcast.destroy(blocking = true) }
      assert(e3.isInternalError)
      assert(e3.getCondition == "INTERNAL_ERROR_BROADCAST")
    } else {
      val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
      assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    }
  }
}

package object testPackage extends Assertions {

  def runCallSiteTest(sc: SparkContext): Unit = {
    val broadcast = sc.broadcast(Array(1, 2, 3, 4))
    broadcast.destroy(blocking = true)
    val thrown = intercept[SparkException] { broadcast.value }
    assert(thrown.getMessage.contains("BroadcastSuite.scala"))
    assert(thrown.isInternalError)
    assert(thrown.getCondition == "INTERNAL_ERROR_BROADCAST")
  }

}

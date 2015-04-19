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

import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.mockito.Mockito.{mock, when}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SecurityManager}
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._


class BlockManagerSuite extends FunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with ResetSystemProperties {

  private val conf = new SparkConf(false)
  var store: BlockManager = null
  var store2: BlockManager = null
  var rpcEnv: RpcEnv = null
  var master: BlockManagerMaster = null
  conf.set("spark.authenticate", "false")
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  conf.set("spark.kryoserializer.buffer.mb", "1")
  val serializer = new KryoSerializer(conf)

  // Implicitly convert strings to BlockIds for test clarity.
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int): RDDBlockId = RDDBlockId(rddId, splitId)

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    val transfer = new NioBlockTransferService(conf, securityMgr)
    val manager = new BlockManager(name, rpcEnv, master, serializer, maxMem, conf,
      mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    manager.initialize("app-id")
    manager
  }

  override def beforeEach(): Unit = {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf.set("os.arch", "amd64")
    conf.set("spark.test.useCompressedOops", "true")
    conf.set("spark.driver.port", rpcEnv.address.port.toString)
    conf.set("spark.storage.unrollFraction", "0.4")
    conf.set("spark.storage.unrollMemoryThreshold", "512")

    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf, new LiveListenerBus)), conf, true)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  override def afterEach(): Unit = {
    if (store != null) {
      store.stop()
      store = null
    }
    if (store2 != null) {
      store2.stop()
      store2 = null
    }
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null
    master = null
  }

  test("StorageLevel object caching") {
    val level1 = StorageLevel(false, false, false, false, 3)
    // this should return the same object as level1
    val level2 = StorageLevel(false, false, false, false, 3)
    // this should return a different object
    val level3 = StorageLevel(false, false, false, false, 2)
    assert(level2 === level1, "level2 is not same as level1")
    assert(level2.eq(level1), "level2 is not the same object as level1")
    assert(level3 != level1, "level3 is same as level1")
    val bytes1 = Utils.serialize(level1)
    val level1_ = Utils.deserialize[StorageLevel](bytes1)
    val bytes2 = Utils.serialize(level2)
    val level2_ = Utils.deserialize[StorageLevel](bytes2)
    assert(level1_ === level1, "Deserialized level1 not same as original level1")
    assert(level1_.eq(level1), "Deserialized level1 not the same object as original level2")
    assert(level2_ === level2, "Deserialized level2 not same as original level2")
    assert(level2_.eq(level1), "Deserialized level2 not the same object as original level1")
  }

  test("BlockManagerId object caching") {
    val id1 = BlockManagerId("e1", "XXX", 1)
    val id2 = BlockManagerId("e1", "XXX", 1) // this should return the same object as id1
    val id3 = BlockManagerId("e1", "XXX", 2) // this should return a different object
    assert(id2 === id1, "id2 is not same as id1")
    assert(id2.eq(id1), "id2 is not the same object as id1")
    assert(id3 != id1, "id3 is same as id1")
    val bytes1 = Utils.serialize(id1)
    val id1_ = Utils.deserialize[BlockManagerId](bytes1)
    val bytes2 = Utils.serialize(id2)
    val id2_ = Utils.deserialize[BlockManagerId](bytes2)
    assert(id1_ === id1, "Deserialized id1 is not same as original id1")
    assert(id1_.eq(id1), "Deserialized id1 is not the same object as original id1")
    assert(id2_ === id2, "Deserialized id2 is not same as original id2")
    assert(id2_.eq(id1), "Deserialized id2 is not the same object as original id1")
  }

  test("BlockManagerId.isDriver() backwards-compatibility with legacy driver ids (SPARK-6716)") {
    assert(BlockManagerId(SparkContext.DRIVER_IDENTIFIER, "XXX", 1).isDriver)
    assert(BlockManagerId(SparkContext.LEGACY_DRIVER_IDENTIFIER, "XXX", 1).isDriver)
    assert(!BlockManagerId("notADriverIdentifier", "XXX", 1).isDriver)
  }

  test("master + 1 manager interaction") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1").size > 0, "master was not told about a1")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
    assert(master.getLocations("a3").size === 0, "master was told about a3")

    // Drop a1 and a2 from memory; this should be reported back to the master
    store.dropFromMemory("a1", null: Either[Array[Any], ByteBuffer])
    store.dropFromMemory("a2", null: Either[Array[Any], ByteBuffer])
    assert(store.getSingle("a1") === None, "a1 not removed from store")
    assert(store.getSingle("a2") === None, "a2 not removed from store")
    assert(master.getLocations("a1").size === 0, "master did not remove a1")
    assert(master.getLocations("a2").size === 0, "master did not remove a2")
  }

  test("master + 2 managers interaction") {
    store = makeBlockManager(2000, "exec1")
    store2 = makeBlockManager(2000, "exec2")

    val peers = master.getPeers(store.blockManagerId)
    assert(peers.size === 1, "master did not return the other manager as a peer")
    assert(peers.head === store2.blockManagerId, "peer returned by master is not the other manager")

    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_2)
    store2.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_2)
    assert(master.getLocations("a1").size === 2, "master did not report 2 locations for a1")
    assert(master.getLocations("a2").size === 2, "master did not report 2 locations for a2")
  }

  test("removing block") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2 and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1-to-remove", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2-to-remove", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3-to-remove", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory and memory size
    val memStatus = master.getMemoryStatus.head._2
    assert(memStatus._1 == 20000L, "total memory " + memStatus._1 + " should equal 20000")
    assert(memStatus._2 <= 12000L, "remaining memory " + memStatus._2 + " should <= 12000")
    assert(store.getSingle("a1-to-remove").isDefined, "a1 was not in store")
    assert(store.getSingle("a2-to-remove").isDefined, "a2 was not in store")
    assert(store.getSingle("a3-to-remove").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1-to-remove").size > 0, "master was not told about a1")
    assert(master.getLocations("a2-to-remove").size > 0, "master was not told about a2")
    assert(master.getLocations("a3-to-remove").size === 0, "master was told about a3")

    // Remove a1 and a2 and a3. Should be no-op for a3.
    master.removeBlock("a1-to-remove")
    master.removeBlock("a2-to-remove")
    master.removeBlock("a3-to-remove")

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a1-to-remove") should be (None)
      master.getLocations("a1-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a2-to-remove") should be (None)
      master.getLocations("a2-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a3-to-remove") should not be (None)
      master.getLocations("a3-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      val memStatus = master.getMemoryStatus.head._2
      memStatus._1 should equal (20000L)
      memStatus._2 should equal (20000L)
    }
  }

  test("removing rdd") {
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    // Putting a1, a2 and a3 in memory.
    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("nonrddblock", a3, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = false)

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 0)) should be (None)
      master.getLocations(rdd(0, 0)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 1)) should be (None)
      master.getLocations(rdd(0, 1)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("nonrddblock") should not be (None)
      master.getLocations("nonrddblock") should have size (1)
    }

    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = true)
    store.getSingle(rdd(0, 0)) should be (None)
    master.getLocations(rdd(0, 0)) should have size 0
    store.getSingle(rdd(0, 1)) should be (None)
    master.getLocations(rdd(0, 1)) should have size 0
  }

  test("removing broadcast") {
    store = makeBlockManager(2000)
    val driverStore = store
    val executorStore = makeBlockManager(2000, "executor")
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    val a4 = new Array[Byte](400)

    val broadcast0BlockId = BroadcastBlockId(0)
    val broadcast1BlockId = BroadcastBlockId(1)
    val broadcast2BlockId = BroadcastBlockId(2)
    val broadcast2BlockId2 = BroadcastBlockId(2, "_")

    // insert broadcast blocks in both the stores
    Seq(driverStore, executorStore).foreach { case s =>
      s.putSingle(broadcast0BlockId, a1, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast1BlockId, a2, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast2BlockId, a3, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast2BlockId2, a4, StorageLevel.DISK_ONLY)
    }

    // verify whether the blocks exist in both the stores
    Seq(driverStore, executorStore).foreach { case s =>
      s.getLocal(broadcast0BlockId) should not be (None)
      s.getLocal(broadcast1BlockId) should not be (None)
      s.getLocal(broadcast2BlockId) should not be (None)
      s.getLocal(broadcast2BlockId2) should not be (None)
    }

    // remove broadcast 0 block only from executors
    master.removeBroadcast(0, removeFromMaster = false, blocking = true)

    // only broadcast 0 block should be removed from the executor store
    executorStore.getLocal(broadcast0BlockId) should be (None)
    executorStore.getLocal(broadcast1BlockId) should not be (None)
    executorStore.getLocal(broadcast2BlockId) should not be (None)

    // nothing should be removed from the driver store
    driverStore.getLocal(broadcast0BlockId) should not be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)
    driverStore.getLocal(broadcast2BlockId) should not be (None)

    // remove broadcast 0 block from the driver as well
    master.removeBroadcast(0, removeFromMaster = true, blocking = true)
    driverStore.getLocal(broadcast0BlockId) should be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)

    // remove broadcast 1 block from both the stores asynchronously
    // and verify all broadcast 1 blocks have been removed
    master.removeBroadcast(1, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast1BlockId) should be (None)
      executorStore.getLocal(broadcast1BlockId) should be (None)
    }

    // remove broadcast 2 from both the stores asynchronously
    // and verify all broadcast 2 blocks have been removed
    master.removeBroadcast(2, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast2BlockId) should be (None)
      driverStore.getLocal(broadcast2BlockId2) should be (None)
      executorStore.getLocal(broadcast2BlockId) should be (None)
      executorStore.getLocal(broadcast2BlockId2) should be (None)
    }
    executorStore.stop()
    driverStore.stop()
    store = null
  }

  test("reregistration on heart beat") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    val reregister = !master.driverEndpoint.askWithReply[Boolean](
      BlockManagerHeartbeat(store.blockManagerId))
    assert(reregister == true)
  }

  test("reregistration on block update") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.waitForAsyncReregister()

    assert(master.getLocations("a1").size > 0, "a1 was not reregistered with master")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
  }

  test("reregistration doesn't dead lock") {
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = List(new Array[Byte](400))

    // try many times to trigger any deadlocks
    for (i <- 1 to 100) {
      master.removeExecutor(store.blockManagerId.executorId)
      val t1 = new Thread {
        override def run() {
          store.putIterator("a2", a2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
        }
      }
      val t2 = new Thread {
        override def run() {
          store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
        }
      }
      val t3 = new Thread {
        override def run() {
          store.reregister()
        }
      }

      t1.start()
      t2.start()
      t3.start()
      t1.join()
      t2.join()
      t3.join()

      store.dropFromMemory("a1", null: Either[Array[Any], ByteBuffer])
      store.dropFromMemory("a2", null: Either[Array[Any], ByteBuffer])
      store.waitForAsyncReregister()
    }
  }

  test("correct BlockResult returned from get() calls") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](500), new Array[Byte](1000), new Array[Byte](1500))
    val list1SizeEstimate = SizeEstimator.estimate(list1.iterator.toArray)
    val list2SizeEstimate = SizeEstimator.estimate(list2.iterator.toArray)
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2memory", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2disk", list2.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val list1Get = store.get("list1")
    assert(list1Get.isDefined, "list1 expected to be in store")
    assert(list1Get.get.data.size === 2)
    assert(list1Get.get.inputMetrics.bytesRead === list1SizeEstimate)
    assert(list1Get.get.inputMetrics.readMethod === DataReadMethod.Memory)
    val list2MemoryGet = store.get("list2memory")
    assert(list2MemoryGet.isDefined, "list2memory expected to be in store")
    assert(list2MemoryGet.get.data.size === 3)
    assert(list2MemoryGet.get.inputMetrics.bytesRead === list2SizeEstimate)
    assert(list2MemoryGet.get.inputMetrics.readMethod === DataReadMethod.Memory)
    val list2DiskGet = store.get("list2disk")
    assert(list2DiskGet.isDefined, "list2memory expected to be in store")
    assert(list2DiskGet.get.data.size === 3)
    // We don't know the exact size of the data on disk, but it should certainly be > 0.
    assert(list2DiskGet.get.inputMetrics.bytesRead > 0)
    assert(list2DiskGet.get.inputMetrics.readMethod === DataReadMethod.Disk)
  }

  test("in-memory LRU storage") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU storage with serialization") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU for partitions of same RDD") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle(rdd(0, 1), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 3), a3, StorageLevel.MEMORY_ONLY)
    // Even though we accessed rdd_0_3 last, it should not have replaced partitions 1 and 2
    // from the same RDD
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
    assert(store.getSingle(rdd(0, 1)).isDefined, "rdd_0_1 was not in store")
    // Check that rdd_0_3 doesn't replace them even after further accesses
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
  }

  test("in-memory LRU for partitions of multiple RDDs") {
    store = makeBlockManager(12000)
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // At this point rdd_1_1 should've replaced rdd_0_1
    assert(store.memoryStore.contains(rdd(1, 1)), "rdd_1_1 was not in store")
    assert(!store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was in store")
    assert(store.memoryStore.contains(rdd(0, 2)), "rdd_0_2 was not in store")
    // Do a get() on rdd_0_2 so that it is the most recently used item
    assert(store.getSingle(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
    // Put in more partitions from RDD 0; they should replace rdd_1_1
    store.putSingle(rdd(0, 3), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 4), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // Now rdd_1_1 should be dropped to add rdd_0_3, but then rdd_0_2 should *not* be dropped
    // when we try to add rdd_0_4.
    assert(!store.memoryStore.contains(rdd(1, 1)), "rdd_1_1 was in store")
    assert(!store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was in store")
    assert(!store.memoryStore.contains(rdd(0, 4)), "rdd_0_4 was in store")
    assert(store.memoryStore.contains(rdd(0, 2)), "rdd_0_2 was not in store")
    assert(store.memoryStore.contains(rdd(0, 3)), "rdd_0_3 was not in store")
  }

  test("tachyon storage") {
    // TODO Make the spark.test.tachyon.enable true after using tachyon 0.5.0 testing jar.
    val tachyonUnitTestEnabled = conf.getBoolean("spark.test.tachyon.enable", false)
    if (tachyonUnitTestEnabled) {
      store = makeBlockManager(1200)
      val a1 = new Array[Byte](400)
      val a2 = new Array[Byte](400)
      val a3 = new Array[Byte](400)
      store.putSingle("a1", a1, StorageLevel.OFF_HEAP)
      store.putSingle("a2", a2, StorageLevel.OFF_HEAP)
      store.putSingle("a3", a3, StorageLevel.OFF_HEAP)
      assert(store.getSingle("a3").isDefined, "a3 was in store")
      assert(store.getSingle("a2").isDefined, "a2 was in store")
      assert(store.getSingle("a1").isDefined, "a1 was in store")
    } else {
      info("tachyon storage test disabled.")
    }
  }

  test("on-disk storage") {
    store = makeBlockManager(1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    store.putSingle("a2", a2, StorageLevel.DISK_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    assert(store.getSingle("a2").isDefined, "a2 was in store")
    assert(store.getSingle("a3").isDefined, "a3 was in store")
    assert(store.getSingle("a1").isDefined, "a1 was in store")
  }

  test("disk and memory storage") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("disk and memory storage with getLocalBytes") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getLocalBytes("a2").isDefined, "a2 was not in store")
    assert(store.getLocalBytes("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("disk and memory storage with serialization") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("disk and memory storage with serialization and getLocalBytes") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getLocalBytes("a2").isDefined, "a2 was not in store")
    assert(store.getLocalBytes("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("LRU with mixed storage levels") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    val a4 = new Array[Byte](4000)
    // First store a1 and a2, both in memory, and a3, on disk only
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    // At this point LRU should not kick in because a3 is only on disk
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    // Now let's add in a4, which uses both disk and memory; a1 should drop out
    store.putSingle("a4", a4, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a4").isDefined, "a4 was not in store")
  }

  test("in-memory LRU with streams") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list3", list3.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    // At this point list2 was gotten last, so LRU will getSingle rid of list3
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3") === None, "list1 was in store")
  }

  test("LRU with mixed storage levels and streams") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list4 = List(new Array[Byte](2000), new Array[Byte](2000))
    // First store list1 and list2, both in memory, and list3, on disk only
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator("list2", list2.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator("list3", list3.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val listForSizeEstimate = new ArrayBuffer[Any]
    listForSizeEstimate ++= list1.iterator
    val listSize = SizeEstimator.estimate(listForSizeEstimate)
    // At this point LRU should not kick in because list3 is only on disk
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    // Now let's add in list4, which uses both disk and memory; list1 should drop out
    store.putIterator("list4", list4.iterator, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list4").isDefined, "list4 was not in store")
    assert(store.get("list4").get.data.size === 2)
  }

  test("negative byte values in ByteBufferInputStream") {
    val buffer = ByteBuffer.wrap(Array[Int](254, 255, 0, 1, 2).map(_.toByte).toArray)
    val stream = new ByteBufferInputStream(buffer)
    val temp = new Array[Byte](10)
    assert(stream.read() === 254, "unexpected byte read")
    assert(stream.read() === 255, "unexpected byte read")
    assert(stream.read() === 0, "unexpected byte read")
    assert(stream.read(temp, 0, temp.length) === 2, "unexpected number of bytes read")
    assert(stream.read() === -1, "end of stream not signalled")
    assert(stream.read(temp, 0, temp.length) === -1, "end of stream not signalled")
  }

  test("overly large block") {
    store = makeBlockManager(5000)
    store.putSingle("a1", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1") === None, "a1 was in store")
    store.putSingle("a2", new Array[Byte](10000), StorageLevel.MEMORY_AND_DISK)
    assert(store.memoryStore.getValues("a2") === None, "a2 was in memory store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
  }

  test("block compression") {
    try {
      conf.set("spark.shuffle.compress", "true")
      store = makeBlockManager(20000, "exec1")
      store.putSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) <= 100,
        "shuffle_0_0_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.shuffle.compress", "false")
      store = makeBlockManager(20000, "exec2")
      store.putSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) >= 10000,
        "shuffle_0_0_0 was compressed")
      store.stop()
      store = null

      conf.set("spark.broadcast.compress", "true")
      store = makeBlockManager(20000, "exec3")
      store.putSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) <= 1000,
        "broadcast_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.broadcast.compress", "false")
      store = makeBlockManager(20000, "exec4")
      store.putSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) >= 10000, "broadcast_0 was compressed")
      store.stop()
      store = null

      conf.set("spark.rdd.compress", "true")
      store = makeBlockManager(20000, "exec5")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) <= 1000, "rdd_0_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.rdd.compress", "false")
      store = makeBlockManager(20000, "exec6")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) >= 10000, "rdd_0_0 was compressed")
      store.stop()
      store = null

      // Check that any other block types are also kept uncompressed
      store = makeBlockManager(20000, "exec7")
      store.putSingle("other_block", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
      assert(store.memoryStore.getSize("other_block") >= 10000, "other_block was compressed")
      store.stop()
      store = null
    } finally {
      System.clearProperty("spark.shuffle.compress")
      System.clearProperty("spark.broadcast.compress")
      System.clearProperty("spark.rdd.compress")
    }
  }

  test("block store put failure") {
    // Use Java serializer so we can create an unserializable error.
    val transfer = new NioBlockTransferService(conf, securityMgr)
    store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      new JavaSerializer(conf), 1200, conf, mapOutputTracker, shuffleManager, transfer, securityMgr,
      0)

    // The put should fail since a1 is not serializable.
    class UnserializableClass
    val a1 = new UnserializableClass
    intercept[java.io.NotSerializableException] {
      store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    }

    // Make sure get a1 doesn't hang and returns None.
    failAfter(1 second) {
      assert(store.getSingle("a1") == None, "a1 should not be in store")
    }
  }

  test("reads of memory-mapped and non memory-mapped files are equivalent") {
    val confKey = "spark.storage.memoryMapThreshold"

    // Create a non-trivial (not all zeros) byte array
    var counter = 0.toByte
    def incr: Byte = {counter = (counter + 1).toByte; counter;}
    val bytes = Array.fill[Byte](1000)(incr)
    val byteBuffer = ByteBuffer.wrap(bytes)

    val blockId = BlockId("rdd_1_2")

    // This sequence of mocks makes these tests fairly brittle. It would
    // be nice to refactor classes involved in disk storage in a way that
    // allows for easier testing.
    val blockManager = mock(classOf[BlockManager])
    when(blockManager.conf).thenReturn(conf.clone.set(confKey, 0.toString))
    val diskBlockManager = new DiskBlockManager(blockManager, conf)

    val diskStoreMapped = new DiskStore(blockManager, diskBlockManager)
    diskStoreMapped.putBytes(blockId, byteBuffer, StorageLevel.DISK_ONLY)
    val mapped = diskStoreMapped.getBytes(blockId).get

    when(blockManager.conf).thenReturn(conf.clone.set(confKey, (1000 * 1000).toString))
    val diskStoreNotMapped = new DiskStore(blockManager, diskBlockManager)
    diskStoreNotMapped.putBytes(blockId, byteBuffer, StorageLevel.DISK_ONLY)
    val notMapped = diskStoreNotMapped.getBytes(blockId).get

    // Not possible to do isInstanceOf due to visibility of HeapByteBuffer
    assert(notMapped.getClass.getName.endsWith("HeapByteBuffer"),
      "Expected HeapByteBuffer for un-mapped read")
    assert(mapped.isInstanceOf[MappedByteBuffer], "Expected MappedByteBuffer for mapped read")

    def arrayFromByteBuffer(in: ByteBuffer): Array[Byte] = {
      val array = new Array[Byte](in.remaining())
      in.get(array)
      array
    }

    val mappedAsArray = arrayFromByteBuffer(mapped)
    val notMappedAsArray = arrayFromByteBuffer(notMapped)
    assert(Arrays.equals(mappedAsArray, bytes))
    assert(Arrays.equals(notMappedAsArray, bytes))
  }

  test("updated block statuses") {
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))
    val bigList = List.fill(8)(new Array[Byte](2000))

    // 1 updated block (i.e. list1)
    val updatedBlocks1 =
      store.putIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks1.size === 1)
    assert(updatedBlocks1.head._1 === TestBlockId("list1"))
    assert(updatedBlocks1.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 1 updated block (i.e. list2)
    val updatedBlocks2 =
      store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    assert(updatedBlocks2.size === 1)
    assert(updatedBlocks2.head._1 === TestBlockId("list2"))
    assert(updatedBlocks2.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 2 updated blocks - list1 is kicked out of memory while list3 is added
    val updatedBlocks3 =
      store.putIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks3.size === 2)
    updatedBlocks3.foreach { case (id, status) =>
      id match {
        case TestBlockId("list1") => assert(status.storageLevel === StorageLevel.NONE)
        case TestBlockId("list3") => assert(status.storageLevel === StorageLevel.MEMORY_ONLY)
        case _ => fail("Updated block is neither list1 nor list3")
      }
    }
    assert(store.memoryStore.contains("list3"), "list3 was not in memory store")

    // 2 updated blocks - list2 is kicked out of memory (but put on disk) while list4 is added
    val updatedBlocks4 =
      store.putIterator("list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks4.size === 2)
    updatedBlocks4.foreach { case (id, status) =>
      id match {
        case TestBlockId("list2") => assert(status.storageLevel === StorageLevel.DISK_ONLY)
        case TestBlockId("list4") => assert(status.storageLevel === StorageLevel.MEMORY_ONLY)
        case _ => fail("Updated block is neither list2 nor list4")
      }
    }
    assert(store.diskStore.contains("list2"), "list2 was not in disk store")
    assert(store.memoryStore.contains("list4"), "list4 was not in memory store")

    // No updated blocks - list5 is too big to fit in store and nothing is kicked out
    val updatedBlocks5 =
      store.putIterator("list5", bigList.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks5.size === 0)

    // memory store contains only list3 and list4
    assert(!store.memoryStore.contains("list1"), "list1 was in memory store")
    assert(!store.memoryStore.contains("list2"), "list2 was in memory store")
    assert(store.memoryStore.contains("list3"), "list3 was not in memory store")
    assert(store.memoryStore.contains("list4"), "list4 was not in memory store")
    assert(!store.memoryStore.contains("list5"), "list5 was in memory store")

    // disk store contains only list2
    assert(!store.diskStore.contains("list1"), "list1 was in disk store")
    assert(store.diskStore.contains("list2"), "list2 was not in disk store")
    assert(!store.diskStore.contains("list3"), "list3 was in disk store")
    assert(!store.diskStore.contains("list4"), "list4 was in disk store")
    assert(!store.diskStore.contains("list5"), "list5 was in disk store")
  }

  test("query block statuses") {
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))

    // Tell master. By LRU, only list2 and list3 remains.
    store.putIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getLocations("list1").size === 0)
    assert(store.master.getLocations("list2").size === 1)
    assert(store.master.getLocations("list3").size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list2", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = true).size === 0)
    assert(store.master.getBlockStatus("list2", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = true).size === 1)

    // This time don't tell master and see what happens. By LRU, only list5 and list6 remains.
    store.putIterator("list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.putIterator("list5", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator("list6", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // getLocations should return nothing because the master is not informed
    // getBlockStatus without asking slaves should have the same result
    // getBlockStatus with asking slaves, however, should return the actual block statuses
    assert(store.master.getLocations("list4").size === 0)
    assert(store.master.getLocations("list5").size === 0)
    assert(store.master.getLocations("list6").size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list5", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list6", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = true).size === 0)
    assert(store.master.getBlockStatus("list5", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list6", askSlaves = true).size === 1)
  }

  test("get matching blocks") {
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](100))

    // insert some blocks
    store.putIterator("list1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("list"), askSlaves = false).size
      === 3)
    assert(store.master.getMatchingBlockIds(_.toString.contains("list1"), askSlaves = false).size
      === 1)

    // insert some more blocks
    store.putIterator("newlist1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("newlist2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator("newlist3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = false).size
      === 1)
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = true).size
      === 3)

    val blockIds = Seq(RDDBlockId(1, 0), RDDBlockId(1, 1), RDDBlockId(2, 0))
    blockIds.foreach { blockId =>
      store.putIterator(blockId, list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    val matchedBlockIds = store.master.getMatchingBlockIds(_ match {
      case RDDBlockId(1, _) => true
      case _ => false
    }, askSlaves = true)
    assert(matchedBlockIds.toSet === Set(RDDBlockId(1, 0), RDDBlockId(1, 1)))
  }

  test("SPARK-1194 regression: fix the same-RDD rule for cache replacement") {
    store = makeBlockManager(12000)
    store.putSingle(rdd(0, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // Access rdd_1_0 to ensure it's not least recently used.
    assert(store.getSingle(rdd(1, 0)).isDefined, "rdd_1_0 was not in store")
    // According to the same-RDD rule, rdd_1_0 should be replaced here.
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // rdd_1_0 should have been replaced, even it's not least recently used.
    assert(store.memoryStore.contains(rdd(0, 0)), "rdd_0_0 was not in store")
    assert(store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was not in store")
    assert(!store.memoryStore.contains(rdd(1, 0)), "rdd_1_0 was in store")
  }

  test("reserve/release unroll memory") {
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    assert(memoryStore.currentUnrollMemory === 0)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Reserve
    memoryStore.reserveUnrollMemoryForThisThread(100)
    assert(memoryStore.currentUnrollMemoryForThisThread === 100)
    memoryStore.reserveUnrollMemoryForThisThread(200)
    assert(memoryStore.currentUnrollMemoryForThisThread === 300)
    memoryStore.reserveUnrollMemoryForThisThread(500)
    assert(memoryStore.currentUnrollMemoryForThisThread === 800)
    memoryStore.reserveUnrollMemoryForThisThread(1000000)
    assert(memoryStore.currentUnrollMemoryForThisThread === 800) // not granted
    // Release
    memoryStore.releaseUnrollMemoryForThisThread(100)
    assert(memoryStore.currentUnrollMemoryForThisThread === 700)
    memoryStore.releaseUnrollMemoryForThisThread(100)
    assert(memoryStore.currentUnrollMemoryForThisThread === 600)
    // Reserve again
    memoryStore.reserveUnrollMemoryForThisThread(4400)
    assert(memoryStore.currentUnrollMemoryForThisThread === 5000)
    memoryStore.reserveUnrollMemoryForThisThread(20000)
    assert(memoryStore.currentUnrollMemoryForThisThread === 5000) // not granted
    // Release again
    memoryStore.releaseUnrollMemoryForThisThread(1000)
    assert(memoryStore.currentUnrollMemoryForThisThread === 4000)
    memoryStore.releaseUnrollMemoryForThisThread() // release all
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)
  }

  /**
   * Verify the result of MemoryStore#unrollSafely is as expected.
   */
  private def verifyUnroll(
      expected: Iterator[Any],
      result: Either[Array[Any], Iterator[Any]],
      shouldBeArray: Boolean): Unit = {
    val actual: Iterator[Any] = result match {
      case Left(arr: Array[Any]) =>
        assert(shouldBeArray, "expected iterator from unroll!")
        arr.iterator
      case Right(it: Iterator[Any]) =>
        assert(!shouldBeArray, "expected array from unroll!")
        it
      case _ =>
        fail("unroll returned neither an iterator nor an array...")
    }
    expected.zip(actual).foreach { case (e, a) =>
      assert(e === a, "unroll did not return original values!")
    }
  }

  test("safely unroll blocks") {
    store = makeBlockManager(12000)
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    val memoryStore = store.memoryStore
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Unroll with all the space in the world. This should succeed and return an array.
    var unrollResult = memoryStore.unrollSafely("unroll", smallList.iterator, droppedBlocks)
    verifyUnroll(smallList.iterator, unrollResult, shouldBeArray = true)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)
    memoryStore.releasePendingUnrollMemoryForThisThread()

    // Unroll with not enough space. This should succeed after kicking out someBlock1.
    store.putIterator("someBlock1", smallList.iterator, StorageLevel.MEMORY_ONLY)
    store.putIterator("someBlock2", smallList.iterator, StorageLevel.MEMORY_ONLY)
    unrollResult = memoryStore.unrollSafely("unroll", smallList.iterator, droppedBlocks)
    verifyUnroll(smallList.iterator, unrollResult, shouldBeArray = true)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)
    assert(droppedBlocks.size === 1)
    assert(droppedBlocks.head._1 === TestBlockId("someBlock1"))
    droppedBlocks.clear()
    memoryStore.releasePendingUnrollMemoryForThisThread()

    // Unroll huge block with not enough space. Even after ensuring free space of 12000 * 0.4 =
    // 4800 bytes, there is still not enough room to unroll this block. This returns an iterator.
    // In the mean time, however, we kicked out someBlock2 before giving up.
    store.putIterator("someBlock3", smallList.iterator, StorageLevel.MEMORY_ONLY)
    unrollResult = memoryStore.unrollSafely("unroll", bigList.iterator, droppedBlocks)
    verifyUnroll(bigList.iterator, unrollResult, shouldBeArray = false)
    assert(memoryStore.currentUnrollMemoryForThisThread > 0) // we returned an iterator
    assert(droppedBlocks.size === 1)
    assert(droppedBlocks.head._1 === TestBlockId("someBlock2"))
    droppedBlocks.clear()
  }

  test("safely unroll blocks through putIterator") {
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Unroll with plenty of space. This should succeed and cache both blocks.
    val result1 = memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    val result2 = memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(result1.size > 0) // unroll was successful
    assert(result2.size > 0)
    assert(result1.data.isLeft) // unroll did not drop this block to disk
    assert(result2.data.isLeft)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Re-put these two blocks so block manager knows about them too. Otherwise, block manager
    // would not know how to drop them from memory later.
    memoryStore.remove("b1")
    memoryStore.remove("b2")
    store.putIterator("b1", smallIterator, memOnly)
    store.putIterator("b2", smallIterator, memOnly)

    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    val result3 = memoryStore.putIterator("b3", smallIterator, memOnly, returnValues = true)
    assert(result3.size > 0)
    assert(result3.data.isLeft)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, memOnly)

    // Unroll huge block with not enough space. This should fail and kick out b2 in the process.
    val result4 = memoryStore.putIterator("b4", bigIterator, memOnly, returnValues = true)
    assert(result4.size === 0) // unroll was unsuccessful
    assert(result4.data.isLeft)
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
    assert(memoryStore.currentUnrollMemoryForThisThread > 0) // we returned an iterator
  }

  /**
   * This test is essentially identical to the preceding one, except that it uses MEMORY_AND_DISK.
   */
  test("safely unroll blocks through putIterator (disk)") {
    store = makeBlockManager(12000)
    val memAndDisk = StorageLevel.MEMORY_AND_DISK
    val memoryStore = store.memoryStore
    val diskStore = store.diskStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    store.putIterator("b1", smallIterator, memAndDisk)
    store.putIterator("b2", smallIterator, memAndDisk)

    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    // Memory store should contain b2 and b3, while disk store should contain only b1
    val result3 = memoryStore.putIterator("b3", smallIterator, memAndDisk, returnValues = true)
    assert(result3.size > 0)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(diskStore.contains("b1"))
    assert(!diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, StorageLevel.MEMORY_ONLY)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Unroll huge block with not enough space. This should fail and drop the new block to disk
    // directly in addition to kicking out b2 in the process. Memory store should contain only
    // b3, while disk store should contain b1, b2 and b4.
    val result4 = memoryStore.putIterator("b4", bigIterator, memAndDisk, returnValues = true)
    assert(result4.size > 0)
    assert(result4.data.isRight) // unroll returned bytes from disk
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
    assert(diskStore.contains("b1"))
    assert(diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    assert(diskStore.contains("b4"))
    assert(memoryStore.currentUnrollMemoryForThisThread > 0) // we returned an iterator
  }

  test("multiple unrolls by the same thread") {
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // All unroll memory used is released because unrollSafely returned an array
    memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)
    memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisThread === 0)

    // Unroll memory is not released because unrollSafely returned an iterator
    // that still depends on the underlying vector used in the process
    memoryStore.putIterator("b3", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB3 = memoryStore.currentUnrollMemoryForThisThread
    assert(unrollMemoryAfterB3 > 0)

    // The unroll memory owned by this thread builds on top of its value after the previous unrolls
    memoryStore.putIterator("b4", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB4 = memoryStore.currentUnrollMemoryForThisThread
    assert(unrollMemoryAfterB4 > unrollMemoryAfterB3)

    // ... but only to a certain extent (until we run out of free space to grant new unroll memory)
    memoryStore.putIterator("b5", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB5 = memoryStore.currentUnrollMemoryForThisThread
    memoryStore.putIterator("b6", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB6 = memoryStore.currentUnrollMemoryForThisThread
    memoryStore.putIterator("b7", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB7 = memoryStore.currentUnrollMemoryForThisThread
    assert(unrollMemoryAfterB5 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB6 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB7 === unrollMemoryAfterB4)
  }

  test("lazily create a big ByteBuffer to avoid OOM if it cannot be put into MemoryStore") {
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val blockId = BlockId("rdd_3_10")
    val result = memoryStore.putBytes(blockId, 13000, () => {
      fail("A big ByteBuffer that cannot be put into MemoryStore should not be created")
    })
    assert(result.size === 13000)
    assert(result.data === null)
    assert(result.droppedBlocks === Nil)
  }

  test("put a small ByteBuffer to MemoryStore") {
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val blockId = BlockId("rdd_3_10")
    var bytes: ByteBuffer = null
    val result = memoryStore.putBytes(blockId, 10000, () => {
      bytes = ByteBuffer.allocate(10000)
      bytes
    })
    assert(result.size === 10000)
    assert(result.data === Right(bytes))
    assert(result.droppedBlocks === Nil)
  }
}

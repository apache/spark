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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.commons.lang3.RandomUtils
import org.mockito.{ArgumentMatchers => mc}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest._
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.network.{BlockDataManager, BlockTransferService, TransportContext}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.server.{NoOpRpcHandler, TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, RegisterExecutor}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.security.{CryptoStreamUtils, EncryptionFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

class BlockManagerSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with LocalSparkContext with ResetSystemProperties
  with EncryptionFunSuite with TimeLimits {

  import BlockManagerSuite._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  var conf: SparkConf = null
  val allStores = ArrayBuffer[BlockManager]()
  var rpcEnv: RpcEnv = null
  var master: BlockManagerMaster = null
  val securityMgr = new SecurityManager(new SparkConf(false))
  val bcastManager = new BroadcastManager(true, new SparkConf(false), securityMgr)
  val mapOutputTracker = new MapOutputTrackerMaster(new SparkConf(false), bcastManager, true)
  val shuffleManager = new SortShuffleManager(new SparkConf(false))

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  val serializer = new KryoSerializer(
    new SparkConf(false).set(Kryo.KRYO_SERIALIZER_BUFFER_SIZE.key, "1m"))

  // Implicitly convert strings to BlockIds for test clarity.
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int): RDDBlockId = RDDBlockId(rddId, splitId)

  private def init(sparkConf: SparkConf): Unit = {
    sparkConf
      .set("spark.app.id", "test")
      .set(IS_TESTING, true)
      .set(MEMORY_FRACTION, 1.0)
      .set(MEMORY_STORAGE_FRACTION, 0.999)
      .set(Kryo.KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
      .set(STORAGE_UNROLL_MEMORY_THRESHOLD, 512L)
  }

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER,
      master: BlockManagerMaster = this.master,
      transferService: Option[BlockTransferService] = Option.empty,
      testConf: Option[SparkConf] = None): BlockManager = {
    val bmConf = testConf.map(_.setAll(conf.getAll)).getOrElse(conf)
    bmConf.set(TEST_MEMORY, maxMem)
    bmConf.set(MEMORY_OFFHEAP_SIZE, maxMem)
    val serializer = new KryoSerializer(bmConf)
    val encryptionKey = if (bmConf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(bmConf))
    } else {
      None
    }
    val bmSecurityMgr = new SecurityManager(bmConf, encryptionKey)
    val transfer = transferService
      .getOrElse(new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1))
    val memManager = UnifiedMemoryManager(bmConf, numCores = 1)
    val serializerManager = new SerializerManager(serializer, bmConf)
    val blockManager = new BlockManager(name, rpcEnv, master, serializerManager, bmConf,
      memManager, mapOutputTracker, shuffleManager, transfer, bmSecurityMgr, 0)
    memManager.setMemoryStore(blockManager.memoryStore)
    allStores += blockManager
    blockManager.initialize("app-id")
    blockManager
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf = new SparkConf(false)
    init(conf)

    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)
    conf.set(DRIVER_PORT, rpcEnv.address.port)

    // Mock SparkContext to reduce the memory usage of tests. It's fine since the only reason we
    // need to create a SparkContext is to initialize LiveListenerBus.
    sc = mock(classOf[SparkContext])
    when(sc.conf).thenReturn(conf)
    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf,
        new LiveListenerBus(conf))), conf, true)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  override def afterEach(): Unit = {
    try {
      conf = null
      allStores.foreach(_.stop())
      allStores.clear()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
      rpcEnv = null
      master = null
    } finally {
      super.afterEach()
    }
  }

  private def stopBlockManager(blockManager: BlockManager): Unit = {
    allStores -= blockManager
    blockManager.stop()
  }

  test("StorageLevel object caching") {
    val level1 = StorageLevel(false, false, false, 3)
    // this should return the same object as level1
    val level2 = StorageLevel(false, false, false, 3)
    // this should return a different object
    val level3 = StorageLevel(false, false, false, 2)
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

  test("BlockManagerId.isDriver() with DRIVER_IDENTIFIER (SPARK-27090)") {
    assert(BlockManagerId(SparkContext.DRIVER_IDENTIFIER, "XXX", 1).isDriver)
    assert(!BlockManagerId("notADriverIdentifier", "XXX", 1).isDriver)
  }

  test("master + 1 manager interaction") {
    val store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory
    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was not in store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1").size > 0, "master was not told about a1")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
    assert(master.getLocations("a3").size === 0, "master was told about a3")

    // Drop a1 and a2 from memory; this should be reported back to the master
    store.dropFromMemoryIfExists("a1", () => null: Either[Array[Any], ChunkedByteBuffer])
    store.dropFromMemoryIfExists("a2", () => null: Either[Array[Any], ChunkedByteBuffer])
    assert(store.getSingleAndReleaseLock("a1") === None, "a1 not removed from store")
    assert(store.getSingleAndReleaseLock("a2") === None, "a2 not removed from store")
    assert(master.getLocations("a1").size === 0, "master did not remove a1")
    assert(master.getLocations("a2").size === 0, "master did not remove a2")
  }

  test("master + 2 managers interaction") {
    val store = makeBlockManager(2000, "exec1")
    val store2 = makeBlockManager(2000, "exec2")

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
    val store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2 and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1-to-remove", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2-to-remove", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3-to-remove", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory and memory size
    val memStatus = master.getMemoryStatus.head._2
    assert(memStatus._1 == 40000L, "total memory " + memStatus._1 + " should equal 40000")
    assert(memStatus._2 <= 32000L, "remaining memory " + memStatus._2 + " should <= 12000")
    assert(store.getSingleAndReleaseLock("a1-to-remove").isDefined, "a1 was not in store")
    assert(store.getSingleAndReleaseLock("a2-to-remove").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3-to-remove").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1-to-remove").size > 0, "master was not told about a1")
    assert(master.getLocations("a2-to-remove").size > 0, "master was not told about a2")
    assert(master.getLocations("a3-to-remove").size === 0, "master was told about a3")

    // Remove a1 and a2 and a3. Should be no-op for a3.
    master.removeBlock("a1-to-remove")
    master.removeBlock("a2-to-remove")
    master.removeBlock("a3-to-remove")

    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!store.hasLocalBlock("a1-to-remove"))
      master.getLocations("a1-to-remove") should have size 0
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!store.hasLocalBlock("a2-to-remove"))
      master.getLocations("a2-to-remove") should have size 0
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(store.hasLocalBlock("a3-to-remove"))
      master.getLocations("a3-to-remove") should have size 0
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      val memStatus = master.getMemoryStatus.head._2
      memStatus._1 should equal (40000L)
      memStatus._2 should equal (40000L)
    }
  }

  test("removing rdd") {
    val store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    // Putting a1, a2 and a3 in memory.
    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("nonrddblock", a3, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = false)

    eventually(timeout(1.second), interval(10.milliseconds)) {
      store.getSingleAndReleaseLock(rdd(0, 0)) should be (None)
      master.getLocations(rdd(0, 0)) should have size 0
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      store.getSingleAndReleaseLock(rdd(0, 1)) should be (None)
      master.getLocations(rdd(0, 1)) should have size 0
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      store.getSingleAndReleaseLock("nonrddblock") should not be (None)
      master.getLocations("nonrddblock") should have size (1)
    }

    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = true)
    store.getSingleAndReleaseLock(rdd(0, 0)) should be (None)
    master.getLocations(rdd(0, 0)) should have size 0
    store.getSingleAndReleaseLock(rdd(0, 1)) should be (None)
    master.getLocations(rdd(0, 1)) should have size 0
  }

  test("removing broadcast") {
    val store = makeBlockManager(2000)
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
      assert(s.hasLocalBlock(broadcast0BlockId))
      assert(s.hasLocalBlock(broadcast1BlockId))
      assert(s.hasLocalBlock(broadcast2BlockId))
      assert(s.hasLocalBlock(broadcast2BlockId2))
    }

    // remove broadcast 0 block only from executors
    master.removeBroadcast(0, removeFromMaster = false, blocking = true)

    // only broadcast 0 block should be removed from the executor store
    assert(!executorStore.hasLocalBlock(broadcast0BlockId))
    assert(executorStore.hasLocalBlock(broadcast1BlockId))
    assert(executorStore.hasLocalBlock(broadcast2BlockId))

    // nothing should be removed from the driver store
    assert(driverStore.hasLocalBlock(broadcast0BlockId))
    assert(driverStore.hasLocalBlock(broadcast1BlockId))
    assert(driverStore.hasLocalBlock(broadcast2BlockId))

    // remove broadcast 0 block from the driver as well
    master.removeBroadcast(0, removeFromMaster = true, blocking = true)
    assert(!driverStore.hasLocalBlock(broadcast0BlockId))
    assert(driverStore.hasLocalBlock(broadcast1BlockId))

    // remove broadcast 1 block from both the stores asynchronously
    // and verify all broadcast 1 blocks have been removed
    master.removeBroadcast(1, removeFromMaster = true, blocking = false)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!driverStore.hasLocalBlock(broadcast1BlockId))
      assert(!executorStore.hasLocalBlock(broadcast1BlockId))
    }

    // remove broadcast 2 from both the stores asynchronously
    // and verify all broadcast 2 blocks have been removed
    master.removeBroadcast(2, removeFromMaster = true, blocking = false)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!driverStore.hasLocalBlock(broadcast2BlockId))
      assert(!driverStore.hasLocalBlock(broadcast2BlockId2))
      assert(!executorStore.hasLocalBlock(broadcast2BlockId))
      assert(!executorStore.hasLocalBlock(broadcast2BlockId2))
    }
    executorStore.stop()
    driverStore.stop()
  }

  test("reregistration on heart beat") {
    val store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    val reregister = !master.driverEndpoint.askSync[Boolean](
      BlockManagerHeartbeat(store.blockManagerId))
    assert(reregister)
  }

  test("reregistration on block update") {
    val store = makeBlockManager(2000)
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
    val store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = List(new Array[Byte](400))

    // try many times to trigger any deadlocks
    for (i <- 1 to 100) {
      master.removeExecutor(store.blockManagerId.executorId)
      val t1 = new Thread {
        override def run() {
          store.putIterator(
            "a2", a2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
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

      store.dropFromMemoryIfExists("a1", () => null: Either[Array[Any], ChunkedByteBuffer])
      store.dropFromMemoryIfExists("a2", () => null: Either[Array[Any], ChunkedByteBuffer])
      store.waitForAsyncReregister()
    }
  }

  test("correct BlockResult returned from get() calls") {
    val store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](500), new Array[Byte](1000), new Array[Byte](1500))
    val list1SizeEstimate = SizeEstimator.estimate(list1.iterator.toArray)
    val list2SizeEstimate = SizeEstimator.estimate(list2.iterator.toArray)
    store.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator(
      "list2memory", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator(
      "list2disk", list2.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val list1Get = store.get("list1")
    assert(list1Get.isDefined, "list1 expected to be in store")
    assert(list1Get.get.data.size === 2)
    assert(list1Get.get.bytes === list1SizeEstimate)
    assert(list1Get.get.readMethod === DataReadMethod.Memory)
    val list2MemoryGet = store.get("list2memory")
    assert(list2MemoryGet.isDefined, "list2memory expected to be in store")
    assert(list2MemoryGet.get.data.size === 3)
    assert(list2MemoryGet.get.bytes === list2SizeEstimate)
    assert(list2MemoryGet.get.readMethod === DataReadMethod.Memory)
    val list2DiskGet = store.get("list2disk")
    assert(list2DiskGet.isDefined, "list2memory expected to be in store")
    assert(list2DiskGet.get.data.size === 3)
    // We don't know the exact size of the data on disk, but it should certainly be > 0.
    assert(list2DiskGet.get.bytes > 0)
    assert(list2DiskGet.get.readMethod === DataReadMethod.Disk)
  }

  test("optimize a location order of blocks without topology information") {
    val localHost = "localhost"
    val otherHost = "otherHost"
    val bmMaster = mock(classOf[BlockManagerMaster])
    val bmId1 = BlockManagerId("id1", localHost, 1)
    val bmId2 = BlockManagerId("id2", localHost, 2)
    val bmId3 = BlockManagerId("id3", otherHost, 3)
    when(bmMaster.getLocations(mc.any[BlockId])).thenReturn(Seq(bmId1, bmId2, bmId3))

    val blockManager = makeBlockManager(128, "exec", bmMaster)
    val sortLocations = PrivateMethod[Seq[BlockManagerId]]('sortLocations)
    val locations = blockManager invokePrivate sortLocations(bmMaster.getLocations("test"))
    assert(locations.map(_.host) === Seq(localHost, localHost, otherHost))
  }

  test("optimize a location order of blocks with topology information") {
    val localHost = "localhost"
    val otherHost = "otherHost"
    val localRack = "localRack"
    val otherRack = "otherRack"

    val bmMaster = mock(classOf[BlockManagerMaster])
    val bmId1 = BlockManagerId("id1", localHost, 1, Some(localRack))
    val bmId2 = BlockManagerId("id2", localHost, 2, Some(localRack))
    val bmId3 = BlockManagerId("id3", otherHost, 3, Some(otherRack))
    val bmId4 = BlockManagerId("id4", otherHost, 4, Some(otherRack))
    val bmId5 = BlockManagerId("id5", otherHost, 5, Some(localRack))
    when(bmMaster.getLocations(mc.any[BlockId]))
      .thenReturn(Seq(bmId1, bmId2, bmId5, bmId3, bmId4))

    val blockManager = makeBlockManager(128, "exec", bmMaster)
    blockManager.blockManagerId =
      BlockManagerId(SparkContext.DRIVER_IDENTIFIER, localHost, 1, Some(localRack))
    val sortLocations = PrivateMethod[Seq[BlockManagerId]]('sortLocations)
    val locations = blockManager invokePrivate sortLocations(bmMaster.getLocations("test"))
    assert(locations.map(_.host) === Seq(localHost, localHost, otherHost, otherHost, otherHost))
    assert(locations.flatMap(_.topologyInfo)
      === Seq(localRack, localRack, localRack, otherRack, otherRack))
  }

  test("SPARK-9591: getRemoteBytes from another location when Exception throw") {
    conf.set("spark.shuffle.io.maxRetries", "0")
    val store = makeBlockManager(8000, "executor1")
    val store2 = makeBlockManager(8000, "executor2")
    val store3 = makeBlockManager(8000, "executor3")
    val list1 = List(new Array[Byte](4000))
    store2.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store3.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getRemoteBytes("list1").isDefined, "list1Get expected to be fetched")
    stopBlockManager(store2)
    assert(store.getRemoteBytes("list1").isDefined, "list1Get expected to be fetched")
    stopBlockManager(store3)
    // Should return None instead of throwing an exception:
    assert(store.getRemoteBytes("list1").isEmpty)
  }

  test("SPARK-14252: getOrElseUpdate should still read from remote storage") {
    val store = makeBlockManager(8000, "executor1")
    val store2 = makeBlockManager(8000, "executor2")
    val list1 = List(new Array[Byte](4000))
    store2.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getOrElseUpdate(
      "list1",
      StorageLevel.MEMORY_ONLY,
      ClassTag.Any,
      () => fail("attempted to compute locally")).isLeft)
  }

  test("in-memory LRU storage") {
    testInMemoryLRUStorage(StorageLevel.MEMORY_ONLY)
  }

  test("in-memory LRU storage with serialization") {
    testInMemoryLRUStorage(StorageLevel.MEMORY_ONLY_SER)
  }

  test("in-memory LRU storage with off-heap") {
    testInMemoryLRUStorage(StorageLevel(
      useDisk = false,
      useMemory = true,
      useOffHeap = true,
      deserialized = false, replication = 1))
  }

  private def testInMemoryLRUStorage(storageLevel: StorageLevel): Unit = {
    val store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, storageLevel)
    store.putSingle("a2", a2, storageLevel)
    store.putSingle("a3", a3, storageLevel)
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3").isDefined, "a3 was not in store")
    assert(store.getSingleAndReleaseLock("a1") === None, "a1 was in store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, storageLevel)
    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was not in store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3") === None, "a3 was in store")
  }

  test("in-memory LRU for partitions of same RDD") {
    val store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle(rdd(0, 1), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 3), a3, StorageLevel.MEMORY_ONLY)
    // Even though we accessed rdd_0_3 last, it should not have replaced partitions 1 and 2
    // from the same RDD
    assert(store.getSingleAndReleaseLock(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingleAndReleaseLock(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
    assert(store.getSingleAndReleaseLock(rdd(0, 1)).isDefined, "rdd_0_1 was not in store")
    // Check that rdd_0_3 doesn't replace them even after further accesses
    assert(store.getSingleAndReleaseLock(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingleAndReleaseLock(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingleAndReleaseLock(rdd(0, 3)) === None, "rdd_0_3 was in store")
  }

  test("in-memory LRU for partitions of multiple RDDs") {
    val store = makeBlockManager(12000)
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // At this point rdd_1_1 should've replaced rdd_0_1
    assert(store.memoryStore.contains(rdd(1, 1)), "rdd_1_1 was not in store")
    assert(!store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was in store")
    assert(store.memoryStore.contains(rdd(0, 2)), "rdd_0_2 was not in store")
    // Do a get() on rdd_0_2 so that it is the most recently used item
    assert(store.getSingleAndReleaseLock(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
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

  encryptionTest("on-disk storage") { _conf =>
    val store = makeBlockManager(1200, testConf = Some(_conf))
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    store.putSingle("a2", a2, StorageLevel.DISK_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was in store")
    assert(store.getSingleAndReleaseLock("a3").isDefined, "a3 was in store")
    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was in store")
  }

  encryptionTest("disk and memory storage") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.MEMORY_AND_DISK, getAsBytes = false, testConf = conf)
  }

  encryptionTest("disk and memory storage with getLocalBytes") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.MEMORY_AND_DISK, getAsBytes = true, testConf = conf)
  }

  encryptionTest("disk and memory storage with serialization") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.MEMORY_AND_DISK_SER, getAsBytes = false, testConf = conf)
  }

  encryptionTest("disk and memory storage with serialization and getLocalBytes") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.MEMORY_AND_DISK_SER, getAsBytes = true, testConf = conf)
  }

  encryptionTest("disk and off-heap memory storage") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.OFF_HEAP, getAsBytes = false, testConf = conf)
  }

  encryptionTest("disk and off-heap memory storage with getLocalBytes") { _conf =>
    testDiskAndMemoryStorage(StorageLevel.OFF_HEAP, getAsBytes = true, testConf = conf)
  }

  def testDiskAndMemoryStorage(
      storageLevel: StorageLevel,
      getAsBytes: Boolean,
      testConf: SparkConf): Unit = {
    val store = makeBlockManager(12000, testConf = Some(testConf))
    val accessMethod =
      if (getAsBytes) store.getLocalBytesAndReleaseLock else store.getSingleAndReleaseLock
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, storageLevel)
    store.putSingle("a2", a2, storageLevel)
    store.putSingle("a3", a3, storageLevel)
    assert(accessMethod("a2").isDefined, "a2 was not in store")
    assert(accessMethod("a3").isDefined, "a3 was not in store")
    assert(accessMethod("a1").isDefined, "a1 was not in store")
    val dataShouldHaveBeenCachedBackIntoMemory = {
      if (storageLevel.deserialized) {
        !getAsBytes
      } else {
        // If the block's storage level is serialized, then always cache the bytes in memory, even
        // if the caller requested values.
        true
      }
    }
    if (dataShouldHaveBeenCachedBackIntoMemory) {
      assert(store.memoryStore.contains("a1"), "a1 was not in memory store")
    } else {
      assert(!store.memoryStore.contains("a1"), "a1 was in memory store")
    }
  }

  encryptionTest("LRU with mixed storage levels") { _conf =>
    val store = makeBlockManager(12000, testConf = Some(_conf))
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    val a4 = new Array[Byte](4000)
    // First store a1 and a2, both in memory, and a3, on disk only
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    // At this point LRU should not kick in because a3 is only on disk
    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was not in store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3").isDefined, "a3 was not in store")
    // Now let's add in a4, which uses both disk and memory; a1 should drop out
    store.putSingle("a4", a4, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingleAndReleaseLock("a1") == None, "a1 was in store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
    assert(store.getSingleAndReleaseLock("a3").isDefined, "a3 was not in store")
    assert(store.getSingleAndReleaseLock("a4").isDefined, "a4 was not in store")
  }

  encryptionTest("in-memory LRU with streams") { _conf =>
    val store = makeBlockManager(12000, testConf = Some(_conf))
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    store.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator(
      "list2", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator(
      "list3", list3.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.getAndReleaseLock("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.getAndReleaseLock("list1") === None, "list1 was in store")
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    // At this point list2 was gotten last, so LRU will getSingle rid of list3
    store.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getAndReleaseLock("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.getAndReleaseLock("list3") === None, "list1 was in store")
  }

  encryptionTest("LRU with mixed storage levels and streams") { _conf =>
    val store = makeBlockManager(12000, testConf = Some(_conf))
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list4 = List(new Array[Byte](2000), new Array[Byte](2000))
    // First store list1 and list2, both in memory, and list3, on disk only
    store.putIterator(
      "list1", list1.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator(
      "list2", list2.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator(
      "list3", list3.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val listForSizeEstimate = new ArrayBuffer[Any]
    listForSizeEstimate ++= list1.iterator
    val listSize = SizeEstimator.estimate(listForSizeEstimate)
    // At this point LRU should not kick in because list3 is only on disk
    assert(store.getAndReleaseLock("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.getAndReleaseLock("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.getAndReleaseLock("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.getAndReleaseLock("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    // Now let's add in list4, which uses both disk and memory; list1 should drop out
    store.putIterator(
      "list4", list4.iterator, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)
    assert(store.getAndReleaseLock("list1") === None, "list1 was in store")
    assert(store.getAndReleaseLock("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.getAndReleaseLock("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.getAndReleaseLock("list4").isDefined, "list4 was not in store")
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
    val store = makeBlockManager(5000)
    store.putSingle("a1", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
    assert(store.getSingleAndReleaseLock("a1") === None, "a1 was in store")
    store.putSingle("a2", new Array[Byte](10000), StorageLevel.MEMORY_AND_DISK)
    assert(!store.memoryStore.contains("a2"), "a2 was in memory store")
    assert(store.getSingleAndReleaseLock("a2").isDefined, "a2 was not in store")
  }

  test("block compression") {
    try {
      conf.set(SHUFFLE_COMPRESS, true)
      var store = makeBlockManager(20000, "exec1")
      store.putSingle(
        ShuffleBlockId(0, 0, 0), new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) <= 100,
        "shuffle_0_0_0 was not compressed")
      stopBlockManager(store)

      conf.set(SHUFFLE_COMPRESS, false)
      store = makeBlockManager(20000, "exec2")
      store.putSingle(
        ShuffleBlockId(0, 0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) >= 10000,
        "shuffle_0_0_0 was compressed")
      stopBlockManager(store)

      conf.set(BROADCAST_COMPRESS, true)
      store = makeBlockManager(20000, "exec3")
      store.putSingle(
        BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) <= 1000,
        "broadcast_0 was not compressed")
      stopBlockManager(store)

      conf.set(BROADCAST_COMPRESS, false)
      store = makeBlockManager(20000, "exec4")
      store.putSingle(
        BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) >= 10000, "broadcast_0 was compressed")
      stopBlockManager(store)

      conf.set(RDD_COMPRESS, true)
      store = makeBlockManager(20000, "exec5")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) <= 1000, "rdd_0_0 was not compressed")
      stopBlockManager(store)

      conf.set(RDD_COMPRESS, false)
      store = makeBlockManager(20000, "exec6")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) >= 10000, "rdd_0_0 was compressed")
      stopBlockManager(store)

      // Check that any other block types are also kept uncompressed
      store = makeBlockManager(20000, "exec7")
      store.putSingle("other_block", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
      assert(store.memoryStore.getSize("other_block") >= 10000, "other_block was compressed")
      stopBlockManager(store)
    } finally {
      System.clearProperty(SHUFFLE_COMPRESS.key)
      System.clearProperty(BROADCAST_COMPRESS.key)
      System.clearProperty(RDD_COMPRESS.key)
    }
  }

  test("block store put failure") {
    // Use Java serializer so we can create an unserializable error.
    conf.set(TEST_MEMORY, 1200L)
    val transfer = new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1)
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      serializerManager, conf, memoryManager, mapOutputTracker,
      shuffleManager, transfer, securityMgr, 0)
    allStores += store
    store.initialize("app-id")

    // The put should fail since a1 is not serializable.
    class UnserializableClass
    val a1 = new UnserializableClass
    intercept[java.io.NotSerializableException] {
      store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    }

    // Make sure get a1 doesn't hang and returns None.
    failAfter(1.second) {
      assert(store.getSingleAndReleaseLock("a1").isEmpty, "a1 should not be in store")
    }
  }

  def testPutBlockDataAsStream(blockManager: BlockManager, storageLevel: StorageLevel): Unit = {
    val message = "message"
    val ser = serializer.newInstance().serialize(message).array()
    val blockId = new RDDBlockId(0, 0)
    val streamCallbackWithId =
      blockManager.putBlockDataAsStream(blockId, storageLevel, ClassTag(message.getClass))
    streamCallbackWithId.onData("0", ByteBuffer.wrap(ser))
    streamCallbackWithId.onComplete("0")
    val blockStatusOption = blockManager.getStatus(blockId)
    assert(!blockStatusOption.isEmpty)
    val blockStatus = blockStatusOption.get
    assert((blockStatus.diskSize > 0) === !storageLevel.useMemory)
    assert((blockStatus.memSize > 0) === storageLevel.useMemory)
    assert(blockManager.getBlockData(blockId).nioByteBuffer().array() === ser)
  }

  Seq(
    "caching" -> StorageLevel.MEMORY_ONLY,
    "caching, serialized" -> StorageLevel.MEMORY_ONLY_SER,
    "caching on disk" -> StorageLevel.DISK_ONLY
  ).foreach { case (name, storageLevel) =>
    encryptionTest(s"test putBlockDataAsStream with $name") { conf =>
      init(conf)
      val ioEncryptionKey =
        if (conf.get(IO_ENCRYPTION_ENABLED)) Some(CryptoStreamUtils.createKey(conf)) else None
      val securityMgr = new SecurityManager(conf, ioEncryptionKey)
      val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
      val transfer =
        new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1)
      val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
      val blockManager = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
        serializerManager, conf, memoryManager, mapOutputTracker,
        shuffleManager, transfer, securityMgr, 0)
      try {
        blockManager.initialize("app-id")
        testPutBlockDataAsStream(blockManager, storageLevel)
      } finally {
        blockManager.stop()
      }
    }
  }

  test("turn off updated block statuses") {
    val conf = new SparkConf()
    conf.set(TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES, false)
    val store = makeBlockManager(12000, testConf = Some(conf))

    store.registerTask(0)
    val list = List.fill(2)(new Array[Byte](2000))

    def getUpdatedBlocks(task: => Unit): Seq[(BlockId, BlockStatus)] = {
      val context = TaskContext.empty()
      try {
        TaskContext.setTaskContext(context)
        task
      } finally {
        TaskContext.unset()
      }
      context.taskMetrics.updatedBlockStatuses
    }

    // 1 updated block (i.e. list1)
    val updatedBlocks1 = getUpdatedBlocks {
      store.putIterator(
        "list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    assert(updatedBlocks1.size === 0)
  }


  test("updated block statuses") {
    val conf = new SparkConf()
    conf.set(TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES, true)
    val store = makeBlockManager(12000, testConf = Some(conf))
    store.registerTask(0)
    val list = List.fill(2)(new Array[Byte](2000))
    val bigList = List.fill(8)(new Array[Byte](2000))

    def getUpdatedBlocks(task: => Unit): Seq[(BlockId, BlockStatus)] = {
      val context = TaskContext.empty()
      try {
        TaskContext.setTaskContext(context)
        task
      } finally {
        TaskContext.unset()
      }
      context.taskMetrics.updatedBlockStatuses
    }

    // 1 updated block (i.e. list1)
    val updatedBlocks1 = getUpdatedBlocks {
      store.putIterator(
        "list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    assert(updatedBlocks1.size === 1)
    assert(updatedBlocks1.head._1 === TestBlockId("list1"))
    assert(updatedBlocks1.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 1 updated block (i.e. list2)
    val updatedBlocks2 = getUpdatedBlocks {
      store.putIterator(
        "list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    }
    assert(updatedBlocks2.size === 1)
    assert(updatedBlocks2.head._1 === TestBlockId("list2"))
    assert(updatedBlocks2.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 2 updated blocks - list1 is kicked out of memory while list3 is added
    val updatedBlocks3 = getUpdatedBlocks {
      store.putIterator(
        "list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
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
    val updatedBlocks4 = getUpdatedBlocks {
      store.putIterator(
        "list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
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
    val updatedBlocks5 = getUpdatedBlocks {
      store.putIterator(
        "list5", bigList.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
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

    // remove block - list2 should be removed from disk
    val updatedBlocks6 = getUpdatedBlocks {
      store.removeBlock(
        "list2", tellMaster = true)
    }
    assert(updatedBlocks6.size === 1)
    assert(updatedBlocks6.head._1 === TestBlockId("list2"))
    assert(updatedBlocks6.head._2.storageLevel == StorageLevel.NONE)
    assert(!store.diskStore.contains("list2"), "list2 was in disk store")
  }

  test("query block statuses") {
    val store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))

    // Tell master. By LRU, only list2 and list3 remains.
    store.putIterator(
      "list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator(
      "list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator(
      "list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

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
    store.putIterator(
      "list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.putIterator(
      "list5", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator(
      "list6", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

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
    val store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](100))

    // insert some blocks
    store.putIterator(
      "list1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator(
      "list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator(
      "list3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("list"), askSlaves = false).size
      === 3)
    assert(store.master.getMatchingBlockIds(_.toString.contains("list1"), askSlaves = false).size
      === 1)

    // insert some more blocks
    store.putIterator(
      "newlist1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator(
      "newlist2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator(
      "newlist3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)

    // getLocations and getBlockStatus should yield the same locations
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = false).size
      === 1)
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = true).size
      === 3)

    val blockIds = Seq(RDDBlockId(1, 0), RDDBlockId(1, 1), RDDBlockId(2, 0))
    blockIds.foreach { blockId =>
      store.putIterator(
        blockId, list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    val matchedBlockIds = store.master.getMatchingBlockIds(_ match {
      case RDDBlockId(1, _) => true
      case _ => false
    }, askSlaves = true)
    assert(matchedBlockIds.toSet === Set(RDDBlockId(1, 0), RDDBlockId(1, 1)))
  }

  test("SPARK-1194 regression: fix the same-RDD rule for cache replacement") {
    val store = makeBlockManager(12000)
    store.putSingle(rdd(0, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // Access rdd_1_0 to ensure it's not least recently used.
    assert(store.getSingleAndReleaseLock(rdd(1, 0)).isDefined, "rdd_1_0 was not in store")
    // According to the same-RDD rule, rdd_1_0 should be replaced here.
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // rdd_1_0 should have been replaced, even it's not least recently used.
    assert(store.memoryStore.contains(rdd(0, 0)), "rdd_0_0 was not in store")
    assert(store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was not in store")
    assert(!store.memoryStore.contains(rdd(1, 0)), "rdd_1_0 was in store")
  }

  test("safely unroll blocks through putIterator (disk)") {
    val store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val diskStore = store.diskStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    store.putIterator("b1", smallIterator, StorageLevel.MEMORY_AND_DISK)
    store.putIterator("b2", smallIterator, StorageLevel.MEMORY_AND_DISK)

    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    // Memory store should contain b2 and b3, while disk store should contain only b1
    val result3 = memoryStore.putIteratorAsValues("b3", smallIterator, ClassTag.Any)
    assert(result3.isRight)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(diskStore.contains("b1"))
    assert(!diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, StorageLevel.MEMORY_ONLY)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll huge block with not enough space. This should fail and return an iterator so that
    // the block may be stored to disk. During the unrolling process, block "b2" should be kicked
    // out, so the memory store should contain only b3, while the disk store should contain
    // b1, b2 and b4.
    val result4 = memoryStore.putIteratorAsValues("b4", bigIterator, ClassTag.Any)
    assert(result4.isLeft)
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
  }

  test("read-locked blocks cannot be evicted from memory") {
    val store = makeBlockManager(12000)
    val arr = new Array[Byte](4000)
    // First store a1 and a2, both in memory, and a3, on disk only
    store.putSingle("a1", arr, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", arr, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // This put should fail because both a1 and a2 should be read-locked:
    store.putSingle("a3", arr, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a3").isEmpty, "a3 was in store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // Release both pins of block a2:
    store.releaseLock("a2")
    store.releaseLock("a2")
    // Block a1 is the least-recently accessed, so an LRU eviction policy would evict it before
    // block a2. However, a1 is still pinned so this put of a3 should evict a2 instead:
    store.putSingle("a3", arr, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a2").isEmpty, "a2 was in store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
  }

  private def testReadWithLossOfOnDiskFiles(
      storageLevel: StorageLevel,
      readMethod: BlockManager => Option[_]): Unit = {
    val store = makeBlockManager(12000)
    assert(store.putSingle("blockId", new Array[Byte](4000), storageLevel))
    assert(store.getStatus("blockId").isDefined)
    // Directly delete all files from the disk store, triggering failures when reading blocks:
    store.diskBlockManager.getAllFiles().foreach(_.delete())
    // The BlockManager still thinks that these blocks exist:
    assert(store.getStatus("blockId").isDefined)
    // Because the BlockManager's metadata claims that the block exists (i.e. that it's present
    // in at least one store), the read attempts to read it and fails when the on-disk file is
    // missing.
    intercept[SparkException] {
      readMethod(store)
    }
    // Subsequent read attempts will succeed; the block isn't present but we return an expected
    // "block not found" response rather than a fatal error:
    assert(readMethod(store).isEmpty)
    // The reason why this second read succeeded is because the metadata entry for the missing
    // block was removed as a result of the read failure:
    assert(store.getStatus("blockId").isEmpty)
  }

  test("remove block if a read fails due to missing DiskStore files (SPARK-15736)") {
    val storageLevels = Seq(
      StorageLevel(useDisk = true, useMemory = false, deserialized = false),
      StorageLevel(useDisk = true, useMemory = false, deserialized = true))
    val readMethods = Map[String, BlockManager => Option[_]](
      "getLocalBytes" -> ((m: BlockManager) => m.getLocalBytes("blockId")),
      "getLocalValues" -> ((m: BlockManager) => m.getLocalValues("blockId"))
    )
    testReadWithLossOfOnDiskFiles(StorageLevel.DISK_ONLY, _.getLocalBytes("blockId"))
    for ((readMethodName, readMethod) <- readMethods; storageLevel <- storageLevels) {
      withClue(s"$readMethodName $storageLevel") {
        testReadWithLossOfOnDiskFiles(storageLevel, readMethod)
      }
    }
  }

  test("SPARK-13328: refresh block locations (fetch should fail after hitting a threshold)") {
    val mockBlockTransferService =
      new MockBlockTransferService(conf.get(BLOCK_FAILURES_BEFORE_LOCATION_REFRESH))
    val store =
      makeBlockManager(8000, "executor1", transferService = Option(mockBlockTransferService))
    store.putSingle("item", 999L, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getRemoteBytes("item").isEmpty)
  }

  test("SPARK-13328: refresh block locations (fetch should succeed after location refresh)") {
    val maxFailuresBeforeLocationRefresh =
      conf.get(BLOCK_FAILURES_BEFORE_LOCATION_REFRESH)
    val mockBlockManagerMaster = mock(classOf[BlockManagerMaster])
    val mockBlockTransferService =
      new MockBlockTransferService(maxFailuresBeforeLocationRefresh)
    // make sure we have more than maxFailuresBeforeLocationRefresh locations
    // so that we have a chance to do location refresh
    val blockManagerIds = (0 to maxFailuresBeforeLocationRefresh)
      .map { i => BlockManagerId(s"id-$i", s"host-$i", i + 1) }
    when(mockBlockManagerMaster.getLocationsAndStatus(mc.any[BlockId])).thenReturn(
      Option(BlockLocationsAndStatus(blockManagerIds, BlockStatus.empty)))
    when(mockBlockManagerMaster.getLocations(mc.any[BlockId])).thenReturn(
      blockManagerIds)

    val store = makeBlockManager(8000, "executor1", mockBlockManagerMaster,
      transferService = Option(mockBlockTransferService))
    val block = store.getRemoteBytes("item")
      .asInstanceOf[Option[ByteBuffer]]
    assert(block.isDefined)
    verify(mockBlockManagerMaster, times(1)).getLocationsAndStatus("item")
    verify(mockBlockManagerMaster, times(1)).getLocations("item")
  }

  test("SPARK-17484: block status is properly updated following an exception in put()") {
    val mockBlockTransferService = new MockBlockTransferService(maxFailures = 10) {
      override def uploadBlock(
          hostname: String,
          port: Int, execId: String,
          blockId: BlockId,
          blockData: ManagedBuffer,
          level: StorageLevel,
          classTag: ClassTag[_]): Future[Unit] = {
        throw new InterruptedException("Intentional interrupt")
      }
    }
    val store =
      makeBlockManager(8000, "executor1", transferService = Option(mockBlockTransferService))
    val store2 =
      makeBlockManager(8000, "executor2", transferService = Option(mockBlockTransferService))
    intercept[InterruptedException] {
      store.putSingle("item", "value", StorageLevel.MEMORY_ONLY_2, tellMaster = true)
    }
    assert(store.getLocalBytes("item").isEmpty)
    assert(master.getLocations("item").isEmpty)
    assert(store2.getRemoteBytes("item").isEmpty)
  }

  test("SPARK-17484: master block locations are updated following an invalid remote block fetch") {
    val store = makeBlockManager(8000, "executor1")
    val store2 = makeBlockManager(8000, "executor2")
    store.putSingle("item", "value", StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(master.getLocations("item").nonEmpty)
    store.removeBlock("item", tellMaster = false)
    assert(master.getLocations("item").nonEmpty)
    assert(store2.getRemoteBytes("item").isEmpty)
    assert(master.getLocations("item").isEmpty)
  }

  test("SPARK-20640: Shuffle registration timeout and maxAttempts conf are working") {
    val tryAgainMsg = "test_spark_20640_try_again"
    val timingoutExecutor = "timingoutExecutor"
    val tryAgainExecutor = "tryAgainExecutor"
    val succeedingExecutor = "succeedingExecutor"

    val failure = new Exception(tryAgainMsg)
    val success = ByteBuffer.wrap(new Array[Byte](0))

    var secondExecutorFailedOnce = false
    var thirdExecutorFailedOnce = false

    val handler = new NoOpRpcHandler {
      override def receive(
          client: TransportClient,
          message: ByteBuffer,
          callback: RpcResponseCallback): Unit = {
        val msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message)
        msgObj match {

          case exec: RegisterExecutor if exec.execId == timingoutExecutor =>
            () // No reply to generate client-side timeout

          case exec: RegisterExecutor
            if exec.execId == tryAgainExecutor && !secondExecutorFailedOnce =>
            secondExecutorFailedOnce = true
            callback.onFailure(failure)

          case exec: RegisterExecutor if exec.execId == tryAgainExecutor =>
            callback.onSuccess(success)

          case exec: RegisterExecutor
            if exec.execId == succeedingExecutor && !thirdExecutorFailedOnce =>
            thirdExecutorFailedOnce = true
            callback.onFailure(failure)

          case exec: RegisterExecutor if exec.execId == succeedingExecutor =>
            callback.onSuccess(success)

        }
      }
    }

    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 0)

    Utils.tryWithResource(new TransportContext(transConf, handler, true)) { transCtx =>
      // a server which delays response 50ms and must try twice for success.
      def newShuffleServer(port: Int): (TransportServer, Int) = {
        (transCtx.createServer(port, Seq.empty[TransportServerBootstrap].asJava), port)
      }

      val candidatePort = RandomUtils.nextInt(1024, 65536)
      val (server, shufflePort) = Utils.startServiceOnPort(candidatePort,
        newShuffleServer, conf, "ShuffleServer")

      conf.set(SHUFFLE_SERVICE_ENABLED.key, "true")
      conf.set(SHUFFLE_SERVICE_PORT.key, shufflePort.toString)
      conf.set(SHUFFLE_REGISTRATION_TIMEOUT.key, "40")
      conf.set(SHUFFLE_REGISTRATION_MAX_ATTEMPTS.key, "1")
      var e = intercept[SparkException] {
        makeBlockManager(8000, timingoutExecutor)
      }.getMessage
      assert(e.contains("TimeoutException"))

      conf.set(SHUFFLE_REGISTRATION_TIMEOUT.key, "1000")
      conf.set(SHUFFLE_REGISTRATION_MAX_ATTEMPTS.key, "1")
      e = intercept[SparkException] {
        makeBlockManager(8000, tryAgainExecutor)
      }.getMessage
      assert(e.contains(tryAgainMsg))

      conf.set(SHUFFLE_REGISTRATION_TIMEOUT.key, "1000")
      conf.set(SHUFFLE_REGISTRATION_MAX_ATTEMPTS.key, "2")
      makeBlockManager(8000, succeedingExecutor)
      server.close()
    }
  }

  test("fetch remote block to local disk if block size is larger than threshold") {
    conf.set(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM, 1000L)

    val mockBlockManagerMaster = mock(classOf[BlockManagerMaster])
    val mockBlockTransferService = new MockBlockTransferService(0)
    val blockLocations = Seq(BlockManagerId("id-0", "host-0", 1))
    val blockStatus = BlockStatus(StorageLevel.DISK_ONLY, 0L, 2000L)

    when(mockBlockManagerMaster.getLocationsAndStatus(mc.any[BlockId])).thenReturn(
      Option(BlockLocationsAndStatus(blockLocations, blockStatus)))
    when(mockBlockManagerMaster.getLocations(mc.any[BlockId])).thenReturn(blockLocations)

    val store = makeBlockManager(8000, "executor1", mockBlockManagerMaster,
      transferService = Option(mockBlockTransferService))
    val block = store.getRemoteBytes("item")
      .asInstanceOf[Option[ByteBuffer]]

    assert(block.isDefined)
    assert(mockBlockTransferService.numCalls === 1)
    // assert FileManager is not null if the block size is larger than threshold.
    assert(mockBlockTransferService.tempFileManager === store.remoteBlockTempFileManager)
  }

  test("query locations of blockIds") {
    val mockBlockManagerMaster = mock(classOf[BlockManagerMaster])
    val blockLocations = Seq(BlockManagerId("1", "host1", 100), BlockManagerId("2", "host2", 200))
    when(mockBlockManagerMaster.getLocations(mc.any[Array[BlockId]]))
      .thenReturn(Array(blockLocations))
    val env = mock(classOf[SparkEnv])

    val blockIds: Array[BlockId] = Array(StreamBlockId(1, 2))
    val locs = BlockManager.blockIdsToLocations(blockIds, env, mockBlockManagerMaster)
    val expectedLocs = Seq("executor_host1_1", "executor_host2_2")
    assert(locs(blockIds(0)) == expectedLocs)
  }

  class MockBlockTransferService(val maxFailures: Int) extends BlockTransferService {
    var numCalls = 0
    var tempFileManager: DownloadFileManager = null

    override def init(blockDataManager: BlockDataManager): Unit = {}

    override def fetchBlocks(
        host: String,
        port: Int,
        execId: String,
        blockIds: Array[String],
        listener: BlockFetchingListener,
        tempFileManager: DownloadFileManager): Unit = {
      listener.onBlockFetchSuccess("mockBlockId", new NioManagedBuffer(ByteBuffer.allocate(1)))
    }

    override def close(): Unit = {}

    override def hostName: String = { "MockBlockTransferServiceHost" }

    override def port: Int = { 63332 }

    override def uploadBlock(
        hostname: String,
        port: Int,
        execId: String,
        blockId: BlockId,
        blockData: ManagedBuffer,
        level: StorageLevel,
        classTag: ClassTag[_]): Future[Unit] = {
      import scala.concurrent.ExecutionContext.Implicits.global
      Future {}
    }

    override def fetchBlockSync(
        host: String,
        port: Int,
        execId: String,
        blockId: String,
        tempFileManager: DownloadFileManager): ManagedBuffer = {
      numCalls += 1
      this.tempFileManager = tempFileManager
      if (numCalls <= maxFailures) {
        throw new RuntimeException("Failing block fetch in the mock block transfer service")
      }
      super.fetchBlockSync(host, port, execId, blockId, tempFileManager)
    }
  }
}

private object BlockManagerSuite {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  private implicit class BlockManagerTestUtils(store: BlockManager) {

    def dropFromMemoryIfExists(
        blockId: BlockId,
        data: () => Either[Array[Any], ChunkedByteBuffer]): Unit = {
      store.blockInfoManager.lockForWriting(blockId).foreach { info =>
        val newEffectiveStorageLevel = store.dropFromMemory(blockId, data)
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          store.releaseLock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          store.blockInfoManager.removeBlock(blockId)
        }
      }
    }

    private def wrapGet[T](f: BlockId => Option[T]): BlockId => Option[T] = (blockId: BlockId) => {
      val result = f(blockId)
      if (result.isDefined) {
        store.releaseLock(blockId)
      }
      result
    }

    def hasLocalBlock(blockId: BlockId): Boolean = {
      getLocalAndReleaseLock(blockId).isDefined
    }

    val getLocalAndReleaseLock: (BlockId) => Option[BlockResult] = wrapGet(store.getLocalValues)
    val getAndReleaseLock: (BlockId) => Option[BlockResult] = wrapGet(store.get)
    val getSingleAndReleaseLock: (BlockId) => Option[Any] = wrapGet(store.getSingle)
    val getLocalBytesAndReleaseLock: (BlockId) => Option[ChunkedByteBuffer] = {
      val allocator = ByteBuffer.allocate _
      wrapGet { bid => store.getLocalBytes(bid).map(_.toChunkedByteBuffer(allocator)) }
    }
  }

}

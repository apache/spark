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

import java.io.{File, InputStream, IOException}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, TimeoutException}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.classTag

import com.esotericsoftware.kryo.KryoException
import org.mockito.{ArgumentCaptor, ArgumentMatchers => mc}
import org.mockito.Mockito.{atLeastOnce, doAnswer, mock, never, spy, times, verify, when}
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.internal.config
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Kryo.{KRYO_USE_POOL, KRYO_USE_UNSAFE}
import org.apache.spark.internal.config.Tests._
import org.apache.spark.memory.{MemoryMode, UnifiedMemoryManager}
import org.apache.spark.network.{BlockDataManager, BlockTransferService, TransportContext}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.server.{NoOpRpcHandler, TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ExecutorDiskUtils, ExternalBlockStoreClient}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, RegisterExecutor}
import org.apache.spark.network.util.{MapConfigProvider, TransportConf}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, MapStatus, MergeStatus, SparkListenerBlockUpdated}
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessages, CoarseGrainedSchedulerBackend}
import org.apache.spark.security.{CryptoStreamUtils, EncryptionFunSuite}
import org.apache.spark.serializer.{DeserializationStream, JavaSerializer, KryoDeserializationStream, KryoSerializer, KryoSerializerInstance, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo, ShuffleBlockResolver, ShuffleManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.io.ChunkedByteBuffer

class BlockManagerSuite extends SparkFunSuite with Matchers with PrivateMethodTester
  with LocalSparkContext with ResetSystemProperties with EncryptionFunSuite with TimeLimits {

  import BlockManagerSuite._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  var conf: SparkConf = null
  val allStores = ArrayBuffer[BlockManager]()
  val sortShuffleManagers = ArrayBuffer[SortShuffleManager]()
  var rpcEnv: RpcEnv = null
  var master: BlockManagerMaster = null
  var liveListenerBus: LiveListenerBus = null
  val securityMgr = new SecurityManager(new SparkConf(false))
  val bcastManager = new BroadcastManager(true, new SparkConf(false))
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
      .set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
      .set(Kryo.KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
      .set(STORAGE_UNROLL_MEMORY_THRESHOLD, 512L)
      .set(Network.RPC_ASK_TIMEOUT, "5s")
      .set(PUSH_BASED_SHUFFLE_ENABLED, true)
      .set(RDD_CACHE_VISIBILITY_TRACKING_ENABLED, true)
  }

  private def makeSortShuffleManager(conf: Option[SparkConf] = None): SortShuffleManager = {
    val newMgr = new SortShuffleManager(conf.getOrElse(new SparkConf(false)))
    sortShuffleManagers += newMgr
    newMgr
  }

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER,
      master: BlockManagerMaster = this.master,
      transferService: Option[BlockTransferService] = Option.empty,
      testConf: Option[SparkConf] = None,
      shuffleManager: ShuffleManager = shuffleManager): BlockManager = {
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
    val serializerManager = new SerializerManager(serializer, bmConf, encryptionKey)
    val transfer = transferService.getOrElse(new NettyBlockTransferService(
      bmConf, securityMgr, serializerManager, "localhost", "localhost", 0, 1))
    val memManager = UnifiedMemoryManager(bmConf, numCores = 1)
    val externalShuffleClient = if (bmConf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(bmConf, "shuffle", 0)
      Some(new ExternalBlockStoreClient(transConf, bmSecurityMgr,
        bmSecurityMgr.isAuthenticationEnabled(), bmConf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }
    val blockManager = new BlockManager(name, rpcEnv, master, serializerManager, bmConf,
      memManager, mapOutputTracker, shuffleManager, transfer, bmSecurityMgr, externalShuffleClient)
    memManager.setMemoryStore(blockManager.memoryStore)
    allStores += blockManager
    blockManager.initialize("app-id")
    blockManager
  }

  // Save modified system properties so that we can restore them after tests.
  val originalArch = System.getProperty("os.arch")
  val originalCompressedOops = System.getProperty(TEST_USE_COMPRESSED_OOPS_KEY)

  def reinitializeSizeEstimator(arch: String, useCompressedOops: String): Unit = {
    def set(k: String, v: String): Unit = {
      if (v == null) {
        System.clearProperty(k)
      } else {
        System.setProperty(k, v)
      }
    }
    set("os.arch", arch)
    set(TEST_USE_COMPRESSED_OOPS_KEY, useCompressedOops)
    val initialize = PrivateMethod[Unit](Symbol("initialize"))
    SizeEstimator invokePrivate initialize()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    reinitializeSizeEstimator("amd64", "true")
    conf = new SparkConf(false)
    init(conf)

    rpcEnv = RpcEnv.create("test", conf.get(config.DRIVER_HOST_ADDRESS),
      conf.get(config.DRIVER_PORT), conf, securityMgr)
    conf.set(DRIVER_PORT, rpcEnv.address.port)
    conf.set(DRIVER_HOST_ADDRESS, rpcEnv.address.host)

    // Mock SparkContext to reduce the memory usage of tests. It's fine since the only reason we
    // need to create a SparkContext is to initialize LiveListenerBus.
    sc = mock(classOf[SparkContext])
    when(sc.conf).thenReturn(conf)

    val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]()
    liveListenerBus = spy[LiveListenerBus](new LiveListenerBus(conf))
    master = spy[BlockManagerMaster](new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf,
        liveListenerBus, None, blockManagerInfo, mapOutputTracker, shuffleManager,
        isDriver = true)),
      rpcEnv.setupEndpoint("blockmanagerHeartbeat",
      new BlockManagerMasterHeartbeatEndpoint(rpcEnv, true, blockManagerInfo)), conf, true))
  }

  override def afterEach(): Unit = {
    // Restore system properties and SizeEstimator to their original states.
    reinitializeSizeEstimator(originalArch, originalCompressedOops)

    try {
      conf = null
      allStores.foreach(_.stop())
      allStores.clear()
      sortShuffleManagers.foreach(_.stop())
      sortShuffleManagers.clear()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
      rpcEnv = null
      master = null
      liveListenerBus = null
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    try {
      // Cleanup the reused items.
      Option(bcastManager).foreach(_.stop())
      Option(mapOutputTracker).foreach(_.stop())
      Option(shuffleManager).foreach(_.stop())
    } finally {
      super.afterAll()
    }
  }

  private def stopBlockManager(blockManager: BlockManager): Unit = {
    allStores -= blockManager
    blockManager.stop()
  }

  /**
   * Setup driverEndpoint, executor-1(BlockManager), executor-2(BlockManager) to simulate
   * the real cluster before the tests. Any requests from driver to executor-1 will be responded
   * in time. However, any requests from driver to executor-2 will be timeouted, in order to test
   * the specific handling of `TimeoutException`, which is raised at driver side.
   *
   * And, when `withLost` is true, we will not register the executor-2 to the driver. Therefore,
   * it behaves like a lost executor in terms of driver's view. When `withLost` is false, we'll
   * register the executor-2 normally.
   */
  private def setupBlockManagerMasterWithBlocks(withLost: Boolean): Unit = {
    // set up a simple DriverEndpoint which simply adds executorIds and
    // checks whether a certain executorId has been added before.
    val driverEndpoint = rpcEnv.setupEndpoint(CoarseGrainedSchedulerBackend.ENDPOINT_NAME,
      new RpcEndpoint {
        private val executorSet = mutable.HashSet[String]()
        override val rpcEnv: RpcEnv = BlockManagerSuite.this.rpcEnv
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case CoarseGrainedClusterMessages.RegisterExecutor(executorId, _, _, _, _, _, _, _) =>
            executorSet += executorId
            context.reply(true)
          case CoarseGrainedClusterMessages.IsExecutorAlive(executorId) =>
            context.reply(executorSet.contains(executorId))
        }
      }
    )

    def createAndRegisterBlockManager(timeout: Boolean): BlockManagerId = {
      val id = if (timeout) "timeout" else "normal"
      val bmRef = rpcEnv.setupEndpoint(s"bm-$id", new RpcEndpoint {
        override val rpcEnv: RpcEnv = BlockManagerSuite.this.rpcEnv
        private def reply[T](context: RpcCallContext, response: T): Unit = {
          if (timeout) {
            Thread.sleep(conf.getTimeAsMs(Network.RPC_ASK_TIMEOUT.key) + 1000)
          }
          context.reply(response)
        }

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case RemoveRdd(_) => reply(context, 1)
          case RemoveBroadcast(_, _) => reply(context, 1)
          case RemoveShuffle(_) => reply(context, true)
        }
      })
      val bmId = BlockManagerId(s"exec-$id", "localhost", 1234, None)
      master.registerBlockManager(bmId, Array.empty, 2000, 0, bmRef)
    }

    // set up normal bm1
    val bm1Id = createAndRegisterBlockManager(false)
    // set up bm2, which intentionally takes more time than RPC_ASK_TIMEOUT to
    // remove rdd/broadcast/shuffle in order to raise timeout error
    val bm2Id = createAndRegisterBlockManager(true)

    driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.RegisterExecutor(
      bm1Id.executorId, null, bm1Id.host, 1, Map.empty, Map.empty,
      Map.empty, 0))

    if (!withLost) {
      driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.RegisterExecutor(
        bm2Id.executorId, null, bm1Id.host, 1, Map.empty, Map.empty, Map.empty, 0))
    }

    eventually(timeout(5.seconds)) {
      // make sure both bm1 and bm2 are registered at driver side BlockManagerMaster
      verify(master, times(2))
        .registerBlockManager(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any())
      assert(driverEndpoint.askSync[Boolean](
        CoarseGrainedClusterMessages.IsExecutorAlive(bm1Id.executorId)))
      assert(driverEndpoint.askSync[Boolean](
        CoarseGrainedClusterMessages.IsExecutorAlive(bm2Id.executorId)) === !withLost)
    }

    // update RDD block info for bm1 and bm2 (Broadcast and shuffle don't report block
    // locations to BlockManagerMaster)
    master.updateBlockInfo(bm1Id, RDDBlockId(0, 0), StorageLevel.MEMORY_ONLY, 100, 0)
    master.updateBlockInfo(bm2Id, RDDBlockId(0, 1), StorageLevel.MEMORY_ONLY, 100, 0)
  }

  Seq(true, false).foreach { shuffleServiceEnabled =>
    test("SPARK-45310: report shuffle block status should respect " +
      s"external shuffle service (enabled=$shuffleServiceEnabled)") {
      val conf = new SparkConf()
        .set(config.SHUFFLE_SERVICE_ENABLED, shuffleServiceEnabled)
        .set(config.Tests.TEST_SKIP_ESS_REGISTER, true)
      val bm = makeBlockManager(1000, "executor", testConf = Some(conf))
      val blockManagerId = bm.blockManagerId
      val shuffleServiceId = bm.shuffleServerId
      bm.reportBlockStatus(BlockId("rdd_0_0"), BlockStatus.empty)
      eventually(timeout(5.seconds)) {
        // For non-shuffle blocks, it should just report block manager id.
        verify(master, times(1))
          .updateBlockInfo(mc.eq(blockManagerId), mc.any(), mc.any(), mc.any(), mc.any())
      }
      bm.reportBlockStatus(BlockId("shuffle_0_0_0.index"), BlockStatus.empty)
      bm.reportBlockStatus(BlockId("shuffle_0_0_0.data"), BlockStatus.empty)
      eventually(timeout(5.seconds)) {
        // For shuffle blocks, it should report shuffle service id (if enabled)
        // instead of block manager id.
        val (expectedBMId, expectedTimes) = if (shuffleServiceEnabled) {
          (shuffleServiceId, 2)
        } else {
          (blockManagerId, 3)
        }
        verify(master, times(expectedTimes))
          .updateBlockInfo(mc.eq(expectedBMId), mc.any(), mc.any(), mc.any(), mc.any())
      }
    }
  }

  test("SPARK-36036: make sure temporary download files are deleted") {
    val store = makeBlockManager(8000, "executor")

    def createAndRegisterTempFileForDeletion(): String = {
      val transportConf = new TransportConf("test", MapConfigProvider.EMPTY)
      val tempDownloadFile = store.remoteBlockTempFileManager.createTempFile(transportConf)

      tempDownloadFile.openForWriting().close()
      assert(new File(tempDownloadFile.path()).exists(), "The file has been created")

      val registered = store.remoteBlockTempFileManager.registerTempFileToClean(tempDownloadFile)
      assert(registered, "The file has been successfully registered for auto clean up")

      // tempDownloadFile and the channel for writing are local to the function so the references
      // are going to be eliminated on exit
      tempDownloadFile.path()
    }

    val filePath = createAndRegisterTempFileForDeletion()

    val numberOfTries = 100 // try increasing if the test starts to behave flaky
    val fileHasBeenDeleted = (1 to numberOfTries).exists { tryNo =>
      // Unless -XX:-DisableExplicitGC is set it works in Hotspot JVM
      System.gc()
      Thread.sleep(tryNo)
      val fileStillExists = new File(filePath).exists()
      !fileStillExists
    }

    assert(fileHasBeenDeleted,
      s"The file was supposed to be auto deleted (GC hinted $numberOfTries times)")
  }

  test("SPARK-32091: count failures from active executors when remove rdd/broadcast/shuffle") {
    setupBlockManagerMasterWithBlocks(false)
    // fail because bm2 will timeout and it's not lost anymore
    assert(intercept[Exception](master.removeRdd(0, true))
      .getCause.isInstanceOf[TimeoutException])
    assert(intercept[Exception](master.removeBroadcast(0, true, true))
      .getCause.isInstanceOf[TimeoutException])
    assert(intercept[Exception](master.removeShuffle(0, true))
      .getCause.isInstanceOf[TimeoutException])
  }

  test("SPARK-32091: ignore failures from lost executors when remove rdd/broadcast/shuffle") {
    setupBlockManagerMasterWithBlocks(true)
    // succeed because bm1 will remove rdd/broadcast successfully and bm2 will
    // timeout but ignored as it's lost
    master.removeRdd(0, true)
    master.removeBroadcast(0, true, true)
    master.removeShuffle(0, true)
  }

  test("SPARK-41360: Avoid block manager re-registration if the executor has been lost") {
    // Set up a DriverEndpoint which always returns isExecutorAlive=false
    rpcEnv.setupEndpoint(CoarseGrainedSchedulerBackend.ENDPOINT_NAME,
      new RpcEndpoint {
        override val rpcEnv: RpcEnv = BlockManagerSuite.this.rpcEnv

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case CoarseGrainedClusterMessages.RegisterExecutor(executorId, _, _, _, _, _, _, _) =>
            context.reply(true)
          case CoarseGrainedClusterMessages.IsExecutorAlive(executorId) =>
            // always return false
            context.reply(false)
        }
      }
    )

    // Set up a block manager endpoint and endpoint reference
    val bmRef = rpcEnv.setupEndpoint(s"bm-0", new RpcEndpoint {
      override val rpcEnv: RpcEnv = BlockManagerSuite.this.rpcEnv

      private def reply[T](context: RpcCallContext, response: T): Unit = {
        context.reply(response)
      }

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case RemoveRdd(_) => reply(context, 1)
        case RemoveBroadcast(_, _) => reply(context, 1)
        case RemoveShuffle(_) => reply(context, true)
      }
    })
    val bmId = BlockManagerId(s"exec-0", "localhost", 1234, None)
    // Register the block manager with isReRegister = true
    val updatedId = master.registerBlockManager(
      bmId, Array.empty, 2000, 0, bmRef, isReRegister = true)
    // The re-registration should fail since the executor is considered as dead by DriverEndpoint
    assert(updatedId.executorId === BlockManagerId.INVALID_EXECUTOR_ID)
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
      assertUpdateBlockInfoReportedForRemovingBlock(store, "a1-to-remove",
        removedFromMemory = true, removedFromDisk = false)
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!store.hasLocalBlock("a2-to-remove"))
      master.getLocations("a2-to-remove") should have size 0
      assertUpdateBlockInfoReportedForRemovingBlock(store, "a2-to-remove",
        removedFromMemory = true, removedFromDisk = false)
    }
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(store.hasLocalBlock("a3-to-remove"))
      master.getLocations("a3-to-remove") should have size 0
      assertUpdateBlockInfoNotReported(store, "a3-to-remove")
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
    assertUpdateBlockInfoReportedForRemovingBlock(executorStore, broadcast0BlockId,
      removedFromMemory = false, removedFromDisk = true)

    // nothing should be removed from the driver store
    assert(driverStore.hasLocalBlock(broadcast0BlockId))
    assert(driverStore.hasLocalBlock(broadcast1BlockId))
    assert(driverStore.hasLocalBlock(broadcast2BlockId))
    assertUpdateBlockInfoNotReported(driverStore, broadcast0BlockId)

    // remove broadcast 0 block from the driver as well
    master.removeBroadcast(0, removeFromMaster = true, blocking = true)
    assert(!driverStore.hasLocalBlock(broadcast0BlockId))
    assert(driverStore.hasLocalBlock(broadcast1BlockId))
    assertUpdateBlockInfoReportedForRemovingBlock(driverStore, broadcast0BlockId,
      removedFromMemory = false, removedFromDisk = true)

    // remove broadcast 1 block from both the stores asynchronously
    // and verify all broadcast 1 blocks have been removed
    master.removeBroadcast(1, removeFromMaster = true, blocking = false)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!driverStore.hasLocalBlock(broadcast1BlockId))
      assert(!executorStore.hasLocalBlock(broadcast1BlockId))
      assertUpdateBlockInfoReportedForRemovingBlock(driverStore, broadcast1BlockId,
        removedFromMemory = false, removedFromDisk = true)
      assertUpdateBlockInfoReportedForRemovingBlock(executorStore, broadcast1BlockId,
        removedFromMemory = false, removedFromDisk = true)
    }

    // remove broadcast 2 from both the stores asynchronously
    // and verify all broadcast 2 blocks have been removed
    master.removeBroadcast(2, removeFromMaster = true, blocking = false)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!driverStore.hasLocalBlock(broadcast2BlockId))
      assert(!driverStore.hasLocalBlock(broadcast2BlockId2))
      assert(!executorStore.hasLocalBlock(broadcast2BlockId))
      assert(!executorStore.hasLocalBlock(broadcast2BlockId2))
      assertUpdateBlockInfoReportedForRemovingBlock(driverStore, broadcast2BlockId,
        removedFromMemory = false, removedFromDisk = true)
      assertUpdateBlockInfoReportedForRemovingBlock(driverStore, broadcast2BlockId2,
        removedFromMemory = false, removedFromDisk = true)
      assertUpdateBlockInfoReportedForRemovingBlock(executorStore, broadcast2BlockId,
        removedFromMemory = false, removedFromDisk = true)
      assertUpdateBlockInfoReportedForRemovingBlock(executorStore, broadcast2BlockId2,
        removedFromMemory = false, removedFromDisk = true)
    }
    executorStore.stop()
    driverStore.stop()
  }

  private def assertUpdateBlockInfoReportedForRemovingBlock(
      store: BlockManager,
      blockId: BlockId,
      removedFromMemory: Boolean,
      removedFromDisk: Boolean): Unit = {
    def assertSizeReported(captor: ArgumentCaptor[Long], expectRemoved: Boolean): Unit = {
      assert(captor.getAllValues().size() >= 1)
      if (expectRemoved) {
        assert(captor.getValue() > 0)
      } else {
        assert(captor.getValue() === 0)
      }
    }

    val memSizeCaptor = ArgumentCaptor.forClass(classOf[Long]).asInstanceOf[ArgumentCaptor[Long]]
    val diskSizeCaptor = ArgumentCaptor.forClass(classOf[Long]).asInstanceOf[ArgumentCaptor[Long]]
    val storageLevelCaptor =
      ArgumentCaptor.forClass(classOf[StorageLevel]).asInstanceOf[ArgumentCaptor[StorageLevel]]
    verify(master, atLeastOnce()).updateBlockInfo(mc.eq(store.blockManagerId), mc.eq(blockId),
      storageLevelCaptor.capture(), memSizeCaptor.capture(), diskSizeCaptor.capture())
    assertSizeReported(memSizeCaptor, removedFromMemory)
    assertSizeReported(diskSizeCaptor, removedFromDisk)
    assert(storageLevelCaptor.getValue.replication == 0)
  }

  private def assertUpdateBlockInfoNotReported(store: BlockManager, blockId: BlockId): Unit = {
    verify(master, never()).updateBlockInfo(mc.eq(store.blockManagerId), mc.eq(blockId),
      mc.any[StorageLevel](), mc.anyInt(), mc.anyInt())
  }

  test("reregistration on heart beat") {
    val store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingleAndReleaseLock("a1").isDefined, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    val reregister = !master.driverHeartbeatEndPoint.askSync[Boolean](
      BlockManagerHeartbeat(store.blockManagerId))
    assert(reregister)
  }

  test("reregistration on block update") {
    val store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)

    // Set up a DriverEndpoint which simulates the executor is alive (required by SPARK-41360)
    rpcEnv.setupEndpoint(CoarseGrainedSchedulerBackend.ENDPOINT_NAME,
      new RpcEndpoint {
        override val rpcEnv: RpcEnv = BlockManagerSuite.this.rpcEnv

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case CoarseGrainedClusterMessages.IsExecutorAlive(executorId) =>
            if (executorId == store.blockManagerId.executorId) {
              context.reply(true)
            } else {
              context.reply(false)
            }
        }
      }
    )

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
        override def run(): Unit = {
          store.putIterator(
            "a2", a2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
        }
      }
      val t2 = new Thread {
        override def run(): Unit = {
          store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
        }
      }
      val t3 = new Thread {
        override def run(): Unit = {
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
    val sortLocations = PrivateMethod[Seq[BlockManagerId]](Symbol("sortLocations"))
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
    val sortLocations = PrivateMethod[Seq[BlockManagerId]](Symbol("sortLocations"))
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

  Seq(
    StorageLevel(useDisk = true, useMemory = false, deserialized = false),
    StorageLevel(useDisk = true, useMemory = false, deserialized = true),
    StorageLevel(useDisk = true, useMemory = false, deserialized = true, replication = 2)
  ).foreach { storageLevel =>
    test(s"SPARK-27622: avoid the network when block requested from same host, $storageLevel") {
      conf.set("spark.shuffle.io.maxRetries", "0")
      val sameHostBm = makeBlockManager(8000, "sameHost", master)

      val otherHostTransferSrv = spy[BlockTransferService](sameHostBm.blockTransferService)
      doAnswer { _ =>
         "otherHost"
      }.when(otherHostTransferSrv).hostName
      val otherHostBm = makeBlockManager(8000, "otherHost", master, Some(otherHostTransferSrv))

      // This test always uses the cleanBm to get the block. In case of replication
      // the block can be added to the otherHostBm as direct disk read will use
      // the local disk of sameHostBm where the block is replicated to.
      // When there is no replication then block must be added via sameHostBm directly.
      val bmToPutBlock = if (storageLevel.replication > 1) otherHostBm else sameHostBm
      val array = Array.fill(16)(Byte.MinValue to Byte.MaxValue).flatten
      val blockId = "list"
      bmToPutBlock.putIterator(blockId, List(array).iterator, storageLevel, tellMaster = true)

      val sameHostTransferSrv = spy[BlockTransferService](sameHostBm.blockTransferService)
      doAnswer { _ =>
         fail("Fetching over network is not expected when the block is requested from same host")
      }.when(sameHostTransferSrv).fetchBlockSync(mc.any(), mc.any(), mc.any(), mc.any(), mc.any())
      val cleanBm = makeBlockManager(8000, "clean", master, Some(sameHostTransferSrv))

      // check getRemoteBytes
      val bytesViaStore1 = cleanBm.getRemoteBytes(blockId)
      assert(bytesViaStore1.isDefined)
      val expectedContent = sameHostBm.getLocalBlockData(blockId).nioByteBuffer().array()
      assert(bytesViaStore1.get.toArray === expectedContent)

      // check getRemoteValues
      val valueViaStore1 = cleanBm.getRemoteValues[List.type](blockId)
      assert(valueViaStore1.isDefined)
      assert(valueViaStore1.get.data.toList.head === array)
    }
  }

  private def testWithFileDelAfterLocalDiskRead(level: StorageLevel, getValueOrBytes: Boolean) = {
    val testedFunc = if (getValueOrBytes) "getRemoteValue()" else "getRemoteBytes()"
    val testNameSuffix = s"$level, $testedFunc"
    test(s"SPARK-27622: as file is removed fall back to network fetch, $testNameSuffix") {
      conf.set("spark.shuffle.io.maxRetries", "0")
      // variable to check the usage of the local disk of the remote executor on the same host
      var sameHostExecutorTried: Boolean = false
      val store2 = makeBlockManager(8000, "executor2", this.master,
        Some(new MockBlockTransferService(0)))
      val blockId = "list"
      val array = Array.fill(16)(Byte.MinValue to Byte.MaxValue).flatten
      store2.putIterator(blockId, List(array).iterator, level, true)
      val expectedBlockData = store2.getLocalBytes(blockId)
      assert(expectedBlockData.isDefined)
      val expectedByteBuffer = expectedBlockData.get.toByteBuffer()
      val mockTransferService = new MockBlockTransferService(0) {
        override def fetchBlockSync(
            host: String,
            port: Int,
            execId: String,
            blockId: String,
            tempFileManager: DownloadFileManager): ManagedBuffer = {
          assert(sameHostExecutorTried, "before using the network local disk of the remote " +
            "executor (running on the same host) is expected to be tried")
          new NioManagedBuffer(expectedByteBuffer)
        }
      }
      val store1 = makeBlockManager(8000, "executor1", this.master, Some(mockTransferService))
      val spiedStore1 = spy[BlockManager](store1)
      doAnswer { inv =>
        val blockId = inv.getArguments()(0).asInstanceOf[BlockId]
        val localDirs = inv.getArguments()(1).asInstanceOf[Array[String]]
        val blockSize = inv.getArguments()(2).asInstanceOf[Long]
        val res = store1.readDiskBlockFromSameHostExecutor(blockId, localDirs, blockSize)
        assert(res.isDefined)
        val file = new File(
          ExecutorDiskUtils.getFilePath(localDirs, store1.subDirsPerLocalDir, blockId.name))
        // delete the file behind the blockId
        assert(file.delete())
        sameHostExecutorTried = true
        res
      }.when(spiedStore1).readDiskBlockFromSameHostExecutor(mc.any(), mc.any(), mc.any())

      if (getValueOrBytes) {
        val valuesViaStore1 = spiedStore1.getRemoteValues(blockId)
        assert(sameHostExecutorTried)
        assert(valuesViaStore1.isDefined)
        assert(valuesViaStore1.get.data.toList.head === array)
      } else {
        val bytesViaStore1 = spiedStore1.getRemoteBytes(blockId)
        assert(sameHostExecutorTried)
        assert(bytesViaStore1.isDefined)
        assert(bytesViaStore1.get.toByteBuffer === expectedByteBuffer)
      }
    }
  }

  Seq(
    StorageLevel(useDisk = true, useMemory = false, deserialized = false),
    StorageLevel(useDisk = true, useMemory = false, deserialized = true)
  ).foreach { storageLevel =>
    Seq(true, false).foreach { valueOrBytes =>
      testWithFileDelAfterLocalDiskRead(storageLevel, valueOrBytes)
    }
  }

  test("SPARK-14252: getOrElseUpdate should still read from remote storage") {
    val store = spy[BlockManager](makeBlockManager(8000, "executor1"))
    val store2 = makeBlockManager(8000, "executor2")
    val list1 = List(new Array[Byte](4000))
    val blockId = RDDBlockId(0, 0)
    store2.putIterator(
      blockId, list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

    doAnswer { _ => true }.when(store).isRDDBlockVisible(mc.any())
    assert(store.getOrElseUpdateRDDBlock(
      0L,
      blockId,
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
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val transfer = new NettyBlockTransferService(
      conf, securityMgr, serializerManager, "localhost", "localhost", 0, 1)
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      serializerManager, conf, memoryManager, mapOutputTracker,
      shuffleManager, transfer, securityMgr, None)
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
    assert(blockManager.getLocalBlockData(blockId).nioByteBuffer().array() === ser)
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
      val transfer = new NettyBlockTransferService(
        conf, securityMgr, serializerManager, "localhost", "localhost", 0, 1)
      val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
      val blockManager = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
        serializerManager, conf, memoryManager, mapOutputTracker,
        shuffleManager, transfer, securityMgr, None)
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
    assert(store.master.getBlockStatus("list1", askStorageEndpoints = false).size === 0)
    assert(store.master.getBlockStatus("list2", askStorageEndpoints = false).size === 1)
    assert(store.master.getBlockStatus("list3", askStorageEndpoints = false).size === 1)
    assert(store.master.getBlockStatus("list1", askStorageEndpoints = true).size === 0)
    assert(store.master.getBlockStatus("list2", askStorageEndpoints = true).size === 1)
    assert(store.master.getBlockStatus("list3", askStorageEndpoints = true).size === 1)

    // This time don't tell master and see what happens. By LRU, only list5 and list6 remains.
    store.putIterator(
      "list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.putIterator(
      "list5", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator(
      "list6", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // getLocations should return nothing because the master is not informed
    // getBlockStatus without asking storage endpoints should have the same result
    // getBlockStatus with asking storage endpoints, however, should return the actual statuses
    assert(store.master.getLocations("list4").size === 0)
    assert(store.master.getLocations("list5").size === 0)
    assert(store.master.getLocations("list6").size === 0)
    assert(store.master.getBlockStatus("list4", askStorageEndpoints = false).size === 0)
    assert(store.master.getBlockStatus("list5", askStorageEndpoints = false).size === 0)
    assert(store.master.getBlockStatus("list6", askStorageEndpoints = false).size === 0)
    assert(store.master.getBlockStatus("list4", askStorageEndpoints = true).size === 0)
    assert(store.master.getBlockStatus("list5", askStorageEndpoints = true).size === 1)
    assert(store.master.getBlockStatus("list6", askStorageEndpoints = true).size === 1)
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
    assert(store.master.getMatchingBlockIds(
      _.toString.contains("list"), askStorageEndpoints = false).size
      === 3)
    assert(store.master.getMatchingBlockIds(
      _.toString.contains("list1"), askStorageEndpoints = false).size
      === 1)

    // insert some more blocks
    store.putIterator(
      "newlist1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator(
      "newlist2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator(
      "newlist3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)

    // getLocations and getBlockStatus should yield the same locations
    assert(
      store.master.getMatchingBlockIds(
        _.toString.contains("newlist"), askStorageEndpoints = false).size
      === 1)
    assert(
      store.master.getMatchingBlockIds(
        _.toString.contains("newlist"), askStorageEndpoints = true).size
      === 3)

    val blockIds = Seq(RDDBlockId(1, 0), RDDBlockId(1, 1), RDDBlockId(2, 0))
    blockIds.foreach { blockId =>
      store.putIterator(
        blockId, list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    val matchedBlockIds = store.master.getMatchingBlockIds(_ match {
      case RDDBlockId(1, _) => true
      case _ => false
    }, askStorageEndpoints = true)
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
    val result3 = memoryStore.putIteratorAsValues("b3", smallIterator, MemoryMode.ON_HEAP,
      ClassTag.Any)
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
    val result4 = memoryStore.putIteratorAsValues("b4", bigIterator, MemoryMode.ON_HEAP,
      ClassTag.Any)
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
    when(mockBlockManagerMaster.getLocationsAndStatus(mc.any[BlockId], mc.any[String])).thenReturn(
      Option(BlockLocationsAndStatus(blockManagerIds, BlockStatus.empty, None)))
    when(mockBlockManagerMaster.getLocations(mc.any[BlockId])).thenReturn(
      blockManagerIds)

    val store = makeBlockManager(8000, "executor1", mockBlockManagerMaster,
      transferService = Option(mockBlockTransferService))
    val block = store.getRemoteBytes("item")
      .asInstanceOf[Option[ByteBuffer]]
    assert(block.isDefined)
    verify(mockBlockManagerMaster, times(1))
      .getLocationsAndStatus("item", "MockBlockTransferServiceHost")
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

  test("SPARK-25888: serving of removed file not detected by shuffle service") {
    // although the existence of the file is checked before serving it but a delete can happen
    // somewhere after that check
    val store = makeBlockManager(8000, "executor1")
    val emptyBlockFetcher = new MockBlockTransferService(0) {
      override def fetchBlockSync(
        host: String,
        port: Int,
        execId: String,
        blockId: String,
        tempFileManager: DownloadFileManager): ManagedBuffer = {
        val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 1)
        // empty ManagedBuffer
        new FileSegmentManagedBuffer(transConf, new File("missing.file"), 0, 0)
      }
    }
    val store2 = makeBlockManager(8000, "executor2", this.master, Some(emptyBlockFetcher))
    store.putSingle("item", "value", StorageLevel.DISK_ONLY, tellMaster = true)
    assert(master.getLocations("item").nonEmpty)
    assert(store2.getRemoteBytes("item").isEmpty)
  }

  test("test sorting of block locations") {
    val localHost = "localhost"
    val otherHost = "otherHost"
    val store = makeBlockManager(8000, "executor1")
    val externalShuffleServicePort = StorageUtils.externalShuffleServicePort(conf)
    val port = store.blockTransferService.port
    val rack = Some("rack")
    val blockManagerWithTopologyInfo = BlockManagerId(
      store.blockManagerId.executorId,
      store.blockManagerId.host,
      store.blockManagerId.port,
      rack)
    store.blockManagerId = blockManagerWithTopologyInfo
    val locations = Seq(
      BlockManagerId("executor4", otherHost, externalShuffleServicePort, rack),
      BlockManagerId("executor3", otherHost, port, rack),
      BlockManagerId("executor6", otherHost, externalShuffleServicePort),
      BlockManagerId("executor5", otherHost, port),
      BlockManagerId("executor2", localHost, externalShuffleServicePort),
      BlockManagerId("executor1", localHost, port))
    val sortedLocations = Seq(
      BlockManagerId("executor1", localHost, port),
      BlockManagerId("executor2", localHost, externalShuffleServicePort),
      BlockManagerId("executor3", otherHost, port, rack),
      BlockManagerId("executor4", otherHost, externalShuffleServicePort, rack),
      BlockManagerId("executor5", otherHost, port),
      BlockManagerId("executor6", otherHost, externalShuffleServicePort))
    assert(store.sortLocations(locations) === sortedLocations)
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

      val candidatePort = ThreadLocalRandom.current().nextInt(1024, 65536)
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

    when(mockBlockManagerMaster.getLocationsAndStatus(mc.any[BlockId], mc.any[String])).thenReturn(
      Option(BlockLocationsAndStatus(blockLocations, blockStatus, None)))
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
      .thenReturn(Array(blockLocations).toImmutableArraySeq)
    val env = mock(classOf[SparkEnv])

    val blockIds: Array[BlockId] = Array(StreamBlockId(1, 2))
    val locs = BlockManager.blockIdsToLocations(blockIds, env, mockBlockManagerMaster)
    val expectedLocs = Seq("executor_host1_1", "executor_host2_2")
    assert(locs(blockIds(0)) == expectedLocs)
  }

  test("SPARK-30594: Do not post SparkListenerBlockUpdated when updateBlockInfo returns false") {
    // update block info for non-existent block manager
    val updateInfo = UpdateBlockInfo(BlockManagerId("1", "host1", 100),
      BlockId("test_1"), StorageLevel.MEMORY_ONLY, 1, 1)
    val result = master.driverEndpoint.askSync[Boolean](updateInfo)

    assert(!result)
    verify(liveListenerBus, never()).post(SparkListenerBlockUpdated(BlockUpdatedInfo(updateInfo)))
  }

  test("we reject putting blocks when we have the wrong shuffle resolver") {
    val badShuffleManager = mock(classOf[ShuffleManager])
    val badShuffleResolver = mock(classOf[ShuffleBlockResolver])
    when(badShuffleManager.shuffleBlockResolver).thenReturn(badShuffleResolver)
    val shuffleBlockId = ShuffleDataBlockId(0, 0, 0)
    val bm = makeBlockManager(100, "exec1", shuffleManager = badShuffleManager)
    val message = "message"
    val exception = intercept[SparkException] {
      bm.putBlockDataAsStream(shuffleBlockId, StorageLevel.DISK_ONLY, ClassTag(message.getClass))
    }
    assert(exception.getMessage.contains("unsupported shuffle resolver"))
  }

  test("test decommission block manager should not be part of peers") {
    val exec1 = "exec1"
    val exec2 = "exec2"
    val exec3 = "exec3"
    val store1 = makeBlockManager(1000, exec1)
    val store2 = makeBlockManager(1000, exec2)
    val store3 = makeBlockManager(1000, exec3)

    assert(master.getPeers(store3.blockManagerId).map(_.executorId).toSet === Set(exec1, exec2))

    val data = new Array[Byte](4)
    val blockId = rdd(0, 0)
    store1.putSingle(blockId, data, StorageLevel.MEMORY_ONLY_2)
    assert(master.getLocations(blockId).size === 2)

    master.decommissionBlockManagers(Seq(exec1))
    // store1 is decommissioned, so it should not be part of peer list for store3
    assert(master.getPeers(store3.blockManagerId).map(_.executorId).toSet === Set(exec2))
  }

  test("test decommissionRddCacheBlocks should migrate all cached blocks") {
    val store1 = makeBlockManager(1000, "exec1")
    val store2 = makeBlockManager(1000, "exec2")
    val store3 = makeBlockManager(1000, "exec3")

    val data = new Array[Byte](4)
    val blockId = rdd(0, 0)
    store1.putSingle(blockId, data, StorageLevel.MEMORY_ONLY_2)
    assert(master.getLocations(blockId).size === 2)
    assert(master.getLocations(blockId).contains(store1.blockManagerId))

    val decomManager = new BlockManagerDecommissioner(conf, store1)
    decomManager.decommissionRddCacheBlocks()
    assert(master.getLocations(blockId).size === 2)
    assert(master.getLocations(blockId).toSet === Set(store2.blockManagerId,
      store3.blockManagerId))
  }

  test("test decommissionRddCacheBlocks should keep the block if it is not able to migrate") {
    val store1 = makeBlockManager(3500, "exec1")
    val store2 = makeBlockManager(1000, "exec2")

    val dataLarge = new Array[Byte](1500)
    val blockIdLarge = rdd(0, 0)
    val dataSmall = new Array[Byte](1)
    val blockIdSmall = rdd(0, 1)

    store1.putSingle(blockIdLarge, dataLarge, StorageLevel.MEMORY_ONLY)
    store1.putSingle(blockIdSmall, dataSmall, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations(blockIdLarge) === Seq(store1.blockManagerId))
    assert(master.getLocations(blockIdSmall) === Seq(store1.blockManagerId))

    val decomManager = new BlockManagerDecommissioner(conf, store1)
    decomManager.decommissionRddCacheBlocks()
    // Smaller block migrated to store2
    assert(master.getLocations(blockIdSmall) === Seq(store2.blockManagerId))
    // Larger block still present in store1 as it can't be migrated
    assert(master.getLocations(blockIdLarge) === Seq(store1.blockManagerId))
  }

  private def testShuffleBlockDecommissioning(
      maxShuffleSize: Option[Int], willReject: Boolean, enableIoEncryption: Boolean) = {
    maxShuffleSize.foreach{ size =>
      conf.set(STORAGE_DECOMMISSION_SHUFFLE_MAX_DISK_SIZE.key, s"${size}b")
    }
    conf.set(IO_ENCRYPTION_ENABLED, enableIoEncryption)

    val shuffleManager1 = makeSortShuffleManager(Some(conf))
    val bm1 = makeBlockManager(3500, "exec1", shuffleManager = shuffleManager1)
    shuffleManager1.shuffleBlockResolver._blockManager = bm1

    val shuffleManager2 = makeSortShuffleManager(Some(conf))
    val bm2 = makeBlockManager(3500, "exec2", shuffleManager = shuffleManager2)
    shuffleManager2.shuffleBlockResolver._blockManager = bm2

    val blockSize = 5
    val shuffleDataBlockContent = Array[Byte](0, 1, 2, 3, 4)
    val shuffleData = ShuffleDataBlockId(0, 0, 0)
    val shuffleData2 = ShuffleDataBlockId(1, 0, 0)
    Files.write(bm1.diskBlockManager.getFile(shuffleData).toPath(), shuffleDataBlockContent)
    Files.write(bm2.diskBlockManager.getFile(shuffleData2).toPath(), shuffleDataBlockContent)
    val shuffleIndexBlockContent = Array[Byte](5, 6, 7, 8, 9)
    val shuffleIndex = ShuffleIndexBlockId(0, 0, 0)
    val shuffleIndexOnly = ShuffleIndexBlockId(0, 1, 0)
    val shuffleIndex2 = ShuffleIndexBlockId(1, 0, 0)
    Files.write(bm1.diskBlockManager.getFile(shuffleIndex).toPath(), shuffleIndexBlockContent)
    Files.write(bm1.diskBlockManager.getFile(shuffleIndexOnly).toPath(), shuffleIndexBlockContent)
    Files.write(bm2.diskBlockManager.getFile(shuffleIndex2).toPath(), shuffleIndexBlockContent)

    mapOutputTracker.registerShuffle(0, 2, MergeStatus.SHUFFLE_PUSH_DUMMY_NUM_REDUCES)
    val decomManager = new BlockManagerDecommissioner(
      conf.set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true), bm1)
    try {
      mapOutputTracker.registerMapOutput(0, 0, MapStatus(bm1.blockManagerId, Array(blockSize), 0))
      mapOutputTracker.registerMapOutput(0, 1, MapStatus(bm1.blockManagerId, Array(blockSize), 1))
      assert(mapOutputTracker.shuffleStatuses(0).mapStatuses(0).location === bm1.blockManagerId)
      assert(mapOutputTracker.shuffleStatuses(0).mapStatuses(1).location === bm1.blockManagerId)

      val env = mock(classOf[SparkEnv])
      when(env.conf).thenReturn(conf)
      SparkEnv.set(env)

      decomManager.refreshMigratableShuffleBlocks()

      if (willReject) {
        eventually(timeout(1.second), interval(10.milliseconds)) {
          assert(mapOutputTracker.shuffleStatuses(0).mapStatuses(0).location === bm2.blockManagerId)
          assert(mapOutputTracker.shuffleStatuses(0).mapStatuses(1).location === bm2.blockManagerId)
        }
        assert(Files.readAllBytes(bm2.diskBlockManager.getFile(shuffleData).toPath())
          === shuffleDataBlockContent)
        assert(Files.readAllBytes(bm2.diskBlockManager.getFile(shuffleIndex).toPath())
          === shuffleIndexBlockContent)
      } else {
        Thread.sleep(1000)
        assert(mapOutputTracker.shuffleStatuses(0).mapStatuses(0).location === bm1.blockManagerId)
      }
    } finally {
      mapOutputTracker.unregisterShuffle(0)
      // Avoid thread leak
      decomManager.stopMigratingShuffleBlocks()
    }
  }

  test("test migration of shuffle blocks during decommissioning - no limit") {
    testShuffleBlockDecommissioning(None, true, false)
  }

  test("test migration of shuffle blocks during decommissioning - no limit - " +
      "io.encryption enabled") {
    testShuffleBlockDecommissioning(None, true, true)
  }

  test("test migration of shuffle blocks during decommissioning - larger limit") {
    testShuffleBlockDecommissioning(Some(10000), true, false)
  }

  test("test migration of shuffle blocks during decommissioning - larger limit - " +
      "io.encryption enabled") {
    testShuffleBlockDecommissioning(Some(10000), true, true)
  }

  test("[SPARK-34363]test migration of shuffle blocks during decommissioning - small limit") {
    testShuffleBlockDecommissioning(Some(1), false, false)
  }

  test("[SPARK-34363]test migration of shuffle blocks during decommissioning - small limit -" +
      " io.encryption enabled") {
    testShuffleBlockDecommissioning(Some(1), false, true)
  }

  test("SPARK-32919: Shuffle push merger locations should be bounded with in" +
    " spark.shuffle.push.retainedMergerLocations") {
    assert(master.getShufflePushMergerLocations(10, Set.empty).isEmpty)
    makeBlockManager(100, "execA",
      transferService = Some(new MockBlockTransferService(10, "hostA")))
    makeBlockManager(100, "execB",
      transferService = Some(new MockBlockTransferService(10, "hostB")))
    makeBlockManager(100, "execC",
      transferService = Some(new MockBlockTransferService(10, "hostC")))
    makeBlockManager(100, "execD",
      transferService = Some(new MockBlockTransferService(10, "hostD")))
    makeBlockManager(100, "execE",
      transferService = Some(new MockBlockTransferService(10, "hostA")))
    assert(master.getShufflePushMergerLocations(10, Set.empty).size == 4)
    assert(master.getShufflePushMergerLocations(10, Set.empty).map(_.host).sorted ===
      Seq("hostC", "hostD", "hostA", "hostB").sorted)
    assert(master.getShufflePushMergerLocations(10, Set("hostB")).size == 3)
  }

  test("SPARK-32919: Prefer active executor locations for shuffle push mergers") {
    makeBlockManager(100, "execA",
      transferService = Some(new MockBlockTransferService(10, "hostA")))
    makeBlockManager(100, "execB",
      transferService = Some(new MockBlockTransferService(10, "hostB")))
    makeBlockManager(100, "execC",
      transferService = Some(new MockBlockTransferService(10, "hostC")))
    makeBlockManager(100, "execD",
      transferService = Some(new MockBlockTransferService(10, "hostD")))
    makeBlockManager(100, "execE",
      transferService = Some(new MockBlockTransferService(10, "hostA")))
    assert(master.getShufflePushMergerLocations(5, Set.empty).size == 4)
    assert(master.getExecutorEndpointRef(SparkContext.DRIVER_IDENTIFIER).isEmpty)
    makeBlockManager(100, SparkContext.DRIVER_IDENTIFIER,
      transferService = Some(new MockBlockTransferService(10, "host-driver")))
    assert(master.getExecutorEndpointRef(SparkContext.DRIVER_IDENTIFIER).isDefined)
    master.removeExecutor("execA")
    master.removeExecutor("execE")

    assert(master.getShufflePushMergerLocations(3, Set.empty).size == 3)
    assert(master.getShufflePushMergerLocations(3, Set.empty).map(_.host).sorted ===
      Seq("hostC", "hostB", "hostD").sorted)
    assert(master.getShufflePushMergerLocations(4, Set.empty).map(_.host).sorted ===
      Seq("hostB", "hostA", "hostC", "hostD").sorted)
    master.removeShufflePushMergerLocation("hostA")
    assert(master.getShufflePushMergerLocations(4, Set.empty).map(_.host).sorted ===
      Seq("hostB", "hostC", "hostD").sorted)
  }

  test("SPARK-33387 Support ordered shuffle block migration") {
    val blocks: Seq[ShuffleBlockInfo] = Seq(
      ShuffleBlockInfo(1, 0L),
      ShuffleBlockInfo(0, 1L),
      ShuffleBlockInfo(0, 0L),
      ShuffleBlockInfo(1, 1L))
    val sortedBlocks = blocks.sortBy(b => (b.shuffleId, b.mapId))

    val resolver = mock(classOf[MigratableResolver])
    when(resolver.getStoredShuffles()).thenReturn(blocks)

    val bm = mock(classOf[BlockManager])
    when(bm.migratableResolver).thenReturn(resolver)
    when(bm.getPeers(mc.any())).thenReturn(Seq.empty)

    val decomManager = new BlockManagerDecommissioner(conf, bm)
    decomManager.refreshMigratableShuffleBlocks()

    assert(sortedBlocks.sameElements(decomManager.shufflesToMigrate.asScala.map(_._1)))
  }

  test("SPARK-34193: Potential race condition during decommissioning with TorrentBroadcast") {
    // Validate that we allow putting of broadcast blocks during decommissioning
    val exec1 = "exec1"

    val store = makeBlockManager(1000, exec1)
    master.decommissionBlockManagers(Seq(exec1))
    val a = new Array[Byte](1)
    // Put a broadcast block, no exception
    val broadcast0BlockId = BroadcastBlockId(0)
    store.putSingle(broadcast0BlockId, a, StorageLevel.DISK_ONLY)
  }

  test("check KryoException when getting disk blocks and 'Input/output error' is occurred") {
    val kryoSerializerWithDiskCorruptedInputStream
      = createKryoSerializerWithDiskCorruptedInputStream()

    case class User(id: Long, name: String)

    conf.set(TEST_MEMORY, 1200L)
    val serializerManager = new SerializerManager(kryoSerializerWithDiskCorruptedInputStream, conf)
    val transfer = new NettyBlockTransferService(
      conf, securityMgr, serializerManager, "localhost", "localhost", 0, 1)
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      serializerManager, conf, memoryManager, mapOutputTracker,
      shuffleManager, transfer, securityMgr, None)
    allStores += store
    store.initialize("app-id")
    store.putSingle("my-block-id", new Array[User](300), StorageLevel.MEMORY_AND_DISK)

    val kryoException = intercept[KryoException] {
      store.get("my-block-id")
    }
    assert(kryoException.getMessage === "java.io.IOException: Input/output error")
    assertUpdateBlockInfoReportedForRemovingBlock(store, "my-block-id",
      removedFromMemory = false, removedFromDisk = true)
  }

  test("check KryoException when saving blocks into memory and 'Input/output error' is occurred") {
    val kryoSerializerWithDiskCorruptedInputStream
      = createKryoSerializerWithDiskCorruptedInputStream()

    conf.set(TEST_MEMORY, 1200L)
    val serializerManager = new SerializerManager(kryoSerializerWithDiskCorruptedInputStream, conf)
    val transfer = new NettyBlockTransferService(
      conf, securityMgr, serializerManager, "localhost", "localhost", 0, 1)
    val memoryManager = UnifiedMemoryManager(conf, numCores = 1)
    val store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      serializerManager, conf, memoryManager, mapOutputTracker,
      shuffleManager, transfer, securityMgr, None)
    allStores += store
    store.initialize("app-id")

    val blockId = RDDBlockId(0, 0)
    val bytes = Array.tabulate[Byte](1000)(_.toByte)
    val byteBuffer = new ChunkedByteBuffer(ByteBuffer.wrap(bytes))

    val kryoException = intercept[KryoException] {
      store.putBytes(blockId, byteBuffer, StorageLevel.MEMORY_AND_DISK)
    }
    assert(kryoException.getMessage === "java.io.IOException: Input/output error")
  }

  test("SPARK-39647: Failure to register with ESS should prevent registering the BM") {
    val handler = new NoOpRpcHandler {
      override def receive(
          client: TransportClient,
          message: ByteBuffer,
          callback: RpcResponseCallback): Unit = {
        val msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message)
        msgObj match {
          case _: RegisterExecutor => () // No reply to generate client-side timeout
        }
      }
    }
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
    Utils.tryWithResource(new TransportContext(transConf, handler, true)) { transCtx =>
      def newShuffleServer(port: Int): (TransportServer, Int) = {
        (transCtx.createServer(port, Seq.empty[TransportServerBootstrap].asJava), port)
      }

      val candidatePort = ThreadLocalRandom.current().nextInt(1024, 65536)
      val (server, shufflePort) = Utils.startServiceOnPort(candidatePort,
        newShuffleServer, conf, "ShuffleServer")

      conf.set(SHUFFLE_SERVICE_ENABLED.key, "true")
      conf.set(SHUFFLE_SERVICE_PORT.key, shufflePort.toString)
      conf.set(SHUFFLE_REGISTRATION_TIMEOUT.key, "40")
      conf.set(SHUFFLE_REGISTRATION_MAX_ATTEMPTS.key, "1")
      val e = intercept[SparkException] {
        makeBlockManager(8000, "timeoutExec")
      }.getMessage
      assert(e.contains("TimeoutException"))
      verify(master, times(0))
        .registerBlockManager(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any())
      server.close()
    }
  }

  test("SPARK-41497: getOrElseUpdateRDDBlock do compute based on cache visibility statue") {
    val store = makeBlockManager(8000, "executor1")
    val blockId = RDDBlockId(rddId = 1, splitIndex = 1)
    var computed: Boolean = false
    val data = Seq(1, 2, 3)
    val makeIterator = () => {
      computed = true
      data.iterator
    }

    // Cache doesn't exist and is not visible.
    assert(store.getStatus(blockId).isEmpty && !store.isRDDBlockVisible(blockId))
    val res1 = store.getOrElseUpdateRDDBlock(
      1, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], makeIterator)
    // Put cache successfully and reported block task info.
    assert(res1.isLeft && computed)
    verify(master, times(1)).updateRDDBlockTaskInfo(blockId, 1)
    assert(store.getStatus(blockId).nonEmpty && !store.isRDDBlockVisible(blockId))

    // Cache exists but not visible.
    computed = false
    val res2 = store.getOrElseUpdateRDDBlock(
      1, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], makeIterator)
    // Load cache successfully and reported block task info.
    assert(res2.isLeft && computed)
    assert(!store.isRDDBlockVisible(blockId))
    verify(master, times(2)).updateRDDBlockTaskInfo(blockId, 1)

    // Cache exists and visible.
    store.blockInfoManager.tryMarkBlockAsVisible(blockId)
    computed = false
    assert(store.getStatus(blockId).nonEmpty && store.isRDDBlockVisible(blockId))
    val res3 = store.getOrElseUpdateRDDBlock(
      1, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], makeIterator)
    // Load cache successfully but not report block task info.
    assert(res3.isLeft && !computed)
    verify(master, times(2)).updateRDDBlockTaskInfo(blockId, 1)
  }


  test("SPARK-41497: mark rdd block as visible") {
    val store = makeBlockManager(8000, "executor1")
    val blockId = RDDBlockId(rddId = 1, splitIndex = 1)
    val data = Seq(1, 2, 3)
    store.putIterator(blockId, data.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.getStatus(blockId).nonEmpty)
    assert(!store.blockInfoManager.isRDDBlockVisible(blockId))
    assert(store.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // Mark rdd block as visible.
    store.blockInfoManager.tryMarkBlockAsVisible(blockId)
    assert(store.blockInfoManager.isRDDBlockVisible(blockId))
    assert(!store.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // Cache the block again should not change the visibility status.
    store.putIterator(blockId, data.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.blockInfoManager.isRDDBlockVisible(blockId))
    assert(!store.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // Remove rdd block.
    store.removeBlock(blockId)
    assert(!store.blockInfoManager.isRDDBlockVisible(blockId))
    assert(!store.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // Visibility status should not be added once rdd is removed.
    store.blockInfoManager.tryMarkBlockAsVisible(blockId)
    assert(!store.blockInfoManager.isRDDBlockVisible(blockId))
    assert(!store.blockInfoManager.containsInvisibleRDDBlock(blockId))
  }


  test("SPARK-41497: master & manager interaction about rdd block visibility information") {
    val store1 = makeBlockManager(8000, "executor1")
    val store2 = makeBlockManager(8000, "executor2")
    val store3 = makeBlockManager(8000, "executor3")

    val taskId = 0L
    val blockId = RDDBlockId(rddId = 1, splitIndex = 1)
    val data = Seq(1, 2, 3)

    store1.getOrElseUpdateRDDBlock(
      taskId, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], () => data.iterator)
    // Block information is reported and block is not visible.
    assert(master.getLocations(blockId).nonEmpty)
    assert(!master.isRDDBlockVisible(blockId))
    assert(store1.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // A copy is reported, visibility status not changed.
    store2.putIterator(blockId, data.iterator, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations(blockId).length === 2)
    assert(!master.isRDDBlockVisible(blockId))
    assert(store2.blockInfoManager.containsInvisibleRDDBlock(blockId))

    // Report rdd block visibility as true, driver should ask block managers to mark the block
    // as visible.
    master.updateRDDBlockVisibility(taskId, visible = true)
    eventually(timeout(5.seconds)) {
      assert(master.isRDDBlockVisible(blockId))
      assert(store1.blockInfoManager.isRDDBlockVisible(blockId))
      assert(store2.blockInfoManager.isRDDBlockVisible(blockId))
    }

    // Visibility status should be updated right after block reported since it's already visible.
    assert(!store3.blockInfoManager.isRDDBlockVisible(blockId))
    store3.putIterator(blockId, data.iterator, StorageLevel.MEMORY_ONLY)
    eventually(timeout(5.seconds)) {
      assert(store3.blockInfoManager.isRDDBlockVisible(blockId))
    }
  }


  test("SPARK-41497: rdd block's visibility status should be cached once got from driver") {
    val store = makeBlockManager(8000, "executor1")
    val taskId = 0L
    val blockId = RDDBlockId(rddId = 1, splitIndex = 1)
    val data = Seq(1, 2, 3)

    store.getOrElseUpdateRDDBlock(
      taskId, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], () => data.iterator)
    // Block information is reported and block is not visible.
    assert(master.getLocations(blockId).nonEmpty)
    assert(!master.isRDDBlockVisible(blockId))
    assert(store.blockInfoManager.containsInvisibleRDDBlock(blockId))

    doAnswer(_ => true).when(master).isRDDBlockVisible(mc.any())
    // Visibility status should be cached.
    assert(store.isRDDBlockVisible(blockId))
    assert(!store.blockInfoManager.containsInvisibleRDDBlock(blockId))
    assert(store.blockInfoManager.isRDDBlockVisible(blockId))
  }

  test("SPARK-41497: getOrElseUpdateRDDBlock should make sure accumulators updated when block" +
    " already exist but still not visible") {
    val store = makeBlockManager(8000, "executor1")
    val taskId = 0L
    val blockId = RDDBlockId(rddId = 1, splitIndex = 1)
    val data = Seq(1, 2, 3)
    val acc = new LongAccumulator
    val makeIterator = () => {
      data.iterator.map { x =>
        acc.add(1)
        x
      }
    }

    store.getOrElseUpdateRDDBlock(
      taskId, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], makeIterator)
    // Block cached but not visible.
    assert(master.getLocations(blockId).nonEmpty)
    assert(!master.isRDDBlockVisible(blockId))
    assert(acc.value === 3)

    store.getOrElseUpdateRDDBlock(
      taskId, blockId, StorageLevel.MEMORY_ONLY, classTag[Int], makeIterator)
    // Accumulator should be updated even though block already exists.
    assert(acc.value === 6)
  }

  private def createKryoSerializerWithDiskCorruptedInputStream(): KryoSerializer = {
    class TestDiskCorruptedInputStream extends InputStream {
      override def read(): Int = throw new IOException("Input/output error")
    }

    class TestKryoDeserializationStream(serInstance: KryoSerializerInstance,
                                        inStream: InputStream,
                                        useUnsafe: Boolean)
      extends KryoDeserializationStream(serInstance, inStream, useUnsafe)

    class TestKryoSerializerInstance(ks: KryoSerializer, useUnsafe: Boolean, usePool: Boolean)
      extends KryoSerializerInstance(ks, useUnsafe, usePool) {
      override def deserializeStream(s: InputStream): DeserializationStream = {
        new TestKryoDeserializationStream(this, new TestDiskCorruptedInputStream(), false)
      }
    }

    class TestKryoSerializer(conf: SparkConf) extends KryoSerializer(conf) {
      override def newInstance(): SerializerInstance = {
        new TestKryoSerializerInstance(this, conf.get(KRYO_USE_UNSAFE), conf.get(KRYO_USE_POOL))
      }
    }

    new TestKryoSerializer(conf)
  }

  class MockBlockTransferService(
      val maxFailures: Int,
      override val hostName: String = "MockBlockTransferServiceHost") extends BlockTransferService {
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

    override def port: Int = { 63332 }

    override def uploadBlock(
        hostname: String,
        port: Int,
        execId: String,
        blockId: BlockId,
        blockData: ManagedBuffer,
        level: StorageLevel,
        classTag: ClassTag[_]): Future[Unit] = {
      // scalastyle:off executioncontextglobal
      import scala.concurrent.ExecutionContext.Implicits.global
      // scalastyle:on executioncontextglobal
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

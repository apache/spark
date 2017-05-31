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

import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.Utils

trait BlockManagerReplicationBehavior extends SparkFunSuite
  with Matchers
  with BeforeAndAfter
  with LocalSparkContext {

  val conf: SparkConf

  protected var rpcEnv: RpcEnv = null
  protected var master: BlockManagerMaster = null
  protected lazy val securityMgr = new SecurityManager(conf)
  protected lazy val bcastManager = new BroadcastManager(true, conf, securityMgr)
  protected lazy val mapOutputTracker = new MapOutputTrackerMaster(conf, bcastManager, true)
  protected lazy val shuffleManager = new SortShuffleManager(conf)

  // List of block manager created during an unit test, so that all of the them can be stopped
  // after the unit test.
  protected val allStores = new ArrayBuffer[BlockManager]

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  protected lazy val serializer = new KryoSerializer(conf)

  // Implicitly convert strings to BlockIds for test clarity.
  protected implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)

  protected def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    conf.set("spark.testing.memory", maxMem.toString)
    conf.set("spark.memory.offHeap.size", maxMem.toString)
    val transfer = new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1)
    val memManager = UnifiedMemoryManager(conf, numCores = 1)
    val serializerManager = new SerializerManager(serializer, conf)
    val store = new BlockManager(name, rpcEnv, master, serializerManager, conf,
      memManager, mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    memManager.setMemoryStore(store.memoryStore)
    store.initialize("app-id")
    allStores += store
    store
  }

  before {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)

    conf.set("spark.authenticate", "false")
    conf.set("spark.driver.port", rpcEnv.address.port.toString)
    conf.set("spark.testing", "true")
    conf.set("spark.memory.fraction", "1")
    conf.set("spark.memory.storageFraction", "1")
    conf.set("spark.storage.unrollFraction", "0.4")
    conf.set("spark.storage.unrollMemoryThreshold", "512")

    // to make a replication attempt to inactive store fail fast
    conf.set("spark.core.connection.ack.wait.timeout", "1s")
    // to make cached peers refresh frequently
    conf.set("spark.storage.cachedPeersTtl", "10")

    sc = new SparkContext("local", "test", conf)
    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf,
        new LiveListenerBus(sc))), conf, true)
    allStores.clear()
  }

  after {
    allStores.foreach { _.stop() }
    allStores.clear()
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null
    master = null
  }


  test("get peers with addition and removal of block managers") {
    val numStores = 4
    val stores = (1 to numStores - 1).map { i => makeBlockManager(1000, s"store$i") }
    val storeIds = stores.map { _.blockManagerId }.toSet
    assert(master.getPeers(stores(0).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(0).blockManagerId })
    assert(master.getPeers(stores(1).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(1).blockManagerId })
    assert(master.getPeers(stores(2).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(2).blockManagerId })

    // Add driver store and test whether it is filtered out
    val driverStore = makeBlockManager(1000, SparkContext.DRIVER_IDENTIFIER)
    assert(master.getPeers(stores(0).blockManagerId).forall(!_.isDriver))
    assert(master.getPeers(stores(1).blockManagerId).forall(!_.isDriver))
    assert(master.getPeers(stores(2).blockManagerId).forall(!_.isDriver))

    // Add a new store and test whether get peers returns it
    val newStore = makeBlockManager(1000, s"store$numStores")
    assert(master.getPeers(stores(0).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(0).blockManagerId } + newStore.blockManagerId)
    assert(master.getPeers(stores(1).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(1).blockManagerId } + newStore.blockManagerId)
    assert(master.getPeers(stores(2).blockManagerId).toSet ===
      storeIds.filterNot { _ == stores(2).blockManagerId } + newStore.blockManagerId)
    assert(master.getPeers(newStore.blockManagerId).toSet === storeIds)

    // Remove a store and test whether get peers returns it
    val storeIdToRemove = stores(0).blockManagerId
    master.removeExecutor(storeIdToRemove.executorId)
    assert(!master.getPeers(stores(1).blockManagerId).contains(storeIdToRemove))
    assert(!master.getPeers(stores(2).blockManagerId).contains(storeIdToRemove))
    assert(!master.getPeers(newStore.blockManagerId).contains(storeIdToRemove))

    // Test whether asking for peers of a unregistered block manager id returns empty list
    assert(master.getPeers(stores(0).blockManagerId).isEmpty)
    assert(master.getPeers(BlockManagerId("", "", 1)).isEmpty)
  }


  test("block replication - 2x replication") {
    testReplication(2,
      Seq(MEMORY_ONLY, MEMORY_ONLY_SER, DISK_ONLY, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER_2)
    )
  }

  test("block replication - 3x replication") {
    // Generate storage levels with 3x replication
    val storageLevels = {
      Seq(MEMORY_ONLY, MEMORY_ONLY_SER, DISK_ONLY, MEMORY_AND_DISK, MEMORY_AND_DISK_SER).map {
        level => StorageLevel(
          level.useDisk, level.useMemory, level.useOffHeap, level.deserialized, 3)
      }
    }
    testReplication(3, storageLevels)
  }

  test("block replication - mixed between 1x to 5x") {
    // Generate storage levels with varying replication
    val storageLevels = Seq(
      MEMORY_ONLY,
      MEMORY_ONLY_SER_2,
      StorageLevel(true, false, false, false, 3),
      StorageLevel(true, true, false, true, 4),
      StorageLevel(true, true, false, false, 5),
      StorageLevel(true, true, false, true, 4),
      StorageLevel(true, false, false, false, 3),
      MEMORY_ONLY_SER_2,
      MEMORY_ONLY
    )
    testReplication(5, storageLevels)
  }

  test("block replication - off-heap") {
    testReplication(2, Seq(OFF_HEAP, StorageLevel(true, true, true, false, 2)))
  }

  test("block replication - 2x replication without peers") {
    intercept[org.scalatest.exceptions.TestFailedException] {
      testReplication(1,
        Seq(StorageLevel.MEMORY_AND_DISK_2, StorageLevel(true, false, false, false, 3)))
    }
  }

  test("block replication - deterministic node selection") {
    val blockSize = 1000
    val storeSize = 10000
    val stores = (1 to 5).map {
      i => makeBlockManager(storeSize, s"store$i")
    }
    val storageLevel2x = StorageLevel.MEMORY_AND_DISK_2
    val storageLevel3x = StorageLevel(true, true, false, true, 3)
    val storageLevel4x = StorageLevel(true, true, false, true, 4)

    def putBlockAndGetLocations(blockId: String, level: StorageLevel): Set[BlockManagerId] = {
      stores.head.putSingle(blockId, new Array[Byte](blockSize), level)
      val locations = master.getLocations(blockId).sortBy { _.executorId }.toSet
      stores.foreach { _.removeBlock(blockId) }
      master.removeBlock(blockId)
      locations
    }

    // Test if two attempts to 2x replication returns same set of locations
    val a1Locs = putBlockAndGetLocations("a1", storageLevel2x)
    assert(putBlockAndGetLocations("a1", storageLevel2x) === a1Locs,
      "Inserting a 2x replicated block second time gave different locations from the first")

    // Test if two attempts to 3x replication returns same set of locations
    val a2Locs3x = putBlockAndGetLocations("a2", storageLevel3x)
    assert(putBlockAndGetLocations("a2", storageLevel3x) === a2Locs3x,
      "Inserting a 3x replicated block second time gave different locations from the first")

    // Test if 2x replication of a2 returns a strict subset of the locations of 3x replication
    val a2Locs2x = putBlockAndGetLocations("a2", storageLevel2x)
    assert(
      a2Locs2x.subsetOf(a2Locs3x),
      "Inserting a with 2x replication gave locations that are not a subset of locations" +
        s" with 3x replication [3x: ${a2Locs3x.mkString(",")}; 2x: ${a2Locs2x.mkString(",")}"
    )

    // Test if 4x replication of a2 returns a strict superset of the locations of 3x replication
    val a2Locs4x = putBlockAndGetLocations("a2", storageLevel4x)
    assert(
      a2Locs3x.subsetOf(a2Locs4x),
      "Inserting a with 4x replication gave locations that are not a superset of locations " +
        s"with 3x replication [3x: ${a2Locs3x.mkString(",")}; 4x: ${a2Locs4x.mkString(",")}"
    )

    // Test if 3x replication of two different blocks gives two different sets of locations
    val a3Locs3x = putBlockAndGetLocations("a3", storageLevel3x)
    assert(a3Locs3x !== a2Locs3x, "Two blocks gave same locations with 3x replication")
  }

  test("block replication - replication failures") {
    /*
      Create a system of three block managers / stores. One of them (say, failableStore)
      cannot receive blocks. So attempts to use that as replication target fails.

            +-----------/fails/-----------> failableStore
            |
        normalStore
            |
            +-----------/works/-----------> anotherNormalStore

        We are first going to add a normal block manager (i.e. normalStore) and the failable block
        manager (i.e. failableStore), and test whether 2x replication fails to create two
        copies of a block. Then we are going to add another normal block manager
        (i.e., anotherNormalStore), and test that now 2x replication works as the
        new store will be used for replication.
     */

    // Add a normal block manager
    val store = makeBlockManager(10000, "store")

    // Insert a block with 2x replication and return the number of copies of the block
    def replicateAndGetNumCopies(blockId: String): Int = {
      store.putSingle(blockId, new Array[Byte](1000), StorageLevel.MEMORY_AND_DISK_2)
      val numLocations = master.getLocations(blockId).size
      allStores.foreach { _.removeBlock(blockId) }
      numLocations
    }

    // Add a failable block manager with a mock transfer service that does not
    // allow receiving of blocks. So attempts to use it as a replication target will fail.
    val failableTransfer = mock(classOf[BlockTransferService]) // this wont actually work
    when(failableTransfer.hostName).thenReturn("some-hostname")
    when(failableTransfer.port).thenReturn(1000)
    conf.set("spark.testing.memory", "10000")
    val memManager = UnifiedMemoryManager(conf, numCores = 1)
    val serializerManager = new SerializerManager(serializer, conf)
    val failableStore = new BlockManager("failable-store", rpcEnv, master, serializerManager, conf,
      memManager, mapOutputTracker, shuffleManager, failableTransfer, securityMgr, 0)
    memManager.setMemoryStore(failableStore.memoryStore)
    failableStore.initialize("app-id")
    allStores += failableStore // so that this gets stopped after test
    assert(master.getPeers(store.blockManagerId).toSet === Set(failableStore.blockManagerId))

    // Test that 2x replication fails by creating only one copy of the block
    assert(replicateAndGetNumCopies("a1") === 1)

    // Add another normal block manager and test that 2x replication works
    makeBlockManager(10000, "anotherStore")
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      assert(replicateAndGetNumCopies("a2") === 2)
    }
  }

  test("block replication - addition and deletion of block managers") {
    val blockSize = 1000
    val storeSize = 10000
    val initialStores = (1 to 2).map { i => makeBlockManager(storeSize, s"store$i") }

    // Insert a block with given replication factor and return the number of copies of the block\
    def replicateAndGetNumCopies(blockId: String, replicationFactor: Int): Int = {
      val storageLevel = StorageLevel(true, true, false, true, replicationFactor)
      initialStores.head.putSingle(blockId, new Array[Byte](blockSize), storageLevel)
      val numLocations = master.getLocations(blockId).size
      allStores.foreach { _.removeBlock(blockId) }
      numLocations
    }

    // 2x replication should work, 3x replication should only replicate 2x
    assert(replicateAndGetNumCopies("a1", 2) === 2)
    assert(replicateAndGetNumCopies("a2", 3) === 2)

    // Add another store, 3x replication should work now, 4x replication should only replicate 3x
    val newStore1 = makeBlockManager(storeSize, s"newstore1")
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      assert(replicateAndGetNumCopies("a3", 3) === 3)
    }
    assert(replicateAndGetNumCopies("a4", 4) === 3)

    // Add another store, 4x replication should work now
    val newStore2 = makeBlockManager(storeSize, s"newstore2")
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      assert(replicateAndGetNumCopies("a5", 4) === 4)
    }

    // Remove all but the 1st store, 2x replication should fail
    (initialStores.tail ++ Seq(newStore1, newStore2)).foreach {
      store =>
        master.removeExecutor(store.blockManagerId.executorId)
        store.stop()
    }
    assert(replicateAndGetNumCopies("a6", 2) === 1)

    // Add new stores, 3x replication should work
    val newStores = (3 to 5).map {
      i => makeBlockManager(storeSize, s"newstore$i")
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      assert(replicateAndGetNumCopies("a7", 3) === 3)
    }
  }



  /**
   * Test replication of blocks with different storage levels (various combinations of
   * memory, disk & serialization). For each storage level, this function tests every store
   * whether the block is present and also tests the master whether its knowledge of blocks
   * is correct. Then it also drops the block from memory of each store (using LRU) and
   * again checks whether the master's knowledge gets updated.
   */
  protected def testReplication(maxReplication: Int, storageLevels: Seq[StorageLevel]) {
    import org.apache.spark.storage.StorageLevel._

    assert(maxReplication > 1,
      s"Cannot test replication factor $maxReplication")

    // storage levels to test with the given replication factor

    val storeSize = 10000
    val blockSize = 1000

    // As many stores as the replication factor
    val stores = (1 to maxReplication).map {
      i => makeBlockManager(storeSize, s"store$i")
    }

    storageLevels.foreach { storageLevel =>
      // Put the block into one of the stores
      val blockId = TestBlockId(
        "block-with-" + storageLevel.description.replace(" ", "-").toLowerCase(Locale.ROOT))
      val testValue = Array.fill[Byte](blockSize)(1)
      stores(0).putSingle(blockId, testValue, storageLevel)

      // Assert that master know two locations for the block
      val blockLocations = master.getLocations(blockId).map(_.executorId).toSet
      assert(blockLocations.size === storageLevel.replication,
        s"master did not have ${storageLevel.replication} locations for $blockId")

      // Test state of the stores that contain the block
      stores.filter {
        testStore => blockLocations.contains(testStore.blockManagerId.executorId)
      }.foreach { testStore =>
        val testStoreName = testStore.blockManagerId.executorId
        val blockResultOpt = testStore.getLocalValues(blockId)
        assert(blockResultOpt.isDefined, s"$blockId was not found in $testStoreName")
        val localValues = blockResultOpt.get.data.toSeq
        assert(localValues.size == 1)
        assert(localValues.head === testValue)
        assert(master.getLocations(blockId).map(_.executorId).toSet.contains(testStoreName),
          s"master does not have status for ${blockId.name} in $testStoreName")

        val memoryStore = testStore.memoryStore
        if (memoryStore.contains(blockId) && !storageLevel.deserialized) {
          memoryStore.getBytes(blockId).get.chunks.foreach { byteBuffer =>
            assert(storageLevel.useOffHeap == byteBuffer.isDirect,
              s"memory mode ${storageLevel.memoryMode} is not compatible with " +
                byteBuffer.getClass.getSimpleName)
          }
        }

        val blockStatus = master.getBlockStatus(blockId)(testStore.blockManagerId)

        // Assert that block status in the master for this store has expected storage level
        assert(
          blockStatus.storageLevel.useDisk === storageLevel.useDisk &&
            blockStatus.storageLevel.useMemory === storageLevel.useMemory &&
            blockStatus.storageLevel.useOffHeap === storageLevel.useOffHeap &&
            blockStatus.storageLevel.deserialized === storageLevel.deserialized,
          s"master does not know correct storage level for ${blockId.name} in $testStoreName")

        // Assert that the block status in the master for this store has correct memory usage info
        assert(!blockStatus.storageLevel.useMemory || blockStatus.memSize >= blockSize,
          s"master does not know size of ${blockId.name} stored in memory of $testStoreName")


        // If the block is supposed to be in memory, then drop the copy of the block in
        // this store test whether master is updated with zero memory usage this store
        if (storageLevel.useMemory) {
          val sl = if (storageLevel.useOffHeap) {
            StorageLevel(false, true, true, false, 1)
          } else {
            MEMORY_ONLY_SER
          }
          // Force the block to be dropped by adding a number of dummy blocks
          (1 to 10).foreach {
            i => testStore.putSingle(s"dummy-block-$i", new Array[Byte](1000), sl)
          }
          (1 to 10).foreach {
            i => testStore.removeBlock(s"dummy-block-$i")
          }

          val newBlockStatusOption = master.getBlockStatus(blockId).get(testStore.blockManagerId)

          // Assert that the block status in the master either does not exist (block removed
          // from every store) or has zero memory usage for this store
          assert(
            newBlockStatusOption.isEmpty || newBlockStatusOption.get.memSize === 0,
            s"after dropping, master does not know size of ${blockId.name} " +
              s"stored in memory of $testStoreName"
          )
        }

        // If the block is supposed to be in disk (after dropping or otherwise, then
        // test whether master has correct disk usage for this store
        if (storageLevel.useDisk) {
          assert(master.getBlockStatus(blockId)(testStore.blockManagerId).diskSize >= blockSize,
            s"after dropping, master does not know size of ${blockId.name} " +
              s"stored in disk of $testStoreName"
          )
        }
      }
      master.removeBlock(blockId)
    }
  }
}

class BlockManagerReplicationSuite extends BlockManagerReplicationBehavior {
  val conf = new SparkConf(false).set("spark.app.id", "test")
  conf.set("spark.kryoserializer.buffer", "1m")
}

class BlockManagerProactiveReplicationSuite extends BlockManagerReplicationBehavior {
  val conf = new SparkConf(false).set("spark.app.id", "test")
  conf.set("spark.kryoserializer.buffer", "1m")
  conf.set("spark.storage.replication.proactive", "true")
  conf.set("spark.storage.exceptionOnPinLeak", "true")

  (2 to 5).foreach { i =>
    test(s"proactive block replication - $i replicas - ${i - 1} block manager deletions") {
      testProactiveReplication(i)
    }
  }

  def testProactiveReplication(replicationFactor: Int) {
    val blockSize = 1000
    val storeSize = 10000
    val initialStores = (1 to 10).map { i => makeBlockManager(storeSize, s"store$i") }

    val blockId = "a1"

    val storageLevel = StorageLevel(true, true, false, true, replicationFactor)
    initialStores.head.putSingle(blockId, new Array[Byte](blockSize), storageLevel)

    val blockLocations = master.getLocations(blockId)
    logInfo(s"Initial locations : $blockLocations")

    assert(blockLocations.size === replicationFactor)

    // remove a random blockManager
    val executorsToRemove = blockLocations.take(replicationFactor - 1).toSet
    logInfo(s"Removing $executorsToRemove")
    initialStores.filter(bm => executorsToRemove.contains(bm.blockManagerId)).foreach { bm =>
      master.removeExecutor(bm.blockManagerId.executorId)
      bm.stop()
      // giving enough time for replication to happen and new block be reported to master
      eventually(timeout(5 seconds), interval(100 millis)) {
        val newLocations = master.getLocations(blockId).toSet
        assert(newLocations.size === replicationFactor)
      }
    }

    val newLocations = eventually(timeout(5 seconds), interval(100 millis)) {
      val _newLocations = master.getLocations(blockId).toSet
      assert(_newLocations.size === replicationFactor)
      _newLocations
    }
    logInfo(s"New locations : $newLocations")

    // new locations should not contain stopped block managers
    assert(newLocations.forall(bmId => !executorsToRemove.contains(bmId)),
      "New locations contain stopped block managers.")

    // Make sure all locks have been released.
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      initialStores.filter(bm => newLocations.contains(bm.blockManagerId)).foreach { bm =>
        assert(bm.blockInfoManager.getTaskLockCount(BlockInfo.NON_TASK_WRITER) === 0)
      }
    }
  }
}

class DummyTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  // number of racks to test with
  val numRacks = 3

  /**
   * Gets the topology information given the host name
   *
   * @param hostname Hostname
   * @return random topology
   */
  override def getTopologyForHost(hostname: String): Option[String] = {
    Some(s"/Rack-${Utils.random.nextInt(numRacks)}")
  }
}

class BlockManagerBasicStrategyReplicationSuite extends BlockManagerReplicationBehavior {
  val conf: SparkConf = new SparkConf(false).set("spark.app.id", "test")
  conf.set("spark.kryoserializer.buffer", "1m")
  conf.set(
    "spark.storage.replication.policy",
    classOf[BasicBlockReplicationPolicy].getName)
  conf.set(
    "spark.storage.replication.topologyMapper",
    classOf[DummyTopologyMapper].getName)
}


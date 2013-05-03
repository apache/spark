package spark.storage

import java.nio.ByteBuffer

import akka.actor._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.time.SpanSugar._

import spark.JavaSerializer
import spark.KryoSerializer
import spark.SizeEstimator
import spark.Utils
import spark.util.AkkaUtils
import spark.util.ByteBufferInputStream

class BlockManagerSuite extends FunSuite with BeforeAndAfter with PrivateMethodTester {
  var store: BlockManager = null
  var store2: BlockManager = null
  var actorSystem: ActorSystem = null
  var master: BlockManagerMaster = null
  var oldArch: String = null
  var oldOops: String = null
  var oldHeartBeat: String = null

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  System.setProperty("spark.kryoserializer.buffer.mb", "1")
  val serializer = new KryoSerializer

  before {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("test", "localhost", 0)
    this.actorSystem = actorSystem
    System.setProperty("spark.driver.port", boundPort.toString)
    System.setProperty("spark.hostPort", "localhost:" + boundPort)

    master = new BlockManagerMaster(
      actorSystem.actorOf(Props(new spark.storage.BlockManagerMasterActor(true))))

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    oldArch = System.setProperty("os.arch", "amd64")
    oldOops = System.setProperty("spark.test.useCompressedOops", "true")
    oldHeartBeat = System.setProperty("spark.storage.disableBlockManagerHeartBeat", "true")
    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
    // Set some value ...
    System.setProperty("spark.hostPort", spark.Utils.localHostName() + ":" + 1111)
  }

  after {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    if (store != null) {
      store.stop()
      store = null
    }
    if (store2 != null) {
      store2.stop()
      store2 = null
    }
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    actorSystem = null
    master = null

    if (oldArch != null) {
      System.setProperty("os.arch", oldArch)
    } else {
      System.clearProperty("os.arch")
    }

    if (oldOops != null) {
      System.setProperty("spark.test.useCompressedOops", oldOops)
    } else {
      System.clearProperty("spark.test.useCompressedOops")
    }
  }

  test("StorageLevel object caching") {
    val level1 = StorageLevel(false, false, false, 3)
    val level2 = StorageLevel(false, false, false, 3) // this should return the same object as level1
    val level3 = StorageLevel(false, false, false, 2) // this should return a different object
    assert(level2 === level1, "level2 is not same as level1")
    assert(level2.eq(level1), "level2 is not the same object as level1")
    assert(level3 != level1, "level3 is same as level1")
    val bytes1 = spark.Utils.serialize(level1)
    val level1_ = spark.Utils.deserialize[StorageLevel](bytes1)
    val bytes2 = spark.Utils.serialize(level2)
    val level2_ = spark.Utils.deserialize[StorageLevel](bytes2)
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
    val bytes1 = spark.Utils.serialize(id1)
    val id1_ = spark.Utils.deserialize[BlockManagerId](bytes1)
    val bytes2 = spark.Utils.serialize(id2)
    val id2_ = spark.Utils.deserialize[BlockManagerId](bytes2)
    assert(id1_ === id1, "Deserialized id1 is not same as original id1")
    assert(id1_.eq(id1), "Deserialized id1 is not the same object as original id1")
    assert(id2_ === id2, "Deserialized id2 is not same as original id2")
    assert(id2_.eq(id1), "Deserialized id2 is not the same object as original id1")
  }

  test("master + 1 manager interaction") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY, false)

    // Checking whether blocks are in memory
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1").size > 0, "master was not told about a1")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
    assert(master.getLocations("a3").size === 0, "master was told about a3")

    // Drop a1 and a2 from memory; this should be reported back to the master
    store.dropFromMemory("a1", null)
    store.dropFromMemory("a2", null)
    assert(store.getSingle("a1") === None, "a1 not removed from store")
    assert(store.getSingle("a2") === None, "a2 not removed from store")
    assert(master.getLocations("a1").size === 0, "master did not remove a1")
    assert(master.getLocations("a2").size === 0, "master did not remove a2")
  }

  test("master + 2 managers interaction") {
    store = new BlockManager("exec1", actorSystem, master, serializer, 2000)
    store2 = new BlockManager("exec2", actorSystem, master, new KryoSerializer, 2000)

    val peers = master.getPeers(store.blockManagerId, 1)
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
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)

    // Putting a1, a2 and a3 in memory and telling master only about a1 and a2
    store.putSingle("a1-to-remove", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2-to-remove", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3-to-remove", a3, StorageLevel.MEMORY_ONLY, false)

    // Checking whether blocks are in memory and memory size
    val memStatus = master.getMemoryStatus.head._2
    assert(memStatus._1 == 2000L, "total memory " + memStatus._1 + " should equal 2000")
    assert(memStatus._2 <= 1200L, "remaining memory " + memStatus._2 + " should <= 1200")
    assert(store.getSingle("a1-to-remove") != None, "a1 was not in store")
    assert(store.getSingle("a2-to-remove") != None, "a2 was not in store")
    assert(store.getSingle("a3-to-remove") != None, "a3 was not in store")

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
      memStatus._1 should equal (2000L)
      memStatus._2 should equal (2000L)
    }
  }

  test("removing rdd") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    // Putting a1, a2 and a3 in memory.
    store.putSingle("rdd_0_0", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_1", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("nonrddblock", a3, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0)

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("rdd_0_0") should be (None)
      master.getLocations("rdd_0_0") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("rdd_0_1") should be (None)
      master.getLocations("rdd_0_1") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("nonrddblock") should not be (None)
      master.getLocations("nonrddblock") should have size (1)
    }
  }

  test("reregistration on heart beat") {
    val heartBeat = PrivateMethod[Unit]('heartBeat)
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    store invokePrivate heartBeat()
    assert(master.getLocations("a1").size > 0, "a1 was not reregistered with master")
  }

  test("reregistration on block update") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    store.putSingle("a2", a1, StorageLevel.MEMORY_ONLY)
    store.waitForAsyncReregister()

    assert(master.getLocations("a1").size > 0, "a1 was not reregistered with master")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
  }

  test("reregistration doesn't dead lock") {
    val heartBeat = PrivateMethod[Unit]('heartBeat)
    store = new BlockManager("<driver>", actorSystem, master, serializer, 2000)
    val a1 = new Array[Byte](400)
    val a2 = List(new Array[Byte](400))

    // try many times to trigger any deadlocks
    for (i <- 1 to 100) {
      master.removeExecutor(store.blockManagerId.executorId)
      val t1 = new Thread {
        override def run() {
          store.put("a2", a2.iterator, StorageLevel.MEMORY_ONLY, true)
        }
      }
      val t2 = new Thread {
        override def run() {
          store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
        }
      }
      val t3 = new Thread {
        override def run() {
          store invokePrivate heartBeat()
        }
      }

      t1.start()
      t2.start()
      t3.start()
      t1.join()
      t2.join()
      t3.join()

      store.dropFromMemory("a1", null)
      store.dropFromMemory("a2", null)
      store.waitForAsyncReregister()
    }
  }

  test("in-memory LRU storage") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU storage with serialization") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU for partitions of same RDD") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("rdd_0_1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_3", a3, StorageLevel.MEMORY_ONLY)
    // Even though we accessed rdd_0_3 last, it should not have replaced partitions 1 and 2
    // from the same RDD
    assert(store.getSingle("rdd_0_3") === None, "rdd_0_3 was in store")
    assert(store.getSingle("rdd_0_2") != None, "rdd_0_2 was not in store")
    assert(store.getSingle("rdd_0_1") != None, "rdd_0_1 was not in store")
    // Check that rdd_0_3 doesn't replace them even after further accesses
    assert(store.getSingle("rdd_0_3") === None, "rdd_0_3 was in store")
    assert(store.getSingle("rdd_0_3") === None, "rdd_0_3 was in store")
    assert(store.getSingle("rdd_0_3") === None, "rdd_0_3 was in store")
  }

  test("in-memory LRU for partitions of multiple RDDs") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    store.putSingle("rdd_0_1", new Array[Byte](400), StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_2", new Array[Byte](400), StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_1_1", new Array[Byte](400), StorageLevel.MEMORY_ONLY)
    // At this point rdd_1_1 should've replaced rdd_0_1
    assert(store.memoryStore.contains("rdd_1_1"), "rdd_1_1 was not in store")
    assert(!store.memoryStore.contains("rdd_0_1"), "rdd_0_1 was in store")
    assert(store.memoryStore.contains("rdd_0_2"), "rdd_0_2 was not in store")
    // Do a get() on rdd_0_2 so that it is the most recently used item
    assert(store.getSingle("rdd_0_2") != None, "rdd_0_2 was not in store")
    // Put in more partitions from RDD 0; they should replace rdd_1_1
    store.putSingle("rdd_0_3", new Array[Byte](400), StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_4", new Array[Byte](400), StorageLevel.MEMORY_ONLY)
    // Now rdd_1_1 should be dropped to add rdd_0_3, but then rdd_0_2 should *not* be dropped
    // when we try to add rdd_0_4.
    assert(!store.memoryStore.contains("rdd_1_1"), "rdd_1_1 was in store")
    assert(!store.memoryStore.contains("rdd_0_1"), "rdd_0_1 was in store")
    assert(!store.memoryStore.contains("rdd_0_4"), "rdd_0_4 was in store")
    assert(store.memoryStore.contains("rdd_0_2"), "rdd_0_2 was not in store")
    assert(store.memoryStore.contains("rdd_0_3"), "rdd_0_3 was not in store")
  }

  test("on-disk storage") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    store.putSingle("a2", a2, StorageLevel.DISK_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    assert(store.getSingle("a2") != None, "a2 was in store")
    assert(store.getSingle("a3") != None, "a3 was in store")
    assert(store.getSingle("a1") != None, "a1 was in store")
  }

  test("disk and memory storage") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.memoryStore.getValues("a1") != None, "a1 was not in memory store")
  }

  test("disk and memory storage with getLocalBytes") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getLocalBytes("a2") != None, "a2 was not in store")
    assert(store.getLocalBytes("a3") != None, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1") != None, "a1 was not in store")
    assert(store.memoryStore.getValues("a1") != None, "a1 was not in memory store")
  }

  test("disk and memory storage with serialization") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.memoryStore.getValues("a1") != None, "a1 was not in memory store")
  }

  test("disk and memory storage with serialization and getLocalBytes") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getLocalBytes("a2") != None, "a2 was not in store")
    assert(store.getLocalBytes("a3") != None, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1") != None, "a1 was not in store")
    assert(store.memoryStore.getValues("a1") != None, "a1 was not in memory store")
  }

  test("LRU with mixed storage levels") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    val a4 = new Array[Byte](400)
    // First store a1 and a2, both in memory, and a3, on disk only
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    // At this point LRU should not kick in because a3 is only on disk
    assert(store.getSingle("a1") != None, "a2 was not in store")
    assert(store.getSingle("a2") != None, "a3 was not in store")
    assert(store.getSingle("a3") != None, "a1 was not in store")
    assert(store.getSingle("a1") != None, "a2 was not in store")
    assert(store.getSingle("a2") != None, "a3 was not in store")
    assert(store.getSingle("a3") != None, "a1 was not in store")
    // Now let's add in a4, which uses both disk and memory; a1 should drop out
    store.putSingle("a4", a4, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a4") != None, "a4 was not in store")
  }

  test("in-memory LRU with streams") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val list1 = List(new Array[Byte](200), new Array[Byte](200))
    val list2 = List(new Array[Byte](200), new Array[Byte](200))
    val list3 = List(new Array[Byte](200), new Array[Byte](200))
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY, true)
    store.put("list2", list2.iterator, StorageLevel.MEMORY_ONLY, true)
    store.put("list3", list3.iterator, StorageLevel.MEMORY_ONLY, true)
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") != None, "list3 was not in store")
    assert(store.get("list3").get.size == 2)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    // At this point list2 was gotten last, so LRU will getSingle rid of list3
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY, true)
    assert(store.get("list1") != None, "list1 was not in store")
    assert(store.get("list1").get.size == 2)
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") === None, "list1 was in store")
  }

  test("LRU with mixed storage levels and streams") {
    store = new BlockManager("<driver>", actorSystem, master, serializer, 1200)
    val list1 = List(new Array[Byte](200), new Array[Byte](200))
    val list2 = List(new Array[Byte](200), new Array[Byte](200))
    val list3 = List(new Array[Byte](200), new Array[Byte](200))
    val list4 = List(new Array[Byte](200), new Array[Byte](200))
    // First store list1 and list2, both in memory, and list3, on disk only
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY_SER, true)
    store.put("list2", list2.iterator, StorageLevel.MEMORY_ONLY_SER, true)
    store.put("list3", list3.iterator, StorageLevel.DISK_ONLY, true)
    // At this point LRU should not kick in because list3 is only on disk
    assert(store.get("list1") != None, "list2 was not in store")
    assert(store.get("list1").get.size === 2)
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size === 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size === 2)
    assert(store.get("list1") != None, "list2 was not in store")
    assert(store.get("list1").get.size === 2)
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size === 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size === 2)
    // Now let's add in list4, which uses both disk and memory; list1 should drop out
    store.put("list4", list4.iterator, StorageLevel.MEMORY_AND_DISK_SER, true)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size === 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size === 2)
    assert(store.get("list4") != None, "list4 was not in store")
    assert(store.get("list4").get.size === 2)
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
    store = new BlockManager("<driver>", actorSystem, master, serializer, 500)
    store.putSingle("a1", new Array[Byte](1000), StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1") === None, "a1 was in store")
    store.putSingle("a2", new Array[Byte](1000), StorageLevel.MEMORY_AND_DISK)
    assert(store.memoryStore.getValues("a2") === None, "a2 was in memory store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
  }

  test("block compression") {
    try {
      System.setProperty("spark.shuffle.compress", "true")
      store = new BlockManager("exec1", actorSystem, master, serializer, 2000)
      store.putSingle("shuffle_0_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("shuffle_0_0_0") <= 100, "shuffle_0_0_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.shuffle.compress", "false")
      store = new BlockManager("exec2", actorSystem, master, serializer, 2000)
      store.putSingle("shuffle_0_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("shuffle_0_0_0") >= 1000, "shuffle_0_0_0 was compressed")
      store.stop()
      store = null

      System.setProperty("spark.broadcast.compress", "true")
      store = new BlockManager("exec3", actorSystem, master, serializer, 2000)
      store.putSingle("broadcast_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("broadcast_0") <= 100, "broadcast_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.broadcast.compress", "false")
      store = new BlockManager("exec4", actorSystem, master, serializer, 2000)
      store.putSingle("broadcast_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("broadcast_0") >= 1000, "broadcast_0 was compressed")
      store.stop()
      store = null

      System.setProperty("spark.rdd.compress", "true")
      store = new BlockManager("exec5", actorSystem, master, serializer, 2000)
      store.putSingle("rdd_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("rdd_0_0") <= 100, "rdd_0_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.rdd.compress", "false")
      store = new BlockManager("exec6", actorSystem, master, serializer, 2000)
      store.putSingle("rdd_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("rdd_0_0") >= 1000, "rdd_0_0 was compressed")
      store.stop()
      store = null

      // Check that any other block types are also kept uncompressed
      store = new BlockManager("exec7", actorSystem, master, serializer, 2000)
      store.putSingle("other_block", new Array[Byte](1000), StorageLevel.MEMORY_ONLY)
      assert(store.memoryStore.getSize("other_block") >= 1000, "other_block was compressed")
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
    store = new BlockManager("<driver>", actorSystem, master, new JavaSerializer, 1200)

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
}

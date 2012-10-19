package spark.storage

import java.nio.ByteBuffer

import akka.actor._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester

import spark.KryoSerializer
import spark.SizeEstimator
import spark.util.ByteBufferInputStream

class BlockManagerSuite extends FunSuite with BeforeAndAfter with PrivateMethodTester {
  var store: BlockManager = null
  var actorSystem: ActorSystem = null
  var master: BlockManagerMaster = null
  var oldArch: String = null
  var oldOops: String = null
  
  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test 
  val serializer = new KryoSerializer

  before {
    actorSystem = ActorSystem("test")
    master = new BlockManagerMaster(actorSystem, true, true)

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case 
    oldArch = System.setProperty("os.arch", "amd64")
    oldOops = System.setProperty("spark.test.useCompressedOops", "true")
    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  after {
    if (store != null) {
      store.stop()
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

  test("manager-master interaction") {
    store = new BlockManager(master, serializer, 2000)
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
    assert(master.mustGetLocations(GetLocations("a1")).size > 0, "master was not told about a1")
    assert(master.mustGetLocations(GetLocations("a2")).size > 0, "master was not told about a2")
    assert(master.mustGetLocations(GetLocations("a3")).size === 0, "master was told about a3")
    
    // Drop a1 and a2 from memory; this should be reported back to the master
    store.dropFromMemory("a1", null)
    store.dropFromMemory("a2", null)
    assert(store.getSingle("a1") === None, "a1 not removed from store")
    assert(store.getSingle("a2") === None, "a2 not removed from store")
    assert(master.mustGetLocations(GetLocations("a1")).size === 0, "master did not remove a1")
    assert(master.mustGetLocations(GetLocations("a2")).size === 0, "master did not remove a2")
  }

  test("in-memory LRU storage") {
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("rdd_0_1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("rdd_0_3", a3, StorageLevel.MEMORY_ONLY)
    // Even though we accessed rdd_0_3 last, it should not have replaced partitiosn 1 and 2
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 1200)
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
    store = new BlockManager(master, serializer, 500)
    store.putSingle("a1", new Array[Byte](1000), StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1") === None, "a1 was in store")
    store.putSingle("a2", new Array[Byte](1000), StorageLevel.MEMORY_AND_DISK)
    assert(store.memoryStore.getValues("a2") === None, "a2 was in memory store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
  }

  test("block compression") {
    try {
      System.setProperty("spark.shuffle.compress", "true")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("shuffle_0_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("shuffle_0_0_0") <= 100, "shuffle_0_0_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.shuffle.compress", "false")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("shuffle_0_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("shuffle_0_0_0") >= 1000, "shuffle_0_0_0 was compressed")
      store.stop()
      store = null

      System.setProperty("spark.broadcast.compress", "true")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("broadcast_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("broadcast_0") <= 100, "broadcast_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.broadcast.compress", "false")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("broadcast_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("broadcast_0") >= 1000, "broadcast_0 was compressed")
      store.stop()
      store = null

      System.setProperty("spark.rdd.compress", "true")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("rdd_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("rdd_0_0") <= 100, "rdd_0_0 was not compressed")
      store.stop()
      store = null

      System.setProperty("spark.rdd.compress", "false")
      store = new BlockManager(master, serializer, 2000)
      store.putSingle("rdd_0_0", new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize("rdd_0_0") >= 1000, "rdd_0_0 was compressed")
      store.stop()
      store = null

      // Check that any other block types are also kept uncompressed
      store = new BlockManager(master, serializer, 2000)
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
}

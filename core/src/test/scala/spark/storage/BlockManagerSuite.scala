package spark.storage

import spark.KryoSerializer

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

class BlockManagerSuite extends FunSuite with BeforeAndAfter{
  before {
     BlockManagerMaster.startBlockManagerMaster(true, true)
  }

  test("in-memory LRU storage") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_DESER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_DESER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY_DESER)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    Thread.sleep(100)
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_DESER)
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    Thread.sleep(100)
    assert(store.getSingle("a3") == None, "a3 was in store")
  }
  
  test("in-memory LRU storage with serialization") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    Thread.sleep(100)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_DESER)
    Thread.sleep(100)
    assert(store.getSingle("a1") != None, "a1 was not in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") == None, "a1 was in store")
  }

  test("on-disk storage") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    store.putSingle("a2", a2, StorageLevel.DISK_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") != None, "a1 was not in store")
  }

  test("disk and memory storage") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_AND_MEMORY_DESER)
    store.putSingle("a2", a2, StorageLevel.DISK_AND_MEMORY_DESER)
    store.putSingle("a3", a3, StorageLevel.DISK_AND_MEMORY_DESER)
    Thread.sleep(100)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") != None, "a1 was not in store")
  }

  test("disk and memory storage with serialization") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_AND_MEMORY)
    store.putSingle("a2", a2, StorageLevel.DISK_AND_MEMORY)
    store.putSingle("a3", a3, StorageLevel.DISK_AND_MEMORY)
    Thread.sleep(100)
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a1") != None, "a1 was not in store")
  }

  test("LRU with mixed storage levels") {
    val store = new BlockManager(1000, new KryoSerializer)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    val a4 = new Array[Byte](400)
    // First store a1 and a2, both in memory, and a3, on disk only
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    // At this point LRU should not kick in because a3 is only on disk
    assert(store.getSingle("a1") != None, "a2 was not in store")
    assert(store.getSingle("a2") != None, "a3 was not in store")
    assert(store.getSingle("a3") != None, "a1 was not in store")
    assert(store.getSingle("a1") != None, "a2 was not in store")
    assert(store.getSingle("a2") != None, "a3 was not in store")
    assert(store.getSingle("a3") != None, "a1 was not in store")
    // Now let's add in a4, which uses both disk and memory; a1 should drop out
    store.putSingle("a4", a4, StorageLevel.DISK_AND_MEMORY)
    Thread.sleep(100)
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2") != None, "a2 was not in store")
    assert(store.getSingle("a3") != None, "a3 was not in store")
    assert(store.getSingle("a4") != None, "a4 was not in store")
  }

  test("in-memory LRU with streams") {
    val store = new BlockManager(1000, new KryoSerializer)
    val list1 = List(new Array[Byte](200), new Array[Byte](200))
    val list2 = List(new Array[Byte](200), new Array[Byte](200))
    val list3 = List(new Array[Byte](200), new Array[Byte](200))
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY_DESER)
    store.put("list2", list2.iterator, StorageLevel.MEMORY_ONLY_DESER)
    store.put("list3", list3.iterator, StorageLevel.MEMORY_ONLY_DESER)
    Thread.sleep(100)
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") != None, "list3 was not in store")
    assert(store.get("list3").get.size == 2)
    assert(store.get("list1") == None, "list1 was in store")
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    // At this point list2 was gotten last, so LRU will getSingle rid of list3
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY_DESER)
    Thread.sleep(100)
    assert(store.get("list1") != None, "list1 was not in store")
    assert(store.get("list1").get.size == 2)
    assert(store.get("list2") != None, "list2 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") == None, "list1 was in store")
  }

  test("LRU with mixed storage levels and streams") {
    val store = new BlockManager(1000, new KryoSerializer)
    val list1 = List(new Array[Byte](200), new Array[Byte](200))
    val list2 = List(new Array[Byte](200), new Array[Byte](200))
    val list3 = List(new Array[Byte](200), new Array[Byte](200))
    val list4 = List(new Array[Byte](200), new Array[Byte](200))
    // First store list1 and list2, both in memory, and list3, on disk only
    store.put("list1", list1.iterator, StorageLevel.MEMORY_ONLY)
    store.put("list2", list2.iterator, StorageLevel.MEMORY_ONLY)
    store.put("list3", list3.iterator, StorageLevel.DISK_ONLY)
    Thread.sleep(100)
    // At this point LRU should not kick in because list3 is only on disk
    assert(store.get("list1") != None, "list2 was not in store")
    assert(store.get("list1").get.size == 2)
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size == 2)
    assert(store.get("list1") != None, "list2 was not in store")
    assert(store.get("list1").get.size == 2)
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size == 2)
    // Now let's add in list4, which uses both disk and memory; list1 should drop out
    store.put("list4", list4.iterator, StorageLevel.DISK_AND_MEMORY)
    assert(store.get("list1") == None, "list1 was in store")
    assert(store.get("list2") != None, "list3 was not in store")
    assert(store.get("list2").get.size == 2)
    assert(store.get("list3") != None, "list1 was not in store")
    assert(store.get("list3").get.size == 2)
    assert(store.get("list4") != None, "list4 was not in store")
    assert(store.get("list4").get.size == 2)
  } 
}

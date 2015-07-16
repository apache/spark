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

package org.apache.spark.ui.storage

import scala.xml.Utility

import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage._

class StoragePageSuite extends SparkFunSuite {

  val storageTab = mock(classOf[StorageTab])
  when(storageTab.basePath).thenReturn("http://localhost:4040")
  val storagePage = new StoragePage(storageTab)

  test("rddTable") {
    val rdd1 = new RDDInfo(1,
      "rdd1",
      10,
      StorageLevel.MEMORY_ONLY,
      Seq.empty)
    rdd1.memSize = 100
    rdd1.numCachedPartitions = 10

    val rdd2 = new RDDInfo(2,
      "rdd2",
      10,
      StorageLevel.DISK_ONLY,
      Seq.empty)
    rdd2.diskSize = 200
    rdd2.numCachedPartitions = 5

    val rdd3 = new RDDInfo(3,
      "rdd3",
      10,
      StorageLevel.MEMORY_AND_DISK_SER,
      Seq.empty)
    rdd3.memSize = 400
    rdd3.diskSize = 500
    rdd3.numCachedPartitions = 10

    val xmlNodes = storagePage.rddTable(Seq(rdd1, rdd2, rdd3))

    val headers = Seq(
      "RDD Name",
      "Storage Level",
      "Cached Partitions",
      "Fraction Cached",
      "Size in Memory",
      "Size in ExternalBlockStore",
      "Size on Disk")
    assert((xmlNodes \\ "th").map(_.text) === headers)

    assert((xmlNodes \\ "tr").size === 3)
    assert(((xmlNodes \\ "tr")(0) \\ "td").map(_.text.trim) ===
      Seq("rdd1", "Memory Deserialized 1x Replicated", "10", "100%", "100.0 B", "0.0 B", "0.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(0) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd?id=1"))

    assert(((xmlNodes \\ "tr")(1) \\ "td").map(_.text.trim) ===
      Seq("rdd2", "Disk Serialized 1x Replicated", "5", "50%", "0.0 B", "0.0 B", "200.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(1) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd?id=2"))

    assert(((xmlNodes \\ "tr")(2) \\ "td").map(_.text.trim) ===
      Seq("rdd3", "Disk Memory Serialized 1x Replicated", "10", "100%", "400.0 B", "0.0 B",
        "500.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(2) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd?id=3"))
  }

  test("empty rddTable") {
    assert(storagePage.rddTable(Seq.empty).isEmpty)
  }

  test("streamBlockStorageLevelDescriptionAndSize") {
    val memoryBlock = BlockUIData(StreamBlockId(0, 0),
      "localhost:1111",
      StorageLevel.MEMORY_ONLY,
      memSize = 100,
      diskSize = 0,
      externalBlockStoreSize = 0)
    assert(("Memory", 100) === storagePage.streamBlockStorageLevelDescriptionAndSize(memoryBlock))

    val memorySerializedBlock = BlockUIData(StreamBlockId(0, 0),
      "localhost:1111",
      StorageLevel.MEMORY_ONLY_SER,
      memSize = 100,
      diskSize = 0,
      externalBlockStoreSize = 0)
    assert(("Memory Serialized", 100) ===
      storagePage.streamBlockStorageLevelDescriptionAndSize(memorySerializedBlock))

    val diskBlock = BlockUIData(StreamBlockId(0, 0),
      "localhost:1111",
      StorageLevel.DISK_ONLY,
      memSize = 0,
      diskSize = 100,
      externalBlockStoreSize = 0)
    assert(("Disk", 100) === storagePage.streamBlockStorageLevelDescriptionAndSize(diskBlock))

    val externalBlock = BlockUIData(StreamBlockId(0, 0),
      "localhost:1111",
      StorageLevel.OFF_HEAP,
      memSize = 0,
      diskSize = 0,
      externalBlockStoreSize = 100)
    assert(("External", 100) ===
      storagePage.streamBlockStorageLevelDescriptionAndSize(externalBlock))
  }

  test("receiverBlockTables") {
    val blocksForExecutor0 = Seq(
      BlockUIData(StreamBlockId(0, 0),
        "localhost:10000",
        StorageLevel.MEMORY_ONLY,
        memSize = 100,
        diskSize = 0,
        externalBlockStoreSize = 0),
      BlockUIData(StreamBlockId(1, 1),
        "localhost:10000",
        StorageLevel.DISK_ONLY,
        memSize = 0,
        diskSize = 100,
        externalBlockStoreSize = 0)
    )
    val executor0 = ExecutorStreamBlockStatus("0", "localhost:10000", blocksForExecutor0)

    val blocksForExecutor1 = Seq(
      BlockUIData(StreamBlockId(0, 0),
        "localhost:10001",
        StorageLevel.MEMORY_ONLY,
        memSize = 100,
        diskSize = 0,
        externalBlockStoreSize = 0),
      BlockUIData(StreamBlockId(2, 2),
        "localhost:10001",
        StorageLevel.OFF_HEAP,
        memSize = 0,
        diskSize = 0,
        externalBlockStoreSize = 200),
      BlockUIData(StreamBlockId(1, 1),
        "localhost:10001",
        StorageLevel.MEMORY_ONLY_SER,
        memSize = 100,
        diskSize = 0,
        externalBlockStoreSize = 0)
    )
    val executor1 = ExecutorStreamBlockStatus("1", "localhost:10001", blocksForExecutor1)
    val xmlNodes = storagePage.receiverBlockTables(Seq(executor0, executor1))

    val executorTable = (xmlNodes \\ "table")(0)
    val executorHeaders = Seq(
      "Executor ID",
      "Address",
      "Total Size in Memory",
      "Total Size in ExternalBlockStore",
      "Total Size on Disk",
      "Stream Blocks")
    assert((executorTable \\ "th").map(_.text) === executorHeaders)

    assert((executorTable \\ "tr").size === 2)
    assert(((executorTable \\ "tr")(0) \\ "td").map(_.text.trim) ===
      Seq("0", "localhost:10000", "100.0 B", "0.0 B", "100.0 B", "2"))
    assert(((executorTable \\ "tr")(1) \\ "td").map(_.text.trim) ===
      Seq("1", "localhost:10001", "200.0 B", "200.0 B", "0.0 B", "3"))

    val blockTable = (xmlNodes \\ "table")(1)
    val blockHeaders = Seq(
      "Block ID",
      "Replication Level",
      "Location",
      "Storage Level",
      "Size")
    assert((blockTable \\ "th").map(_.text) === blockHeaders)

    assert((blockTable \\ "tr").size === 5)
    assert(((blockTable \\ "tr")(0) \\ "td").map(_.text.trim) ===
      Seq("input-0-0", "2", "localhost:10000", "Memory", "100.0 B"))
    // Check "rowspan=2" for the first 2 columns
    assert(((blockTable \\ "tr")(0) \\ "td")(0).attribute("rowspan").map(_.text) === Some("2"))
    assert(((blockTable \\ "tr")(0) \\ "td")(1).attribute("rowspan").map(_.text) === Some("2"))

    assert(((blockTable \\ "tr")(1) \\ "td").map(_.text.trim) ===
      Seq("localhost:10001", "Memory", "100.0 B"))

    assert(((blockTable \\ "tr")(2) \\ "td").map(_.text.trim) ===
      Seq("input-1-1", "2", "localhost:10000", "Disk", "100.0 B"))
    // Check "rowspan=2" for the first 2 columns
    assert(((blockTable \\ "tr")(2) \\ "td")(0).attribute("rowspan").map(_.text) === Some("2"))
    assert(((blockTable \\ "tr")(2) \\ "td")(1).attribute("rowspan").map(_.text) === Some("2"))

    assert(((blockTable \\ "tr")(3) \\ "td").map(_.text.trim) ===
      Seq("localhost:10001", "Memory Serialized", "100.0 B"))

    assert(((blockTable \\ "tr")(4) \\ "td").map(_.text.trim) ===
      Seq("input-2-2", "1", "localhost:10001", "External", "200.0 B"))
    // Check "rowspan=1" for the first 2 columns
    assert(((blockTable \\ "tr")(4) \\ "td")(0).attribute("rowspan").map(_.text) === Some("1"))
    assert(((blockTable \\ "tr")(4) \\ "td")(1).attribute("rowspan").map(_.text) === Some("1"))
  }

  test("empty receiverBlockTables") {
    assert(storagePage.receiverBlockTables(Seq.empty).isEmpty)

    val executor0 = ExecutorStreamBlockStatus("0", "localhost:10000", Seq.empty)
    val executor1 = ExecutorStreamBlockStatus("1", "localhost:10001", Seq.empty)
    assert(storagePage.receiverBlockTables(Seq(executor0, executor1)).isEmpty)
  }
}

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

import javax.servlet.http.HttpServletRequest

import org.mockito.Mockito._
import scala.xml.{Node, Text}

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.StreamBlockData
import org.apache.spark.status.api.v1.RDDStorageInfo
import org.apache.spark.storage._

class StoragePageSuite extends SparkFunSuite {

  val storageTab = mock(classOf[StorageTab])
  when(storageTab.basePath).thenReturn("http://localhost:4040")
  val storagePage = new StoragePage(storageTab, null)
  val request = mock(classOf[HttpServletRequest])

  test("rddTable") {
    val rdd1 = new RDDStorageInfo(1,
      "rdd1",
      10,
      10,
      StorageLevel.MEMORY_ONLY.description,
      100L,
      0L,
      None,
      None)

    val rdd2 = new RDDStorageInfo(2,
      "rdd2",
      10,
      5,
      StorageLevel.DISK_ONLY.description,
      0L,
      200L,
      None,
      None)

    val rdd3 = new RDDStorageInfo(3,
      "rdd3",
      10,
      10,
      StorageLevel.MEMORY_AND_DISK_SER.description,
      400L,
      500L,
      None,
      None)

    val xmlNodes = storagePage.rddTable(request, Seq(rdd1, rdd2, rdd3))

    val headers = Seq(
      "ID",
      "RDD Name",
      "Storage Level",
      "Cached Partitions",
      "Fraction Cached",
      "Size in Memory",
      "Size on Disk")

    val headerRow: Seq[Node] = {
      headers.view.zipWithIndex.map { x =>
        storagePage.tooltips(x._2) match {
          case Some(tooltip) =>
            <th width={""} class={""}>
              <span data-toggle="tooltip" title={tooltip}>
                {Text(x._1)}
              </span>
            </th>
          case None => <th width={""} class={""}>{Text(x._1)}</th>
        }
      }.toList
    }
    assert((xmlNodes \\ "th").map(_.text) === headerRow.map(_.text))

    assert((xmlNodes \\ "tr").size === 3)
    assert(((xmlNodes \\ "tr")(0) \\ "td").map(_.text.trim) ===
      Seq("1", "rdd1", "Memory Deserialized 1x Replicated", "10", "100%", "100.0 B", "0.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(0) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd/?id=1"))

    assert(((xmlNodes \\ "tr")(1) \\ "td").map(_.text.trim) ===
      Seq("2", "rdd2", "Disk Serialized 1x Replicated", "5", "50%", "0.0 B", "200.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(1) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd/?id=2"))

    assert(((xmlNodes \\ "tr")(2) \\ "td").map(_.text.trim) ===
      Seq("3", "rdd3", "Disk Memory Serialized 1x Replicated", "10", "100%", "400.0 B", "500.0 B"))
    // Check the url
    assert(((xmlNodes \\ "tr")(2) \\ "td" \ "a")(0).attribute("href").map(_.text) ===
      Some("http://localhost:4040/storage/rdd/?id=3"))
  }

  test("empty rddTable") {
    assert(storagePage.rddTable(request, Seq.empty).isEmpty)
  }

  test("streamBlockStorageLevelDescriptionAndSize") {
    val memoryBlock = new StreamBlockData("0",
      "0",
      "localhost:1111",
      StorageLevel.MEMORY_ONLY.description,
      true,
      false,
      true,
      100,
      0)
    assert(("Memory", 100) === storagePage.streamBlockStorageLevelDescriptionAndSize(memoryBlock))

    val memorySerializedBlock = new StreamBlockData("0",
      "0",
      "localhost:1111",
      StorageLevel.MEMORY_ONLY_SER.description,
      true,
      false,
      false,
      memSize = 100,
      diskSize = 0)
    assert(("Memory Serialized", 100) ===
      storagePage.streamBlockStorageLevelDescriptionAndSize(memorySerializedBlock))

    val diskBlock = new StreamBlockData("0",
      "0",
      "localhost:1111",
      StorageLevel.DISK_ONLY.description,
      false,
      true,
      false,
      0,
      100)
    assert(("Disk", 100) === storagePage.streamBlockStorageLevelDescriptionAndSize(diskBlock))
  }

  test("receiverBlockTables") {
    val blocksForExecutor0 = Seq(
      new StreamBlockData(StreamBlockId(0, 0).name,
        "0",
        "localhost:10000",
        StorageLevel.MEMORY_ONLY.description,
        true,
        false,
        true,
        100,
        0),
      new StreamBlockData(StreamBlockId(1, 1).name,
        "0",
        "localhost:10000",
        StorageLevel.DISK_ONLY.description,
        false,
        true,
        false,
        0,
        100)
    )

    val blocksForExecutor1 = Seq(
      new StreamBlockData(StreamBlockId(0, 0).name,
        "1",
        "localhost:10001",
        StorageLevel.MEMORY_ONLY.description,
        true,
        false,
        true,
        memSize = 100,
        diskSize = 0),
      new StreamBlockData(StreamBlockId(1, 1).name,
        "1",
        "localhost:10001",
        StorageLevel.MEMORY_ONLY_SER.description,
        true,
        false,
        false,
        100,
        0)
    )

    val xmlNodes = storagePage.receiverBlockTables(blocksForExecutor0 ++ blocksForExecutor1)

    val executorTable = (xmlNodes \\ "table")(0)
    val executorHeaders = Seq(
      "Executor ID",
      "Address",
      "Total Size in Memory",
      "Total Size on Disk",
      "Stream Blocks")
    assert((executorTable \\ "th").map(_.text) === executorHeaders)

    assert((executorTable \\ "tr").size === 2)
    assert(((executorTable \\ "tr")(0) \\ "td").map(_.text.trim) ===
      Seq("0", "localhost:10000", "100.0 B", "100.0 B", "2"))
    assert(((executorTable \\ "tr")(1) \\ "td").map(_.text.trim) ===
      Seq("1", "localhost:10001", "200.0 B", "0.0 B", "2"))

    val blockTable = (xmlNodes \\ "table")(1)
    val blockHeaders = Seq(
      "Block ID",
      "Replication Level",
      "Location",
      "Storage Level",
      "Size")
    assert((blockTable \\ "th").map(_.text) === blockHeaders)

    assert((blockTable \\ "tr").size === 4)
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
  }

  test("empty receiverBlockTables") {
    assert(storagePage.receiverBlockTables(Seq.empty).isEmpty)
  }

}

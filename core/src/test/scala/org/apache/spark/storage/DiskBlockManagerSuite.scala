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

import java.io.{FileWriter, File}

import scala.collection.mutable

import com.google.common.io.Files
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class DiskBlockManagerSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val rootDir0 = Files.createTempDir()
  rootDir0.deleteOnExit()
  val rootDir1 = Files.createTempDir()
  rootDir1.deleteOnExit()
  val rootDirs = rootDir0.getName + "," + rootDir1.getName
  println("Created root dirs: " + rootDirs)

  // This suite focuses primarily on consolidation features,
  // so we coerce consolidation if not already enabled.
  val consolidateProp = "spark.shuffle.consolidateFiles"
  val oldConsolidate = Option(System.getProperty(consolidateProp))
  System.setProperty(consolidateProp, "true")

  val shuffleBlockManager = new ShuffleBlockManager(null) {
    var idToSegmentMap = mutable.Map[ShuffleBlockId, FileSegment]()
    override def getBlockLocation(id: ShuffleBlockId) = idToSegmentMap(id)
  }

  var diskBlockManager: DiskBlockManager = _

  override def afterAll() {
    oldConsolidate.map(c => System.setProperty(consolidateProp, c))
  }

  override def beforeEach() {
    diskBlockManager = new DiskBlockManager(shuffleBlockManager, rootDirs)
    shuffleBlockManager.idToSegmentMap.clear()
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    assertSegmentEquals(blockId, blockId.name, 0, 0)

    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assertSegmentEquals(blockId, blockId.name, 0, 10)

    newFile.delete()
  }

  test("block appending") {
    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 15)
    assertSegmentEquals(blockId, blockId.name, 0, 15)
    val newFile2 = diskBlockManager.getFile(blockId)
    assert(newFile === newFile2)
    writeToFile(newFile2, 12)
    assertSegmentEquals(blockId, blockId.name, 0, 27)
    newFile.delete()
  }

  test("block remapping") {
    val filename = "test"
    val blockId0 = new ShuffleBlockId(1, 2, 3)
    val newFile = diskBlockManager.getFile(filename)
    writeToFile(newFile, 15)
    shuffleBlockManager.idToSegmentMap(blockId0) = new FileSegment(newFile, 0, 15)
    assertSegmentEquals(blockId0, filename, 0, 15)

    val blockId1 = new ShuffleBlockId(1, 2, 4)
    val newFile2 = diskBlockManager.getFile(filename)
    writeToFile(newFile2, 12)
    shuffleBlockManager.idToSegmentMap(blockId1) = new FileSegment(newFile, 15, 12)
    assertSegmentEquals(blockId1, filename, 15, 12)

    assert(newFile === newFile2)
    newFile.delete()
  }

  def assertSegmentEquals(blockId: BlockId, filename: String, offset: Int, length: Int) {
    val segment = diskBlockManager.getBlockLocation(blockId)
    assert(segment.file.getName === filename)
    assert(segment.offset === offset)
    assert(segment.length === length)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}

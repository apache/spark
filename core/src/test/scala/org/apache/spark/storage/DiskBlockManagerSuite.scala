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

import java.io.{File, FileWriter}

import org.apache.spark.shuffle.hash.HashShuffleManager

import scala.collection.mutable
import scala.language.reflectiveCalls

import akka.actor.Props
import com.google.common.io.Files
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.executor.ShuffleWriteMetrics

class DiskBlockManagerSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val testConf = new SparkConf(false)
  private var rootDir0: File = _
  private var rootDir1: File = _
  private var rootDirs: String = _

  // This suite focuses primarily on consolidation features,
  // so we coerce consolidation if not already enabled.
  testConf.set("spark.shuffle.consolidateFiles", "true")

  private val shuffleManager = new HashShuffleManager(testConf.clone)

  val shuffleBlockManager = new ShuffleBlockManager(null, shuffleManager) {
    override def conf = testConf.clone
    var idToSegmentMap = mutable.Map[ShuffleBlockId, FileSegment]()
    override def getBlockLocation(id: ShuffleBlockId) = idToSegmentMap(id)
  }

  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDir0 = Files.createTempDir()
    rootDir0.deleteOnExit()
    rootDir1 = Files.createTempDir()
    rootDir1.deleteOnExit()
    rootDirs = rootDir0.getAbsolutePath + "," + rootDir1.getAbsolutePath
    println("Created root dirs: " + rootDirs)
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(rootDir0)
    Utils.deleteRecursively(rootDir1)
  }

  override def beforeEach() {
    val conf = testConf.clone
    conf.set("spark.local.dir", rootDirs)
    diskBlockManager = new DiskBlockManager(shuffleBlockManager, conf)
    shuffleBlockManager.idToSegmentMap.clear()
  }

  override def afterEach() {
    diskBlockManager.stop()
    shuffleBlockManager.idToSegmentMap.clear()
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    assertSegmentEquals(blockId, blockId.name, 0, 0)

    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assertSegmentEquals(blockId, blockId.name, 0, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("enumerating blocks") {
    val ids = (1 to 100).map(i => TestBlockId("test_" + i))
    val files = ids.map(id => diskBlockManager.getFile(id))
    files.foreach(file => writeToFile(file, 10))
    assert(diskBlockManager.getAllBlocks.toSet === ids.toSet)
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

  private def checkSegments(segment1: FileSegment, segment2: FileSegment) {
    assert (segment1.file.getCanonicalPath === segment2.file.getCanonicalPath)
    assert (segment1.offset === segment2.offset)
    assert (segment1.length === segment2.length)
  }

  test("consolidated shuffle can write to shuffle group without messing existing offsets/lengths") {

    val serializer = new JavaSerializer(testConf)
    val confCopy = testConf.clone
    // reset after EACH object write. This is to ensure that there are bytes appended after
    // an object is written. So if the codepaths assume writeObject is end of data, this should
    // flush those bugs out. This was common bug in ExternalAppendOnlyMap, etc.
    confCopy.set("spark.serializer.objectStreamReset", "1")

    val securityManager = new org.apache.spark.SecurityManager(confCopy)
    // Do not use the shuffleBlockManager above !
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("test", "localhost", 0, confCopy,
      securityManager)
    val master = new BlockManagerMaster(
      actorSystem.actorOf(Props(new BlockManagerMasterActor(true, confCopy, new LiveListenerBus))),
      confCopy)
    val store = new BlockManager("<driver>", actorSystem, master , serializer, confCopy,
      securityManager, null, shuffleManager)

    try {

      val shuffleManager = store.shuffleBlockManager

      val shuffle1 = shuffleManager.forMapTask(1, 1, 1, serializer, new ShuffleWriteMetrics)
      for (writer <- shuffle1.writers) {
        writer.write("test1")
        writer.write("test2")
      }
      for (writer <- shuffle1.writers) {
        writer.commitAndClose()
      }

      val shuffle1Segment = shuffle1.writers(0).fileSegment()
      shuffle1.releaseWriters(success = true)

      val shuffle2 = shuffleManager.forMapTask(1, 2, 1, new JavaSerializer(testConf),
        new ShuffleWriteMetrics)

      for (writer <- shuffle2.writers) {
        writer.write("test3")
        writer.write("test4")
      }
      for (writer <- shuffle2.writers) {
        writer.commitAndClose()
      }
      val shuffle2Segment = shuffle2.writers(0).fileSegment()
      shuffle2.releaseWriters(success = true)

      // Now comes the test :
      // Write to shuffle 3; and close it, but before registering it, check if the file lengths for
      // previous task (forof shuffle1) is the same as 'segments'. Earlier, we were inferring length
      // of block based on remaining data in file : which could mess things up when there is concurrent read
      // and writes happening to the same shuffle group.

      val shuffle3 = shuffleManager.forMapTask(1, 3, 1, new JavaSerializer(testConf),
        new ShuffleWriteMetrics)
      for (writer <- shuffle3.writers) {
        writer.write("test3")
        writer.write("test4")
      }
      for (writer <- shuffle3.writers) {
        writer.commitAndClose()
      }
      // check before we register.
      checkSegments(shuffle2Segment, shuffleManager.getBlockLocation(ShuffleBlockId(1, 2, 0)))
      shuffle3.releaseWriters(success = true)
      checkSegments(shuffle2Segment, shuffleManager.getBlockLocation(ShuffleBlockId(1, 2, 0)))
      shuffleManager.removeShuffle(1)
    } finally {

      if (store != null) {
        store.stop()
      }
      actorSystem.shutdown()
      actorSystem.awaitTermination()
    }
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

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

import scala.collection.mutable
import scala.language.reflectiveCalls

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.{SparkConfWithEnv, Utils}

object DiskBlockManagerTieredSuite {
  val SPARK_DIRS_TIERS_CONF = "000011112222"
  case class mockDiskAndTier(diskSpaceStatus: Array[Boolean], expectedTiers: Array[Int])
  val mockTestsSeq = Array(
    mockDiskAndTier(Array(true, true, true), Array(0, 0, 0)),
    mockDiskAndTier(Array(true, true, false, true, false, false, true, false, false, false),
      Array(0, 0, 1, 2, 2)),
    mockDiskAndTier(Array(false, false, false, false, false, false, false, false, false),
      Array(2, 2, 2)),
    mockDiskAndTier(Array(true, false, true, false, false, true,
      false, false, true, true, false, true, true, false, false, false),
      Array(0, 1, 2, 2, 0, 1, 0, 2))
  )

  // this class override hasEnoughSpace in the TieredAllocator to mock disk space change
  class MockTieredAllocator(
      conf: SparkConf,
      localDirs: Array[File],
      subDirsPerLocalDir: Int)
    extends TieredAllocator(
      conf,
      localDirs,
      subDirsPerLocalDir) {
    private val caseNum = conf.getInt("spark.testTieredStorage.caseNum", 0)
    private val diskStatus = mockTestsSeq(caseNum).diskSpaceStatus

    private var index = -1
    override def hasEnoughSpace(file: File): Boolean = {
      index += 1
      if (index < diskStatus.length) {
        diskStatus(index)
      }
      else {
        true
      }
    }
  }
}

class DiskBlockManagerTieredSuite
  extends SparkFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  import org.apache.spark.storage.DiskBlockManagerTieredSuite._

  private val testConf = new SparkConfWithEnv(Map("SPARK_DIRS_TIERS" -> SPARK_DIRS_TIERS_CONF))
  private var rootDirs: Seq[File] = _

  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDirs = (0 until SPARK_DIRS_TIERS_CONF.length).map(_ => Utils.createTempDir())
    testConf.set("spark.local.dir", rootDirs.map(_.getAbsolutePath).mkString(","))
  }

  override def afterAll() {
    try {
      rootDirs.foreach(Utils.deleteRecursively)
    } finally {
      super.afterAll()
    }
  }

  override def afterEach() {
    try {
      diskBlockManager.stop()
    } finally {
      super.afterEach()
    }
  }

  def testTieredStorage(caseNum: Int): Unit = {
    testConf.set("spark.diskStore.allocator", classOf[MockTieredAllocator].getName)
      .set("spark.testTieredStorage.caseNum", caseNum.toString)
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)

    val createdBlocks = mutable.ArrayBuffer[TestBlockId]()
    val rootDirsPath = rootDirs.map(_.getCanonicalPath)
    val tierSeq = mockTestsSeq(caseNum).expectedTiers
    val tiersConf = SPARK_DIRS_TIERS_CONF
    assert(!tierSeq.exists(s => s < 0 || s > tiersConf(tiersConf.length - 1).toInt))
    tierSeq.zipWithIndex.foreach {
      case (i, idx) =>
        val blockId = TestBlockId("test" + idx)
        createdBlocks += blockId
        val file = diskBlockManager.getFile(blockId)
        writeToFile(file, 10)
        val path = rootDirsPath.find(file.getCanonicalPath.startsWith(_))
        val j = rootDirsPath.indexOf(path.get) / 4
        assert(i == j)
    }

    createdBlocks.foreach {
      b => assert(diskBlockManager.containsBlock(b))
    }
  }

  test("basic block creation with hash allocator") {
    testConf.set("spark.diskStore.allocator", "hash")
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
    val blockId = TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("basic block creation with tiered allocator") {
    testConf.set("spark.diskStore.allocator", "tiered")
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
    // repeat 20 times and make sure the blocks are only allocated in tie 0
    (0 until 100).foreach {
      i =>
        val blockId = TestBlockId("test" + i)
        val file = diskBlockManager.getFile(blockId)
        val rootDirsPath = rootDirs.map(_.getCanonicalPath)
        val path = rootDirsPath.find(file.getCanonicalPath.startsWith(_))
        val j = rootDirsPath.indexOf(path.get) / 4
        assert(j == 0)
        writeToFile(file, 10)
        assert(diskBlockManager.containsBlock(blockId))
        file.delete()
        assert(!diskBlockManager.containsBlock(blockId))
    }
  }

  test("tiered storage: enough disk space, all in tier 0") {
    testTieredStorage(0)
  }

  test("tiered storage: in all ties including 0, 1, 2...") {
    testTieredStorage(1)
  }

  test("tiered storage: no disk space, all in the last tier") {
    testTieredStorage(2)
  }

  test("tiered storage: disks space keep changing") {
    testTieredStorage(3)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}

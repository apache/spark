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

import java.io.{File, FileWriter, IOException}

import scala.language.reflectiveCalls

import org.mockito.Matchers.{eq => meq}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkFunSuite}

class DiskBlockManagerSuite extends SparkFunSuite
    with BeforeAndAfterEach with BeforeAndAfterAll with PrivateMethodTester {
  private val testConf = new SparkConf(false)
  private var rootDir0: File = _
  private var rootDir1: File = _

  val blockManager = mock(classOf[BlockManager])
  when(blockManager.conf).thenReturn(testConf)
  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDir0 = Utils.createTempDir()
    rootDir1 = Utils.createTempDir()
    testConf.set("spark.local.dir", rootDir0.getAbsolutePath + "," + rootDir1.getAbsolutePath)
  }

  override def afterAll() {
    super.afterAll()
    Utils.deleteRecursively(rootDir0)
    Utils.deleteRecursively(rootDir1)
  }

  override def beforeEach() {
    diskBlockManager = new DiskBlockManager(blockManager, testConf.clone)
  }

  override def afterEach() {
    diskBlockManager.stop()
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
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

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }

  test("bypassing network access") {
    val mockBlockManagerMaster = mock(classOf[BlockManagerMaster])
    val mockBlockManager = mock(classOf[BlockManager])

    // Assume two executors in an identical host
    val localBmId1 = BlockManagerId("test-exec1", "test-client1", 1)
    val localBmId2 = BlockManagerId("test-exec2", "test-client1", 2)

    // Assume that localBmId2 holds 'shuffle_1_0_0'
    val blockIdInLocalBmId2 = ShuffleBlockId(1, 0, 0)
    val tempDir = Utils.createTempDir()
    try {
      // Create mock classes for testing
      when(mockBlockManagerMaster.getLocalDirsPath(meq(localBmId1)))
        .thenReturn(Map(localBmId2 -> Array(tempDir.getAbsolutePath)))
      when(mockBlockManager.conf).thenReturn(testConf)
      when(mockBlockManager.master).thenReturn(mockBlockManagerMaster)
      when(mockBlockManager.blockManagerId).thenReturn(localBmId1)

      val testDiskBlockManager = new DiskBlockManager(mockBlockManager, testConf.clone)

      val getBlockDir: String => File = (s: String) => {
        val (_, subDirId) = testDiskBlockManager.getDirInfo(s, 1)
        new File(tempDir, "%02x".format(subDirId))
      }

      // Create a dummy file for a shuffle block
      val blockDir = getBlockDir(blockIdInLocalBmId2.name)
      assert(blockDir.mkdir())
      val dummyBlockFile = new File(blockDir, blockIdInLocalBmId2.name)
      assert(dummyBlockFile.createNewFile())

      val file = testDiskBlockManager.getFile(
        blockIdInLocalBmId2, localBmId2)
      assert(dummyBlockFile.getName === file.getName)
      assert(dummyBlockFile.toString.contains(tempDir.toString))

      verify(mockBlockManagerMaster, times(1)).getLocalDirsPath(meq(localBmId1))
      verify(mockBlockManager, times(1)).conf
      verify(mockBlockManager, times(1)).master
      verify(mockBlockManager, times(3)).blockManagerId

      // Throw an IOException if given shuffle file not found
      val blockIdNotInLocalBmId2 = ShuffleBlockId(2, 0, 0)
      val errMsg = intercept[IOException] {
        testDiskBlockManager.getFile(blockIdNotInLocalBmId2, localBmId2)
      }
      assert(errMsg.getMessage contains s"File '${getBlockDir(blockIdNotInLocalBmId2.name)}/" +
        s"${blockIdNotInLocalBmId2}' not found in local dir")
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

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

import scala.language.reflectiveCalls

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.{SparkConfWithEnv, Utils}


class DiskBlockManagerSuite extends SparkFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val testConf = new SparkConf(false)
  private var rootDirs: Seq[File] = _

  var diskBlockManager: DiskBlockManager = _

  override def beforeAll() {
    super.beforeAll()
    rootDirs = (0 until 4).map(_ => Utils.createTempDir())
    testConf.set("spark.local.dir", rootDirs.map(_.getAbsolutePath).mkString(","))
  }

  override def afterAll() {
    try {
      rootDirs.foreach(Utils.deleteRecursively)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach() {
    super.beforeEach()
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
  }

  override def afterEach() {
    try {
      diskBlockManager.stop()
    } finally {
      super.afterEach()
    }
  }

  test("basic block creation") {
    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("basic block creation using TieredAllocator") {
    val conf = new SparkConfWithEnv(Map("SPARK_DIRS_TIERS" -> "0000"))
    conf.setAll(testConf.getAll)
    conf.set("spark.diskStore.allocator", "TieredAllocator")
    diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)

    val blockId = new TestBlockId("test")
    val newFile = diskBlockManager.getFile(blockId)
    writeToFile(newFile, 10)
    assert(diskBlockManager.containsBlock(blockId))
    newFile.delete()
    assert(!diskBlockManager.containsBlock(blockId))
  }

  test("dev_testing") {
    class TieredTestAllocator(
        conf: SparkConf,
        localDirs: Array[File],
        subDirsPerLocalDir: Int)
      extends TieredAllocator(
        conf,
        localDirs,
        subDirsPerLocalDir) {
      override def hasEnoughSpace(file: File): Boolean = {
        // scalastyle:off
        println("in testing")
        // scalastyle:on
        false
      }
    }
    val conf = new SparkConfWithEnv(Map("SPARK_DIRS_TIERS" -> "0000"))
    conf.setAll(testConf.getAll)
    conf.set("spark.diskStore.allocator", "org.apache.spark.storage.TieredAllocator")
    diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)

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
}

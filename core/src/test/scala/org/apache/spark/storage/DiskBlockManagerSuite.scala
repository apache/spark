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
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.util.HashMap

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.Utils


class DiskBlockManagerSuite extends SparkFunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  private val testConf = new SparkConf(false)
  private var rootDir0: File = _
  private var rootDir1: File = _
  private var rootDirs: String = _

  var diskBlockManager: DiskBlockManager = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rootDir0 = Utils.createTempDir()
    rootDir1 = Utils.createTempDir()
    rootDirs = rootDir0.getAbsolutePath + "," + rootDir1.getAbsolutePath
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(rootDir0)
      Utils.deleteRecursively(rootDir1)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = testConf.clone
    conf.set("spark.local.dir", rootDirs)
    diskBlockManager = new DiskBlockManager(conf, deleteFilesOnStop = true)
  }

  override def afterEach(): Unit = {
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

  test("enumerating blocks") {
    val ids = (1 to 100).map(i => TestBlockId("test_" + i))
    val files = ids.map(id => diskBlockManager.getFile(id))
    files.foreach(file => writeToFile(file, 10))
    assert(diskBlockManager.getAllBlocks.toSet === ids.toSet)
  }

  test("SPARK-22227: non-block files are skipped") {
    val file = diskBlockManager.getFile("unmanaged_file")
    writeToFile(file, 10)
    assert(diskBlockManager.getAllBlocks().isEmpty)
  }

  test("should still create merge directories if one already exists under a local dir") {
    val mergeDir0 = new File(rootDir0, DiskBlockManager.MERGE_DIRECTORY)
    if (!mergeDir0.exists()) {
      Files.createDirectories(mergeDir0.toPath)
    }
    val mergeDir1 = new File(rootDir1, DiskBlockManager.MERGE_DIRECTORY)
    if (mergeDir1.exists()) {
      Utils.deleteRecursively(mergeDir1)
    }
    testConf.set("spark.local.dir", rootDirs)
    testConf.set("spark.shuffle.push.enabled", "true")
    testConf.set(config.Tests.IS_TESTING, true)
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
    assert(Utils.getConfiguredLocalDirs(testConf).map(
      rootDir => new File(rootDir, DiskBlockManager.MERGE_DIRECTORY))
      .filter(mergeDir => mergeDir.exists()).length === 2)
    // mergeDir0 will be skipped as it already exists
    assert(mergeDir0.list().length === 0)
    // Sub directories get created under mergeDir1
    assert(mergeDir1.list().length === testConf.get(config.DISKSTORE_SUB_DIRECTORIES))
  }

  test("Test dir creation with permission 770") {
    val testDir = new File("target/testDir");
    FileUtils.deleteQuietly(testDir)
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
    diskBlockManager.createDirWithPermission770(testDir)
    assert(testDir.exists && testDir.isDirectory)
    val permission = PosixFilePermissions.toString(
      Files.getPosixFilePermissions(Paths.get("target/testDir")))
    assert(permission.equals("rwxrwx---"))
    FileUtils.deleteQuietly(testDir)
  }

  test("Encode merged directory name and attemptId in shuffleManager field") {
    testConf.set(config.APP_ATTEMPT_ID, "1");
    diskBlockManager = new DiskBlockManager(testConf, deleteFilesOnStop = true)
    val mergedShuffleMeta = diskBlockManager.getMergeDirectoryAndAttemptIDJsonString();
    val mapper: ObjectMapper = new ObjectMapper
    val typeRef: TypeReference[HashMap[String, String]] =
      new TypeReference[HashMap[String, String]]() {}
    val metaMap: HashMap[String, String] = mapper.readValue(mergedShuffleMeta, typeRef)
    val mergeDir = metaMap.get(DiskBlockManager.MERGE_DIR_KEY)
    assert(mergeDir.equals(DiskBlockManager.MERGE_DIRECTORY + "_1"))
    val attemptId = metaMap.get(DiskBlockManager.ATTEMPT_ID_KEY)
    assert(attemptId.equals("1"))
  }

  def writeToFile(file: File, numBytes: Int): Unit = {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }
}

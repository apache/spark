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

package org.apache.spark.shuffle.sort

import java.io._

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage._
import org.apache.spark.util.Utils


class IndexShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _

  private var tempDir: File = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    MockitoAnnotations.initMocks(this)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))

    TaskContext.setTaskContext(taskContext)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  test("commit shuffle files multiple times") {
    val shuffleId = 1
    val mapId = 2
    val idxName = s"shuffle_${shuffleId}_${mapId}_0.index"
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val out = new FileOutputStream(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val indexFile = new File(tempDir.getAbsolutePath, idxName)
    val dataFile = resolver.getDataFile(shuffleId, mapId)

    assert(indexFile.exists())
    assert(indexFile.length() === (lengths.length + 1) * 8)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp.exists())

    val lengths2 = new Array[Long](3)
    val dataTmp2 = File.createTempFile("shuffle", null, tempDir)
    val out2 = new FileOutputStream(dataTmp2)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    assert(indexFile.length() === (lengths.length + 1) * 8)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(dataFile.exists())
    assert(dataFile.length() === 30)
    assert(!dataTmp2.exists())

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val dataIn = new FileInputStream(dataFile)
    Utils.tryWithSafeFinally {
      dataIn.read(firstByte)
    } {
      dataIn.close()
    }
    assert(firstByte(0) === 0)

    // The index file should not change
    val indexIn = new DataInputStream(new FileInputStream(indexFile))
    Utils.tryWithSafeFinally {
      indexIn.readLong() // the first offset is always 0
      assert(indexIn.readLong() === 10, "The index file should not change")
    } {
      indexIn.close()
    }

    // remove data file
    dataFile.delete()

    val lengths3 = Array[Long](7, 10, 15, 3)
    val dataTmp3 = File.createTempFile("shuffle", null, tempDir)
    val out3 = new FileOutputStream(dataTmp3)
    Utils.tryWithSafeFinally {
      out3.write(Array[Byte](2))
      out3.write(new Array[Byte](34))
    } {
      out3.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths3, dataTmp3)
    assert(indexFile.length() === (lengths3.length + 1) * 8)
    assert(lengths3.toSeq != lengths.toSeq)
    assert(dataFile.exists())
    assert(dataFile.length() === 35)
    assert(!dataTmp3.exists())

    // The dataFile should be the new one, since we deleted the dataFile from the first attempt
    val dataIn2 = new FileInputStream(dataFile)
    Utils.tryWithSafeFinally {
      dataIn2.read(firstByte)
    } {
      dataIn2.close()
    }
    assert(firstByte(0) === 2)

    // The index file should be updated, since we deleted the dataFile from the first attempt
    val indexIn2 = new DataInputStream(new FileInputStream(indexFile))
    Utils.tryWithSafeFinally {
      indexIn2.readLong() // the first offset is always 0
      assert(indexIn2.readLong() === 7, "The index file should be updated")
    } {
      indexIn2.close()
    }
  }

  test("get data file should in different task attempts") {
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val shuffleId = 1
    val mapId = 2
    when(taskContext.attemptNumber()).thenReturn(0, Seq(1, 2, 3): _*)
    assert(resolver.getDataFile(shuffleId, mapId).getName.endsWith("0.data"))
    assert(resolver.getDataFile(shuffleId, mapId).getName.endsWith("1.data"))
    assert(resolver.getDataFile(shuffleId, mapId).getName.endsWith("2.data"))
    assert(resolver.getDataFile(shuffleId, mapId).getName.endsWith("3.data"))
  }

  test("different task attempts should be able to choose different local dirs") {
    val localDirSuffixes = 1 to 4
    val dirs = localDirSuffixes.map(x => tempDir + "/test_local" + x).mkString(",")
    val confClone = conf.clone.set("spark.local.dir", dirs)
    val resolver = new IndexShuffleBlockResolver(confClone, blockManager)
    val dbm = new DiskBlockManager(confClone, true)
    when(blockManager.diskBlockManager).thenReturn(dbm)
    when(taskContext.attemptNumber()).thenReturn(0, Seq(1, 2, 3): _*)
    val dataFiles = localDirSuffixes.map(_ => resolver.getDataFile(1, 2))
    val usedLocalDirSuffixed =
      dataFiles.map(_.getAbsolutePath.split("test_local")(1).substring(0, 1).toInt)
    assert(usedLocalDirSuffixed.diff(localDirSuffixes).isEmpty)
  }

  test("new task attempt should be able to success in another available local dir") {
    val localDirSuffixes = 1 to 2
    val dirs = localDirSuffixes.map { x => tempDir + "/test_local" + x }.mkString(",")
    val confClone = conf.clone.set("spark.local.dir", dirs)
    val resolver = new IndexShuffleBlockResolver(confClone, blockManager)
    val dbm = new DiskBlockManager(confClone, true)
    when(blockManager.diskBlockManager).thenReturn(dbm, dbm)
    val shuffleId = 1
    val mapId = 2
    val lengths = Array[Long](10, 0, 20)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val out = new FileOutputStream(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    val idxName = s"shuffle_${shuffleId}_${mapId}_0.index"
    val localDirIdx = Utils.nonNegativeHash(idxName) % localDirSuffixes.length

    val badDisk = dbm.localDirs(localDirIdx)
    badDisk.setWritable(false) // just like a disk error occurs

    // 1. index -> fail
    // 2. index -> data -> verify data
    when(taskContext.attemptNumber()).thenReturn(0, Seq(1, 1, 1): _*)
    val e =
      intercept[IOException](resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp))
    assert(e.getMessage.contains(badDisk.getAbsolutePath))

    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    val dataFile = resolver.getDataFile(shuffleId, mapId)
    assert(dataFile.exists())
  }
}

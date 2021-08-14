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

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream}

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.roaringbitmap.RoaringBitmap
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class IndexShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _

  private var tempDir: File = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
  private val appId = "TESTAPP"

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    MockitoAnnotations.openMocks(this).close()

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.getFile(any[String])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.getMergedShuffleFile(
      any[BlockId], any[Option[Array[String]]])).thenAnswer(
      (invocation: InvocationOnMock) => new File(tempDir, invocation.getArguments.head.toString))
    when(diskBlockManager.localDirs).thenReturn(Array(tempDir))
    conf.set("spark.app.id", appId)
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
    resolver.writeMetadataFileAndCommit(shuffleId, mapId, lengths, Array.empty, dataTmp)

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
    resolver.writeMetadataFileAndCommit(shuffleId, mapId, lengths2, Array.empty, dataTmp2)

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
    resolver.writeMetadataFileAndCommit(shuffleId, mapId, lengths3, Array.empty, dataTmp3)
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

  test("SPARK-33198 getMigrationBlocks should not fail at missing files") {
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    assert(resolver.getMigrationBlocks(ShuffleBlockInfo(Int.MaxValue, Long.MaxValue)).isEmpty)
  }

  test("getMergedBlockData should return expected FileSegmentManagedBuffer list") {
    val shuffleId = 1
    val shuffleMergeId = 0
    val reduceId = 1
    val dataFileName = s"shuffleMerged_${appId}_${shuffleId}_${shuffleMergeId}_$reduceId.data"
    val dataFile = new File(tempDir.getAbsolutePath, dataFileName)
    val out = new FileOutputStream(dataFile)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    val indexFileName = s"shuffleMerged_${appId}_${shuffleId}_${shuffleMergeId}_$reduceId.index"
    generateMergedShuffleIndexFile(indexFileName)
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val dirs = Some(Array[String](tempDir.getAbsolutePath))
    val managedBufferList =
      resolver.getMergedBlockData(ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId),
        dirs)
    assert(managedBufferList.size === 3)
    assert(managedBufferList(0).size === 10)
    assert(managedBufferList(1).size === 0)
    assert(managedBufferList(2).size === 20)
  }

  test("getMergedBlockMeta should return expected MergedBlockMeta") {
    val shuffleId = 1
    val shuffleMergeId = 0
    val reduceId = 1
    val metaFileName = s"shuffleMerged_${appId}_${shuffleId}_${shuffleMergeId}_$reduceId.meta"
    val metaFile = new File(tempDir.getAbsolutePath, metaFileName)
    val chunkTracker = new RoaringBitmap()
    val metaFileOutputStream = new FileOutputStream(metaFile)
    val outMeta = new DataOutputStream(metaFileOutputStream)
    Utils.tryWithSafeFinally {
      chunkTracker.add(1)
      chunkTracker.add(2)
      chunkTracker.serialize(outMeta)
      chunkTracker.clear()
      chunkTracker.add(3)
      chunkTracker.add(4)
      chunkTracker.serialize(outMeta)
      chunkTracker.clear()
      chunkTracker.add(5)
      chunkTracker.add(6)
      chunkTracker.serialize(outMeta)
    }{
      outMeta.close()
    }
    val indexFileName = s"shuffleMerged_${appId}_${shuffleId}_${shuffleMergeId}_$reduceId.index"
    generateMergedShuffleIndexFile(indexFileName)
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val dirs = Some(Array[String](tempDir.getAbsolutePath))
    val mergedBlockMeta =
      resolver.getMergedBlockMeta(
        ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId),
        dirs)
    assert(mergedBlockMeta.getNumChunks === 3)
    assert(mergedBlockMeta.readChunkBitmaps().size === 3)
    assert(mergedBlockMeta.readChunkBitmaps()(0).contains(1))
    assert(mergedBlockMeta.readChunkBitmaps()(0).contains(2))
    assert(!mergedBlockMeta.readChunkBitmaps()(0).contains(3))
    assert(mergedBlockMeta.readChunkBitmaps()(1).contains(3))
    assert(mergedBlockMeta.readChunkBitmaps()(1).contains(4))
    assert(!mergedBlockMeta.readChunkBitmaps()(1).contains(5))
    assert(mergedBlockMeta.readChunkBitmaps()(2).contains(5))
    assert(mergedBlockMeta.readChunkBitmaps()(2).contains(6))
    assert(!mergedBlockMeta.readChunkBitmaps()(2).contains(1))
  }

  private def generateMergedShuffleIndexFile(indexFileName: String): Unit = {
    val lengths = Array[Long](10, 0, 20)
    val indexFile = new File(tempDir.getAbsolutePath, indexFileName)
    val outIndex = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    Utils.tryWithSafeFinally {
      var offset = 0L
      outIndex.writeLong(offset)
      for (length <- lengths) {
        offset += length
        outIndex.writeLong(offset)
      }
    } {
      outIndex.close()
    }
  }

  test("write checksum file") {
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    val dataTmp = File.createTempFile("shuffle", null, tempDir)
    val indexInMemory = Array[Long](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val checksumsInMemory = Array[Long](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    resolver.writeMetadataFileAndCommit(0, 0, indexInMemory, checksumsInMemory, dataTmp)
    val checksumFile = resolver.getChecksumFile(0, 0, conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
    assert(checksumFile.exists())
    val checksumFileName = checksumFile.toString
    val checksumAlgo = checksumFileName.substring(checksumFileName.lastIndexOf(".") + 1)
    assert(checksumAlgo === conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
    val checksumsFromFile = resolver.getChecksums(checksumFile, 10)
    assert(checksumsInMemory === checksumsFromFile)
  }
}

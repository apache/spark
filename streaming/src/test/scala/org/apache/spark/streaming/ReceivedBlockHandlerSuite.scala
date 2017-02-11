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

package org.apache.spark.streaming

import java.io.File
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.memory.StaticMemoryManager
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.util._
import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.util.io.ChunkedByteBuffer

class ReceivedBlockHandlerSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with Matchers
  with LocalSparkContext
  with Logging {

  import WriteAheadLogBasedBlockHandler._
  import WriteAheadLogSuite._

  val conf = new SparkConf()
    .set("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs", "1")
    .set("spark.app.id", "streaming-test")
  val hadoopConf = new Configuration()
  val streamId = 1
  val securityMgr = new SecurityManager(conf)
  val broadcastManager = new BroadcastManager(true, conf, securityMgr)
  val mapOutputTracker = new MapOutputTrackerMaster(conf, broadcastManager, true)
  val shuffleManager = new SortShuffleManager(conf)
  val serializer = new KryoSerializer(conf)
  var serializerManager = new SerializerManager(serializer, conf)
  val manualClock = new ManualClock
  val blockManagerSize = 10000000
  val blockManagerBuffer = new ArrayBuffer[BlockManager]()

  var rpcEnv: RpcEnv = null
  var blockManagerMaster: BlockManagerMaster = null
  var blockManager: BlockManager = null
  var storageLevel: StorageLevel = null
  var tempDirectory: File = null

  before {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)
    conf.set("spark.driver.port", rpcEnv.address.port.toString)

    sc = new SparkContext("local", "test", conf)
    blockManagerMaster = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf,
        new LiveListenerBus(sc))), conf, true)

    storageLevel = StorageLevel.MEMORY_ONLY_SER
    blockManager = createBlockManager(blockManagerSize, conf)

    tempDirectory = Utils.createTempDir()
    manualClock.setTime(0)
  }

  after {
    for ( blockManager <- blockManagerBuffer ) {
      if (blockManager != null) {
        blockManager.stop()
      }
    }
    blockManager = null
    blockManagerBuffer.clear()
    if (blockManagerMaster != null) {
      blockManagerMaster.stop()
      blockManagerMaster = null
    }
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null

    Utils.deleteRecursively(tempDirectory)
  }

  test("BlockManagerBasedBlockHandler - store blocks") {
    withBlockManagerBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        val storedData = blockIds.flatMap { blockId =>
          blockManager
            .getLocalValues(blockId)
            .map(_.data.map(_.toString).toList)
            .getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of BlockManagerBasedStoreResult
        assert(
          storeResults.forall { _.isInstanceOf[BlockManagerBasedStoreResult] },
          "Unexpected store result type"
        )
      }
    }
  }

  test("BlockManagerBasedBlockHandler - handle errors in storing block") {
    withBlockManagerBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - store blocks") {
    withWriteAheadLogBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        val storedData = blockIds.flatMap { blockId =>
          blockManager
            .getLocalValues(blockId)
            .map(_.data.map(_.toString).toList)
            .getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of WriteAheadLogBasedStoreResult
        assert(
          storeResults.forall { _.isInstanceOf[WriteAheadLogBasedStoreResult] },
          "Unexpected store result type"
        )
        // Verify the data in write ahead log files is correct
        val walSegments = storeResults.map { result =>
          result.asInstanceOf[WriteAheadLogBasedStoreResult].walRecordHandle
        }
        val loggedData = walSegments.flatMap { walSegment =>
          val fileSegment = walSegment.asInstanceOf[FileBasedWriteAheadLogSegment]
          val reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf)
          val bytes = reader.read(fileSegment)
          reader.close()
          serializerManager.dataDeserializeStream(
            generateBlockId(), new ChunkedByteBuffer(bytes).toInputStream())(ClassTag.Any).toList
        }
        loggedData shouldEqual data
      }
    }
  }

  test("WriteAheadLogBasedBlockHandler - handle errors in storing block") {
    withWriteAheadLogBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - clean old blocks") {
    withWriteAheadLogBasedBlockHandler { handler =>
      val blocks = Seq.tabulate(10) { i => IteratorBlock(Iterator(1 to i)) }
      storeBlocks(handler, blocks)

      val preCleanupLogFiles = getWriteAheadLogFiles()
      require(preCleanupLogFiles.size > 1)

      // this depends on the number of blocks inserted using generateAndStoreData()
      manualClock.getTimeMillis() shouldEqual 5000L

      val cleanupThreshTime = 3000L
      handler.cleanupOldBlocks(cleanupThreshTime)
      eventually(timeout(10000 millis), interval(10 millis)) {
        getWriteAheadLogFiles().size should be < preCleanupLogFiles.size
      }
    }
  }

  test("Test Block - count messages") {
    // Test count with BlockManagedBasedBlockHandler
    testCountWithBlockManagerBasedBlockHandler(true)
    // Test count with WriteAheadLogBasedBlockHandler
    testCountWithBlockManagerBasedBlockHandler(false)
  }

  test("Test Block - isFullyConsumed") {
    val sparkConf = new SparkConf().set("spark.app.id", "streaming-test")
    sparkConf.set("spark.storage.unrollMemoryThreshold", "512")
    // spark.storage.unrollFraction set to 0.4 for BlockManager
    sparkConf.set("spark.storage.unrollFraction", "0.4")
    // Block Manager with 12000 * 0.4 = 4800 bytes of free space for unroll
    blockManager = createBlockManager(12000, sparkConf)

    // there is not enough space to store this block in MEMORY,
    // But BlockManager will be able to serialize this block to WAL
    // and hence count returns correct value.
     testRecordcount(false, StorageLevel.MEMORY_ONLY,
      IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator), blockManager, Some(70))

    // there is not enough space to store this block in MEMORY,
    // But BlockManager will be able to serialize this block to DISK
    // and hence count returns correct value.
    testRecordcount(true, StorageLevel.MEMORY_AND_DISK,
      IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator), blockManager, Some(70))

    // there is not enough space to store this block With MEMORY_ONLY StorageLevel.
    // BlockManager will not be able to unroll this block
    // and hence it will not tryToPut this block, resulting the SparkException
    storageLevel = StorageLevel.MEMORY_ONLY
    withBlockManagerBasedBlockHandler { handler =>
      val thrown = intercept[SparkException] {
        storeSingleBlock(handler, IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator))
      }
    }
  }

  private def testCountWithBlockManagerBasedBlockHandler(isBlockManagerBasedBlockHandler: Boolean) {
    // ByteBufferBlock-MEMORY_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      ByteBufferBlock(ByteBuffer.wrap(Array.tabulate(100)(i => i.toByte))), blockManager, None)
    // ByteBufferBlock-MEMORY_ONLY_SER
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      ByteBufferBlock(ByteBuffer.wrap(Array.tabulate(100)(i => i.toByte))), blockManager, None)
    // ArrayBufferBlock-MEMORY_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      ArrayBufferBlock(ArrayBuffer.fill(25)(0)), blockManager, Some(25))
    // ArrayBufferBlock-MEMORY_ONLY_SER
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      ArrayBufferBlock(ArrayBuffer.fill(25)(0)), blockManager, Some(25))
    // ArrayBufferBlock-DISK_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.DISK_ONLY,
      ArrayBufferBlock(ArrayBuffer.fill(50)(0)), blockManager, Some(50))
    // ArrayBufferBlock-MEMORY_AND_DISK
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_AND_DISK,
      ArrayBufferBlock(ArrayBuffer.fill(75)(0)), blockManager, Some(75))
    // IteratorBlock-MEMORY_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      IteratorBlock((ArrayBuffer.fill(100)(0)).iterator), blockManager, Some(100))
    // IteratorBlock-MEMORY_ONLY_SER
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      IteratorBlock((ArrayBuffer.fill(100)(0)).iterator), blockManager, Some(100))
    // IteratorBlock-DISK_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.DISK_ONLY,
      IteratorBlock((ArrayBuffer.fill(125)(0)).iterator), blockManager, Some(125))
    // IteratorBlock-MEMORY_AND_DISK
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_AND_DISK,
      IteratorBlock((ArrayBuffer.fill(150)(0)).iterator), blockManager, Some(150))
  }

  private def createBlockManager(
      maxMem: Long,
      conf: SparkConf,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    val memManager = new StaticMemoryManager(conf, Long.MaxValue, maxMem, numCores = 1)
    val transfer = new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1)
    val blockManager = new BlockManager(name, rpcEnv, blockManagerMaster, serializerManager, conf,
      memManager, mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    memManager.setMemoryStore(blockManager.memoryStore)
    blockManager.initialize("app-id")
    blockManagerBuffer += blockManager
    blockManager
  }

  /**
   * Test storing of data using different types of Handler, StorageLevel and ReceivedBlocks
   * and verify the correct record count
   */
  private def testRecordcount(isBlockManagedBasedBlockHandler: Boolean,
      sLevel: StorageLevel,
      receivedBlock: ReceivedBlock,
      bManager: BlockManager,
      expectedNumRecords: Option[Long]
      ) {
    blockManager = bManager
    storageLevel = sLevel
    var bId: StreamBlockId = null
    try {
      if (isBlockManagedBasedBlockHandler) {
        // test received block with BlockManager based handler
        withBlockManagerBasedBlockHandler { handler =>
          val (blockId, blockStoreResult) = storeSingleBlock(handler, receivedBlock)
          bId = blockId
          assert(blockStoreResult.numRecords === expectedNumRecords,
            "Message count not matches for a " +
            receivedBlock.getClass.getName +
            " being inserted using BlockManagerBasedBlockHandler with " + sLevel)
       }
      } else {
        // test received block with WAL based handler
        withWriteAheadLogBasedBlockHandler { handler =>
          val (blockId, blockStoreResult) = storeSingleBlock(handler, receivedBlock)
          bId = blockId
          assert(blockStoreResult.numRecords === expectedNumRecords,
            "Message count not matches for a " +
            receivedBlock.getClass.getName +
            " being inserted using WriteAheadLogBasedBlockHandler with " + sLevel)
        }
      }
    } finally {
     // Removing the Block Id to use same blockManager for next test
     blockManager.removeBlock(bId, true)
    }
  }

  /**
   * Test storing of data using different forms of ReceivedBlocks and verify that they succeeded
   * using the given verification function
   */
  private def testBlockStoring(receivedBlockHandler: ReceivedBlockHandler)
      (verifyFunc: (Seq[String], Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) => Unit) {
    val data = Seq.tabulate(100) { _.toString }

    def storeAndVerify(blocks: Seq[ReceivedBlock]) {
      blocks should not be empty
      val (blockIds, storeResults) = storeBlocks(receivedBlockHandler, blocks)
      withClue(s"Testing with ${blocks.head.getClass.getSimpleName}s:") {
        // Verify returns store results have correct block ids
        (storeResults.map { _.blockId }) shouldEqual blockIds

        // Call handler-specific verification function
        verifyFunc(data, blockIds, storeResults)
      }
    }

    def dataToByteBuffer(b: Seq[String]) =
      serializerManager.dataSerialize(generateBlockId, b.iterator)

    val blocks = data.grouped(10).toSeq

    storeAndVerify(blocks.map { b => IteratorBlock(b.toIterator) })
    storeAndVerify(blocks.map { b => ArrayBufferBlock(new ArrayBuffer ++= b) })
    storeAndVerify(blocks.map { b => ByteBufferBlock(dataToByteBuffer(b).toByteBuffer) })
  }

  /** Test error handling when blocks that cannot be stored */
  private def testErrorHandling(receivedBlockHandler: ReceivedBlockHandler) {
    // Handle error in iterator (e.g. divide-by-zero error)
    intercept[Exception] {
      val iterator = (10 to (-10, -1)).toIterator.map { _ / 0 }
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), IteratorBlock(iterator))
    }

    // Handler error in block manager storing (e.g. too big block)
    intercept[SparkException] {
      val byteBuffer = ByteBuffer.wrap(new Array[Byte](blockManagerSize + 1))
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), ByteBufferBlock(byteBuffer))
    }
  }

  /** Instantiate a BlockManagerBasedBlockHandler and run a code with it */
  private def withBlockManagerBasedBlockHandler(body: BlockManagerBasedBlockHandler => Unit) {
    body(new BlockManagerBasedBlockHandler(blockManager, storageLevel))
  }

  /** Instantiate a WriteAheadLogBasedBlockHandler and run a code with it */
  private def withWriteAheadLogBasedBlockHandler(body: WriteAheadLogBasedBlockHandler => Unit) {
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = false) === 1)
    val receivedBlockHandler = new WriteAheadLogBasedBlockHandler(blockManager, serializerManager,
      1, storageLevel, conf, hadoopConf, tempDirectory.toString, manualClock)
    try {
      body(receivedBlockHandler)
    } finally {
      receivedBlockHandler.stop()
    }
  }

  /** Store blocks using a handler */
  private def storeBlocks(
      receivedBlockHandler: ReceivedBlockHandler,
      blocks: Seq[ReceivedBlock]
    ): (Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) = {
    val blockIds = Seq.fill(blocks.size)(generateBlockId())
    val storeResults = blocks.zip(blockIds).map {
      case (block, id) =>
        manualClock.advance(500) // log rolling interval set to 1000 ms through SparkConf
        logDebug("Inserting block " + id)
        receivedBlockHandler.storeBlock(id, block)
    }.toList
    logDebug("Done inserting")
    (blockIds, storeResults)
  }

  /** Store single block using a handler */
  private def storeSingleBlock(
      handler: ReceivedBlockHandler,
      block: ReceivedBlock
    ): (StreamBlockId, ReceivedBlockStoreResult) = {
    val blockId = generateBlockId
    val blockStoreResult = handler.storeBlock(blockId, block)
    logDebug("Done inserting")
    (blockId, blockStoreResult)
  }

  private def getWriteAheadLogFiles(): Seq[String] = {
    getLogFilesInDirectory(checkpointDirToLogDir(tempDirectory.toString, streamId))
  }

  private def generateBlockId(): StreamBlockId = StreamBlockId(streamId, scala.util.Random.nextLong)
}


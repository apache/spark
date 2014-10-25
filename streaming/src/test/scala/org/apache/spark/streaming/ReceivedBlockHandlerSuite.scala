package org.apache.spark.streaming

import java.io.File
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import akka.actor.{ActorSystem, Props}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage._
import org.apache.spark.streaming.util._
import org.apache.spark.streaming.receiver._
import org.apache.spark.util.AkkaUtils
import WriteAheadLogBasedBlockHandler._
import WriteAheadLogSuite._

class ReceivedBlockHandlerSuite extends FunSuite with BeforeAndAfter with Matchers with Logging {

  val conf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.rollingInterval", "1")
  val hadoopConf = new Configuration()
  val storageLevel = StorageLevel.MEMORY_ONLY_SER
  val streamId = 1
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)
  val serializer = new KryoSerializer(conf)
  val manualClock = new ManualClock
  val blockManagerSize = 10000000

  var actorSystem: ActorSystem = null
  var blockManagerMaster: BlockManagerMaster = null
  var blockManager: BlockManager = null
  var receivedBlockHandler: ReceivedBlockHandler = null
  var tempDirectory: File = null

  before {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      "test", "localhost", 0, conf = conf, securityManager = securityMgr)
    this.actorSystem = actorSystem
    conf.set("spark.driver.port", boundPort.toString)

    blockManagerMaster = new BlockManagerMaster(
      actorSystem.actorOf(Props(new BlockManagerMasterActor(true, conf, new LiveListenerBus))),
      conf, true)

    blockManager = new BlockManager("bm", actorSystem, blockManagerMaster, serializer,
      blockManagerSize, conf, mapOutputTracker, shuffleManager,
      new NioBlockTransferService(conf, securityMgr))

    tempDirectory = Files.createTempDir()
    manualClock.setTime(0)
  }

  after {
    if (receivedBlockHandler != null) {
      if (receivedBlockHandler.isInstanceOf[WriteAheadLogBasedBlockHandler]) {
        receivedBlockHandler.asInstanceOf[WriteAheadLogBasedBlockHandler].stop()
      }
    }
    if (blockManager != null) {
      blockManager.stop()
      blockManager = null
    }
    if (blockManagerMaster != null) {
      blockManagerMaster.stop()
      blockManagerMaster = null
    }
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    actorSystem = null

    if (tempDirectory != null && tempDirectory.exists()) {
      FileUtils.deleteDirectory(tempDirectory)
      tempDirectory = null
    }
  }

  test("BlockManagerBasedBlockHandler - store blocks") {
    createBlockManagerBasedBlockHandler()
    testBlockStore { case (data, blockIds, storeResults) =>
      // Verify the data in block manager is correct
      val storedData = blockIds.flatMap { blockId =>
        blockManager.getLocal(blockId).map { _.data.map {_.toString}.toList }.getOrElse(List.empty)
      }.toList
      storedData shouldEqual data

      // Verify the store results were None
      storeResults.foreach { _ shouldEqual None }
    }
  }

  test("BlockManagerBasedBlockHandler - handle errors in storing block") {
    createBlockManagerBasedBlockHandler()
    testErrorHandling()
  }

  test("WriteAheadLogBasedBlockHandler - store blocks") {
    createWriteAheadLogBasedBlockHandler()
    testBlockStore { case (data, blockIds, storeResults) =>
      // Verify the data in block manager is correct
      val storedData = blockIds.flatMap { blockId =>
        blockManager.getLocal(blockId).map { _.data.map {_.toString}.toList }.getOrElse(List.empty)
      }.toList
      storedData shouldEqual data

      // Verify the data in write ahead log files is correct
      storeResults.foreach { _.isInstanceOf[Some[AnyRef]] }
      val fileSegments = storeResults.map { _.asInstanceOf[Some[WriteAheadLogFileSegment]].get }
      val loggedData = fileSegments.flatMap { segment =>
        val reader = new WriteAheadLogRandomReader(segment.path, hadoopConf)
        val bytes = reader.read(segment)
        reader.close()
        blockManager.dataDeserialize(generateBlockId(), bytes).toList
      }
      loggedData shouldEqual data
    }
  }

  test("WriteAheadLogBasedBlockHandler - handle errors in storing block") {
    createWriteAheadLogBasedBlockHandler()
    testErrorHandling()
  }

  test("WriteAheadLogBasedBlockHandler - cleanup old blocks") {
    createWriteAheadLogBasedBlockHandler()
    storeBlocks((1 to 10).map { i => IteratorBlock(Seq(1 to i).toIterator)})
    val preCleanupLogFiles = getWriteAheadLogFiles()
    preCleanupLogFiles.size should be > 1

    // this depends on the number of blocks inserted using generateAndStoreData()
    manualClock.currentTime() shouldEqual 5000L

    val cleanupThreshTime = 3000L
    receivedBlockHandler.cleanupOldBlock(cleanupThreshTime)
    eventually(timeout(10000 millis), interval(10 millis)) {
      getWriteAheadLogFiles().size should be < preCleanupLogFiles.size
    }
  }

  /**
   * Test storing of data using different forms of ReceivedBlocks and verify that they suceeded
   * using the given verification function
   */
  def testBlockStore(verifyFunc: (Seq[String], Seq[StreamBlockId], Seq[Option[AnyRef]]) => Unit) {
    val data = (1 to 100).map { _.toString }

    def storeAndVerify(blocks: Seq[ReceivedBlock]) {
      blocks should not be empty
      val (blockIds, storeResults) = storeBlocks(blocks)
      withClue(s"Testing with ${blocks.head.getClass.getSimpleName}s:") {
        verifyFunc(data, blockIds, storeResults)
      }
    }

    val blocks = data.grouped(10).toSeq
    storeAndVerify(blocks.map { b => {
      IteratorBlock(b.toIterator)
    } })
    storeAndVerify(blocks.map { b => ArrayBufferBlock(new ArrayBuffer ++= b) })
    storeAndVerify(blocks.map {
      b => ByteBufferBlock(blockManager.dataSerialize(generateBlockId, b.iterator))
    })
  }


  /** Test error handling when blocks that cannot be stored */
  def testErrorHandling() {
    // Handle error in iterator (e.g. divide-by-zero error)
    intercept[Exception] {
      val iterator = (10 to(-10, -1)).toIterator.map {
        _ / 0
      }
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), IteratorBlock(iterator))
    }

    // Handler error in block manager storing (e.g. too big block)
    intercept[SparkException] {
      val byteBuffer = ByteBuffer.wrap(new Array[Byte](blockManagerSize + 1))
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), ByteBufferBlock(byteBuffer))
    }
  }

  def createBlockManagerBasedBlockHandler() {
    receivedBlockHandler = new BlockManagerBasedBlockHandler(blockManager, storageLevel)
  }

  def createWriteAheadLogBasedBlockHandler() {
    receivedBlockHandler = new WriteAheadLogBasedBlockHandler(blockManager, 1,
      storageLevel, conf, hadoopConf, tempDirectory.toString, manualClock)
  }

  def storeBlocks(blocks: Seq[ReceivedBlock]): (Seq[StreamBlockId], Seq[Option[AnyRef]]) = {
    val blockIds = (0 until blocks.size).map { _ => generateBlockId() }
    val storeResults = blocks.zip(blockIds).map {
      case (block, id) =>
        manualClock.addToTime(500) // log rolling interval set to 1000 ms through SparkConf
        logInfo("Inserting block " + id)
        receivedBlockHandler.storeBlock(id, block)
    }.toList
    logInfo("Done inserting")
    (blockIds, storeResults)
  }

  def getWriteAheadLogFiles(): Seq[String] = {
    getLogFilesInDirectory(checkpointDirToLogDir(tempDirectory.toString, streamId))
  }

  def generateBlockId(): StreamBlockId = StreamBlockId(streamId, scala.util.Random.nextLong)
}

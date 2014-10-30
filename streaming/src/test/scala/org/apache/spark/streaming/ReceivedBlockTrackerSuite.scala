package org.apache.spark.streaming

import java.io.File

import scala.Some
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.util.WriteAheadLogSuite._
import org.apache.spark.streaming.util.{WriteAheadLogReader, Clock, ManualClock, SystemClock}
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.storage.StreamBlockId
import scala.Some
import org.apache.spark.streaming.receiver.BlockManagerBasedStoreResult

class ReceivedBlockTrackerSuite
  extends FunSuite with BeforeAndAfter with Matchers with Logging {

  val conf = new SparkConf().setMaster("local[2]").setAppName("ReceivedBlockTrackerSuite")
  conf.set("spark.streaming.receivedBlockTracker.writeAheadLog.rotationIntervalSecs", "1")

  val hadoopConf = new Configuration()
  val akkaTimeout = 10 seconds
  val streamId = 1

  var allReceivedBlockTrackers = new ArrayBuffer[ReceivedBlockTracker]()
  var checkpointDirectory: File = null

  before {
    checkpointDirectory = Files.createTempDir()
  }

  after {
    allReceivedBlockTrackers.foreach { _.stop() }
    if (checkpointDirectory != null && checkpointDirectory.exists()) {
      FileUtils.deleteDirectory(checkpointDirectory)
      checkpointDirectory = null
    }
  }

  test("block addition, and block to batch allocation") {
    val receivedBlockTracker = createTracker(enableCheckpoint = false)
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual Seq.empty

    val blockInfos = generateBlockInfos()
    blockInfos.map(receivedBlockTracker.addBlock)

    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
    receivedBlockTracker.getOrAllocateBlocksToBatch(1, streamId) shouldEqual blockInfos
    receivedBlockTracker.getUnallocatedBlocks(streamId) should have size 0
    receivedBlockTracker.getOrAllocateBlocksToBatch(1, streamId) shouldEqual blockInfos
    receivedBlockTracker.getOrAllocateBlocksToBatch(2, streamId) should have size 0
  }

  test("block addition, block to batch allocation and cleanup with write ahead log") {
    val manualClock = new ManualClock
    conf.getInt(
      "spark.streaming.receivedBlockTracker.writeAheadLog.rotationIntervalSecs", -1) should be (1)

    // Set the time increment level to twice the rotation interval so that every increment creates
    // a new log file
    val timeIncrementMillis = 2000L
    def incrementTime() {
      manualClock.addToTime(timeIncrementMillis)
    }

    // Generate and add blocks to the given tracker
    def addBlockInfos(tracker: ReceivedBlockTracker): Seq[ReceivedBlockInfo] = {
      val blockInfos = generateBlockInfos()
      blockInfos.map(tracker.addBlock)
      blockInfos
    }

    // Print the data present in the log ahead files in the log directory
    def printLogFiles(message: String) {
      val fileContents = getWriteAheadLogFiles().map { file =>
          (s"\n>>>>> $file: <<<<<\n${getWrittenLogData(file).mkString("\n")}")
      }.mkString("\n")
      logInfo(s"\n\n=====================\n$message\n$fileContents\n=====================\n")
    }

    // Start tracker and add blocks
    val tracker1 = createTracker(enableCheckpoint = true, clock = manualClock)
    val blockInfos1 = addBlockInfos(tracker1)
    tracker1.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Verify whether write ahead log has correct contents
    val expectedWrittenData1 = blockInfos1.map(BlockAddition)
    getWrittenLogData() shouldEqual expectedWrittenData1
    getWriteAheadLogFiles() should have size 1

    // Restart tracker and verify recovered list of unallocated blocks
    incrementTime()
    val tracker2 = createTracker(enableCheckpoint = true, clock = manualClock)
    tracker2.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Allocate blocks to batch and verify whether the unallocated blocks got allocated
    val batchTime1 = manualClock.currentTime
    tracker2.getOrAllocateBlocksToBatch(batchTime1, streamId) shouldEqual blockInfos1

    // Add more blocks and allocate to another batch
    incrementTime()
    val batchTime2 = manualClock.currentTime
    val blockInfos2 = addBlockInfos(tracker2)
    tracker2.getOrAllocateBlocksToBatch(batchTime2, streamId) shouldEqual blockInfos2

    // Verify whether log has correct contents
    val expectedWrittenData2 = expectedWrittenData1 ++
      Seq(createBatchAllocation(batchTime1, blockInfos1)) ++
      blockInfos2.map(BlockAddition) ++
      Seq(createBatchAllocation(batchTime2, blockInfos2))
    getWrittenLogData() shouldEqual expectedWrittenData2

    // Restart tracker and verify recovered state
    incrementTime()
    val tracker3 = createTracker(enableCheckpoint = true, clock = manualClock)
    tracker3.getOrAllocateBlocksToBatch(batchTime1, streamId) shouldEqual blockInfos1
    tracker3.getOrAllocateBlocksToBatch(batchTime2, streamId) shouldEqual blockInfos2
    tracker3.getUnallocatedBlocks(streamId) shouldBe empty

    // Cleanup first batch but not second batch
    val oldestLogFile = getWriteAheadLogFiles().head
    incrementTime()
    tracker3.cleanupOldBatches(batchTime2)

    // Verify that the batch allocations have been cleaned, and the act has been written to log
    tracker3.getOrAllocateBlocksToBatch(batchTime1, streamId) shouldEqual Seq.empty
    getWrittenLogData(getWriteAheadLogFiles().last) should contain(createBatchCleanup(batchTime1))

    // Verify that at least one log file gets deleted
    eventually(timeout(10 seconds), interval(10 millisecond )) {
      getWriteAheadLogFiles() should not contain oldestLogFile
    }
    printLogFiles("After cleanup")

    // Restart tracker and verify recovered state, specifically whether info about the first
    // batch has been removed, but not the second batch
    incrementTime()
    val tracker4 = createTracker(enableCheckpoint = true, clock = manualClock)
    tracker4.getUnallocatedBlocks(streamId) shouldBe empty
    tracker4.getOrAllocateBlocksToBatch(batchTime1, streamId) shouldBe empty  // should be cleaned
    tracker4.getOrAllocateBlocksToBatch(batchTime2, streamId) shouldEqual blockInfos2
  }

  /**
   * Create tracker object with the optional provided clock. Use fake clock if you
   * want to control time by manually incrementing it to test log cleanup.
   */
  def createTracker(enableCheckpoint: Boolean, clock: Clock = new SystemClock): ReceivedBlockTracker = {
    val cpDirOption = if (enableCheckpoint) Some(checkpointDirectory.toString) else None
    val tracker = new ReceivedBlockTracker(conf, hadoopConf, Seq(streamId), clock, cpDirOption)
    allReceivedBlockTrackers += tracker
    tracker
  }

  /** Generate blocks infos using random ids */
  def generateBlockInfos(): Seq[ReceivedBlockInfo] = {
    List.fill(5)(ReceivedBlockInfo(streamId, 0,
      BlockManagerBasedStoreResult(StreamBlockId(streamId, math.abs(Random.nextInt)))))
  }

  /** Get all the data written in the given write ahead log file. */
  def getWrittenLogData(logFile: String): Seq[ReceivedBlockTrackerAction] = {
    getWrittenLogData(Seq(logFile))
  }

  /**
   * Get all the data written in the given write ahead log files. By default, it will read all
   * files in the test log directory.
   */
  def getWrittenLogData(logFiles: Seq[String] = getWriteAheadLogFiles): Seq[ReceivedBlockTrackerAction] = {
    logFiles.flatMap {
      file => new WriteAheadLogReader(file, hadoopConf).toSeq
    }.map { byteBuffer =>
      Utils.deserialize[ReceivedBlockTrackerAction](byteBuffer.array)
    }.toList
  }

  /** Get all the write ahead log files in the test directory */
  def getWriteAheadLogFiles(): Seq[String] = {
    import ReceivedBlockTracker._
    val logDir = checkpointDirToLogDir(checkpointDirectory.toString)
    getLogFilesInDirectory(logDir).map { _.toString }
  }

  /** Create batch allocation object from the given info */
  def createBatchAllocation(time: Long, blockInfos: Seq[ReceivedBlockInfo]): BatchAllocations = {
    BatchAllocations(time, AllocatedBlocks(Map((streamId -> blockInfos))))
  }

  /** Create batch cleanup object from the given info */
  def createBatchCleanup(time: Long, moreTimes: Long*): BatchCleanup = {
    BatchCleanup((Seq(time) ++ moreTimes).map(Time.apply))
  }

  implicit def millisToTime(milliseconds: Long): Time = Time(milliseconds)

  implicit def timeToMillis(time: Time): Long = time.milliseconds
}

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
import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.BlockManagerBasedStoreResult
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.util._
import org.apache.spark.streaming.util.WriteAheadLogSuite._
import org.apache.spark.util.{Clock, ManualClock, SystemClock, Utils}

class ReceivedBlockTrackerSuite
  extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {

  val hadoopConf = new Configuration()
  val streamId = 1

  var allReceivedBlockTrackers = new ArrayBuffer[ReceivedBlockTracker]()
  var checkpointDirectory: File = null
  var conf: SparkConf = null

  before {
    conf = new SparkConf().setMaster("local[2]").setAppName("ReceivedBlockTrackerSuite")
    checkpointDirectory = Utils.createTempDir()
  }

  after {
    allReceivedBlockTrackers.foreach { _.stop() }
    Utils.deleteRecursively(checkpointDirectory)
  }

  test("block addition, and block to batch allocation") {
    val receivedBlockTracker = createTracker(setCheckpointDir = false)
    receivedBlockTracker.isWriteAheadLogEnabled should be (false)  // should be disable by default
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual Seq.empty

    val blockInfos = generateBlockInfos()
    blockInfos.map(receivedBlockTracker.addBlock)

    // Verify added blocks are unallocated blocks
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
    receivedBlockTracker.hasUnallocatedReceivedBlocks should be (true)


    // Allocate the blocks to a batch and verify that all of them have been allocated
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos
    receivedBlockTracker.getBlocksOfBatch(1) shouldEqual Map(streamId -> blockInfos)
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldBe empty
    receivedBlockTracker.hasUnallocatedReceivedBlocks should be (false)

    // Allocate no blocks to another batch
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty
    receivedBlockTracker.getBlocksOfBatch(2) shouldEqual Map(streamId -> Seq.empty)

    // Verify that older batches have no operation on batch allocation,
    // will return the same blocks as previously allocated.
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos

    blockInfos.map(receivedBlockTracker.addBlock)
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
  }

  test("recovery and cleanup with write ahead logs") {
    val manualClock = new ManualClock
    // Set the time increment level to twice the rotation interval so that every increment creates
    // a new log file

    def incrementTime() {
      val timeIncrementMillis = 2000L
      manualClock.advance(timeIncrementMillis)
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

    // Set WAL configuration
    conf.set("spark.streaming.driver.writeAheadLog.rollingIntervalSecs", "1")
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = true) === 1)

    // Start tracker and add blocks
    val tracker1 = createTracker(clock = manualClock)
    tracker1.isWriteAheadLogEnabled should be (true)

    val blockInfos1 = addBlockInfos(tracker1)
    tracker1.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Verify whether write ahead log has correct contents
    val expectedWrittenData1 = blockInfos1.map(BlockAdditionEvent)
    getWrittenLogData() shouldEqual expectedWrittenData1
    getWriteAheadLogFiles() should have size 1
    tracker1.stop()

    incrementTime()

    // Recovery without recovery from WAL and verify list of unallocated blocks is empty
    val tracker1_ = createTracker(clock = manualClock, recoverFromWriteAheadLog = false)
    tracker1_.getUnallocatedBlocks(streamId) shouldBe empty
    tracker1_.hasUnallocatedReceivedBlocks should be (false)
    tracker1_.stop()

    // Restart tracker and verify recovered list of unallocated blocks
    val tracker2 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    val unallocatedBlocks = tracker2.getUnallocatedBlocks(streamId).toList
    unallocatedBlocks shouldEqual blockInfos1
    unallocatedBlocks.foreach { block =>
      block.isBlockIdValid() should be (false)
    }


    // Allocate blocks to batch and verify whether the unallocated blocks got allocated
    val batchTime1 = manualClock.getTimeMillis()
    tracker2.allocateBlocksToBatch(batchTime1)
    tracker2.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual blockInfos1
    tracker2.getBlocksOfBatch(batchTime1) shouldEqual Map(streamId -> blockInfos1)

    // Add more blocks and allocate to another batch
    incrementTime()
    val batchTime2 = manualClock.getTimeMillis()
    val blockInfos2 = addBlockInfos(tracker2)
    tracker2.allocateBlocksToBatch(batchTime2)
    tracker2.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
    tracker2.stop()

    // Verify whether log has correct contents
    val expectedWrittenData2 = expectedWrittenData1 ++
      Seq(createBatchAllocation(batchTime1, blockInfos1)) ++
      blockInfos2.map(BlockAdditionEvent) ++
      Seq(createBatchAllocation(batchTime2, blockInfos2))
    getWrittenLogData() shouldEqual expectedWrittenData2

    // Restart tracker and verify recovered state
    incrementTime()
    val tracker3 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    tracker3.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual blockInfos1
    tracker3.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
    tracker3.getUnallocatedBlocks(streamId) shouldBe empty

    // Cleanup first batch but not second batch
    val oldestLogFile = getWriteAheadLogFiles().head
    incrementTime()
    tracker3.cleanupOldBatches(batchTime2, waitForCompletion = true)

    // Verify that the batch allocations have been cleaned, and the act has been written to log
    tracker3.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual Seq.empty
    getWrittenLogData(getWriteAheadLogFiles().last) should contain(createBatchCleanup(batchTime1))

    // Verify that at least one log file gets deleted
    eventually(timeout(10 seconds), interval(10 millisecond)) {
      getWriteAheadLogFiles() should not contain oldestLogFile
    }
    printLogFiles("After clean")
    tracker3.stop()

    // Restart tracker and verify recovered state, specifically whether info about the first
    // batch has been removed, but not the second batch
    incrementTime()
    val tracker4 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    tracker4.getUnallocatedBlocks(streamId) shouldBe empty
    tracker4.getBlocksOfBatchAndStream(batchTime1, streamId) shouldBe empty  // should be cleaned
    tracker4.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
    tracker4.stop()
  }

  test("disable write ahead log when checkpoint directory is not set") {
    // When checkpoint is disabled, then the write ahead log is disabled
    val tracker1 = createTracker(setCheckpointDir = false)
    tracker1.isWriteAheadLogEnabled should be (false)
  }

  test("parallel file deletion in FileBasedWriteAheadLog is robust to deletion error") {
    conf.set("spark.streaming.driver.writeAheadLog.rollingIntervalSecs", "1")
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = true) === 1)

    val addBlocks = generateBlockInfos()
    val batch1 = addBlocks.slice(0, 1)
    val batch2 = addBlocks.slice(1, 3)
    val batch3 = addBlocks.slice(3, addBlocks.length)

    assert(getWriteAheadLogFiles().length === 0)

    // list of timestamps for files
    val t = Seq.tabulate(5)(i => i * 1000)

    writeEventsManually(getLogFileName(t(0)), Seq(createBatchCleanup(t(0))))
    assert(getWriteAheadLogFiles().length === 1)

    // The goal is to create several log files which should have been cleaned up.
    // If we face any issue during recovery, because these old files exist, then we need to make
    // deletion more robust rather than a parallelized operation where we fire and forget
    val batch1Allocation = createBatchAllocation(t(1), batch1)
    writeEventsManually(getLogFileName(t(1)), batch1.map(BlockAdditionEvent) :+ batch1Allocation)

    writeEventsManually(getLogFileName(t(2)), Seq(createBatchCleanup(t(1))))

    val batch2Allocation = createBatchAllocation(t(3), batch2)
    writeEventsManually(getLogFileName(t(3)), batch2.map(BlockAdditionEvent) :+ batch2Allocation)

    writeEventsManually(getLogFileName(t(4)), batch3.map(BlockAdditionEvent))

    // We should have 5 different log files as we called `writeEventsManually` with 5 different
    // timestamps
    assert(getWriteAheadLogFiles().length === 5)

    // Create the tracker to recover from the log files. We're going to ask the tracker to clean
    // things up, and then we're going to rewrite that data, and recover using a different tracker.
    // They should have identical data no matter what
    val tracker = createTracker(recoverFromWriteAheadLog = true, clock = new ManualClock(t(4)))

    def compareTrackers(base: ReceivedBlockTracker, subject: ReceivedBlockTracker): Unit = {
      subject.getBlocksOfBatchAndStream(t(3), streamId) should be(
        base.getBlocksOfBatchAndStream(t(3), streamId))
      subject.getBlocksOfBatchAndStream(t(1), streamId) should be(
        base.getBlocksOfBatchAndStream(t(1), streamId))
      subject.getBlocksOfBatchAndStream(t(0), streamId) should be(Nil)
    }

    // ask the tracker to clean up some old files
    tracker.cleanupOldBatches(t(3), waitForCompletion = true)
    assert(getWriteAheadLogFiles().length === 3)

    val tracker2 = createTracker(recoverFromWriteAheadLog = true, clock = new ManualClock(t(4)))
    compareTrackers(tracker, tracker2)

    // rewrite first file
    writeEventsManually(getLogFileName(t(0)), Seq(createBatchCleanup(t(0))))
    assert(getWriteAheadLogFiles().length === 4)
    // make sure trackers are consistent
    val tracker3 = createTracker(recoverFromWriteAheadLog = true, clock = new ManualClock(t(4)))
    compareTrackers(tracker, tracker3)

    // rewrite second file
    writeEventsManually(getLogFileName(t(1)), batch1.map(BlockAdditionEvent) :+ batch1Allocation)
    assert(getWriteAheadLogFiles().length === 5)
    // make sure trackers are consistent
    val tracker4 = createTracker(recoverFromWriteAheadLog = true, clock = new ManualClock(t(4)))
    compareTrackers(tracker, tracker4)
  }

  /**
   * Create tracker object with the optional provided clock. Use fake clock if you
   * want to control time by manually incrementing it to test log clean.
   */
  def createTracker(
      setCheckpointDir: Boolean = true,
      recoverFromWriteAheadLog: Boolean = false,
      clock: Clock = new SystemClock): ReceivedBlockTracker = {
    val cpDirOption = if (setCheckpointDir) Some(checkpointDirectory.toString) else None
    val tracker = new ReceivedBlockTracker(
      conf, hadoopConf, Seq(streamId), clock, recoverFromWriteAheadLog, cpDirOption)
    allReceivedBlockTrackers += tracker
    tracker
  }

  /** Generate blocks infos using random ids */
  def generateBlockInfos(): Seq[ReceivedBlockInfo] = {
    List.fill(5)(ReceivedBlockInfo(streamId, Some(0L), None,
      BlockManagerBasedStoreResult(StreamBlockId(streamId, math.abs(Random.nextInt)), Some(0L))))
  }

  /**
   * Write received block tracker events to a file manually.
   */
  def writeEventsManually(filePath: String, events: Seq[ReceivedBlockTrackerLogEvent]): Unit = {
    val writer = HdfsUtils.getOutputStream(filePath, hadoopConf)
    events.foreach { event =>
      val bytes = Utils.serialize(event)
      writer.writeInt(bytes.size)
      writer.write(bytes)
    }
    writer.close()
  }

  /** Get all the data written in the given write ahead log file. */
  def getWrittenLogData(logFile: String): Seq[ReceivedBlockTrackerLogEvent] = {
    getWrittenLogData(Seq(logFile))
  }

  /** Get the log file name for the given log start time. */
  def getLogFileName(time: Long, rollingIntervalSecs: Int = 1): String = {
    checkpointDirectory.toString + File.separator + "receivedBlockMetadata" +
      File.separator + s"log-$time-${time + rollingIntervalSecs * 1000}"
  }

  /**
   * Get all the data written in the given write ahead log files. By default, it will read all
   * files in the test log directory.
   */
  def getWrittenLogData(logFiles: Seq[String] = getWriteAheadLogFiles)
    : Seq[ReceivedBlockTrackerLogEvent] = {
    logFiles.flatMap {
      file => new FileBasedWriteAheadLogReader(file, hadoopConf).toSeq
    }.flatMap { byteBuffer =>
      val validBuffer = if (WriteAheadLogUtils.isBatchingEnabled(conf, isDriver = true)) {
        Utils.deserialize[Array[Array[Byte]]](byteBuffer.array()).map(ByteBuffer.wrap)
      } else {
        Array(byteBuffer)
      }
      validBuffer.map(b => Utils.deserialize[ReceivedBlockTrackerLogEvent](b.array()))
    }.toList
  }

  /** Get all the write ahead log files in the test directory */
  def getWriteAheadLogFiles(): Seq[String] = {
    import ReceivedBlockTracker._
    val logDir = checkpointDirToLogDir(checkpointDirectory.toString)
    getLogFilesInDirectory(logDir).map { _.toString }
  }

  /** Create batch allocation object from the given info */
  def createBatchAllocation(time: Long, blockInfos: Seq[ReceivedBlockInfo])
    : BatchAllocationEvent = {
    BatchAllocationEvent(time, AllocatedBlocks(Map((streamId -> blockInfos))))
  }

  /** Create batch clean object from the given info */
  def createBatchCleanup(time: Long, moreTimes: Long*): BatchCleanupEvent = {
    BatchCleanupEvent((Seq(time) ++ moreTimes).map(Time.apply))
  }

  implicit def millisToTime(milliseconds: Long): Time = Time(milliseconds)

  implicit def timeToMillis(time: Time): Long = time.milliseconds
}

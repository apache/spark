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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.BlockManagerBasedStoreResult
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.util.{Clock, ManualClock, SystemClock, WriteAheadLogReader}
import org.apache.spark.streaming.util.WriteAheadLogSuite._
import org.apache.spark.util.Utils

class ReceivedBlockTrackerSuite
  extends FunSuite with BeforeAndAfter with Matchers with Logging {

  val hadoopConf = new Configuration()
  val akkaTimeout = 10 seconds
  val streamId = 1

  var allReceivedBlockTrackers = new ArrayBuffer[ReceivedBlockTracker]()
  var checkpointDirectory: File = null
  var conf: SparkConf = null

  before {
    conf = new SparkConf().setMaster("local[2]").setAppName("ReceivedBlockTrackerSuite")
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
    val receivedBlockTracker = createTracker(setCheckpointDir = false)
    receivedBlockTracker.isLogManagerEnabled should be (false)  // should be disable by default
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual Seq.empty

    val blockInfos = generateBlockInfos()
    blockInfos.map(receivedBlockTracker.addBlock)

    // Verify added blocks are unallocated blocks
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos

    // Allocate the blocks to a batch and verify that all of them have been allocated
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldBe empty

    // Allocate no blocks to another batch
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty

    // Verify that older batches have no operation on batch allocation,
    // will return the same blocks as previously allocated.
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos

    blockInfos.map(receivedBlockTracker.addBlock)
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
  }

  test("block addition, block to batch allocation and cleanup with write ahead log") {
    val manualClock = new ManualClock
    // Set the time increment level to twice the rotation interval so that every increment creates
    // a new log file

    def incrementTime() {
      val timeIncrementMillis = 2000L
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
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    conf.set("spark.streaming.receivedBlockTracker.writeAheadLog.rotationIntervalSecs", "1")
    val tracker1 = createTracker(clock = manualClock)
    tracker1.isLogManagerEnabled should be (true)

    val blockInfos1 = addBlockInfos(tracker1)
    tracker1.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Verify whether write ahead log has correct contents
    val expectedWrittenData1 = blockInfos1.map(BlockAdditionEvent)
    getWrittenLogData() shouldEqual expectedWrittenData1
    getWriteAheadLogFiles() should have size 1

    // Restart tracker and verify recovered list of unallocated blocks
    incrementTime()
    val tracker2 = createTracker(clock = manualClock)
    tracker2.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Allocate blocks to batch and verify whether the unallocated blocks got allocated
    val batchTime1 = manualClock.currentTime
    tracker2.allocateBlocksToBatch(batchTime1)
    tracker2.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual blockInfos1

    // Add more blocks and allocate to another batch
    incrementTime()
    val batchTime2 = manualClock.currentTime
    val blockInfos2 = addBlockInfos(tracker2)
    tracker2.allocateBlocksToBatch(batchTime2)
    tracker2.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2

    // Verify whether log has correct contents
    val expectedWrittenData2 = expectedWrittenData1 ++
      Seq(createBatchAllocation(batchTime1, blockInfos1)) ++
      blockInfos2.map(BlockAdditionEvent) ++
      Seq(createBatchAllocation(batchTime2, blockInfos2))
    getWrittenLogData() shouldEqual expectedWrittenData2

    // Restart tracker and verify recovered state
    incrementTime()
    val tracker3 = createTracker(clock = manualClock)
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
    printLogFiles("After cleanup")

    // Restart tracker and verify recovered state, specifically whether info about the first
    // batch has been removed, but not the second batch
    incrementTime()
    val tracker4 = createTracker(clock = manualClock)
    tracker4.getUnallocatedBlocks(streamId) shouldBe empty
    tracker4.getBlocksOfBatchAndStream(batchTime1, streamId) shouldBe empty  // should be cleaned
    tracker4.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
  }

  test("enabling write ahead log but not setting checkpoint dir") {
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    intercept[SparkException] {
      createTracker(setCheckpointDir = false)
    }
  }

  test("setting checkpoint dir but not enabling write ahead log") {
    // When WAL config is not set, log manager should not be enabled
    val tracker1 = createTracker(setCheckpointDir = true)
    tracker1.isLogManagerEnabled should be (false)

    // When WAL is explicitly disabled, log manager should not be enabled
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "false")
    val tracker2 = createTracker(setCheckpointDir = true)
    tracker2.isLogManagerEnabled should be(false)
  }

  /**
   * Create tracker object with the optional provided clock. Use fake clock if you
   * want to control time by manually incrementing it to test log cleanup.
   */
  def createTracker(
      setCheckpointDir: Boolean = true,
      clock: Clock = new SystemClock): ReceivedBlockTracker = {
    val cpDirOption = if (setCheckpointDir) Some(checkpointDirectory.toString) else None
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
  def getWrittenLogData(logFile: String): Seq[ReceivedBlockTrackerLogEvent] = {
    getWrittenLogData(Seq(logFile))
  }

  /**
   * Get all the data written in the given write ahead log files. By default, it will read all
   * files in the test log directory.
   */
  def getWrittenLogData(logFiles: Seq[String] = getWriteAheadLogFiles): Seq[ReceivedBlockTrackerLogEvent] = {
    logFiles.flatMap {
      file => new WriteAheadLogReader(file, hadoopConf).toSeq
    }.map { byteBuffer =>
      Utils.deserialize[ReceivedBlockTrackerLogEvent](byteBuffer.array)
    }.toList
  }

  /** Get all the write ahead log files in the test directory */
  def getWriteAheadLogFiles(): Seq[String] = {
    import ReceivedBlockTracker._
    val logDir = checkpointDirToLogDir(checkpointDirectory.toString)
    getLogFilesInDirectory(logDir).map { _.toString }
  }

  /** Create batch allocation object from the given info */
  def createBatchAllocation(time: Long, blockInfos: Seq[ReceivedBlockInfo]): BatchAllocationEvent = {
    BatchAllocationEvent(time, AllocatedBlocks(Map((streamId -> blockInfos))))
  }

  /** Create batch cleanup object from the given info */
  def createBatchCleanup(time: Long, moreTimes: Long*): BatchCleanupEvent = {
    BatchCleanupEvent((Seq(time) ++ moreTimes).map(Time.apply))
  }

  implicit def millisToTime(milliseconds: Long): Time = Time(milliseconds)

  implicit def timeToMillis(time: Time): Long = time.milliseconds
}

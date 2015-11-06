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
package org.apache.spark.streaming.util

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.Matchers.{eq => meq}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import org.scalatest.{PrivateMethodTester, BeforeAndAfterEach, BeforeAndAfter}
import org.scalatest.mock.MockitoSugar

import org.apache.spark.streaming.scheduler._
import org.apache.spark.util.{ThreadUtils, ManualClock, Utils}
import org.apache.spark.{SparkException, SparkConf, SparkFunSuite}

/** Common tests for WriteAheadLogs that we would like to test with different configurations. */
abstract class CommonWriteAheadLogTests(
    allowBatching: Boolean,
    closeFileAfterWrite: Boolean,
    testTag: String = "")
  extends SparkFunSuite with BeforeAndAfter {

  import WriteAheadLogSuite._

  protected val hadoopConf = new Configuration()
  protected var tempDir: File = null
  protected var testDir: String = null
  protected var testFile: String = null
  protected var writeAheadLog: WriteAheadLog = null
  protected def testPrefix = if (testTag != "") testTag + " - " else testTag

  before {
    tempDir = Utils.createTempDir()
    testDir = tempDir.toString
    testFile = new File(tempDir, "testFile").toString
    if (writeAheadLog != null) {
      writeAheadLog.close()
      writeAheadLog = null
    }
  }

  after {
    Utils.deleteRecursively(tempDir)
  }

  test(testPrefix + "read all logs") {
    // Write data manually for testing reading through WriteAheadLog
    val writtenData = (1 to 10).map { i =>
      val data = generateRandomData()
      val file = testDir + s"/log-$i-$i"
      writeDataManually(data, file)
      data
    }.flatten

    val logDirectoryPath = new Path(testDir)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)
    assert(fileSystem.exists(logDirectoryPath) === true)

    // Read data using manager and verify
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData === writtenData)
  }

  test(testPrefix + "write logs") {
    // Write data with rotation using WriteAheadLog class
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite = closeFileAfterWrite,
      allowBatching = allowBatching)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val writtenData = readAndDeserializeDataManually(logFiles, allowBatching)
    assert(writtenData === dataToWrite)
  }

  test(testPrefix + "read all logs after write") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite, allowBatching)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(dataToWrite === readData)
  }

  test(testPrefix + "clean old logs") {
    logCleanUpTest(waitForCompletion = false)
  }

  test(testPrefix + "clean old logs synchronously") {
    logCleanUpTest(waitForCompletion = true)
  }

  private def logCleanUpTest(waitForCompletion: Boolean): Unit = {
    // Write data with manager, recover with new manager and verify
    val manualClock = new ManualClock
    val dataToWrite = generateRandomData()
    writeAheadLog = writeDataUsingWriteAheadLog(testDir, dataToWrite, closeFileAfterWrite,
      allowBatching, manualClock, closeLog = false)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)

    writeAheadLog.clean(manualClock.getTimeMillis() / 2, waitForCompletion)

    if (waitForCompletion) {
      assert(getLogFilesInDirectory(testDir).size < logFiles.size)
    } else {
      eventually(Eventually.timeout(1 second), interval(10 milliseconds)) {
        assert(getLogFilesInDirectory(testDir).size < logFiles.size)
      }
    }
  }

  test(testPrefix + "handling file errors while reading rotating logs") {
    // Generate a set of log files
    val manualClock = new ManualClock
    val dataToWrite1 = generateRandomData()
    writeDataUsingWriteAheadLog(testDir, dataToWrite1, closeFileAfterWrite, allowBatching,
      manualClock)
    val logFiles1 = getLogFilesInDirectory(testDir)
    assert(logFiles1.size > 1)


    // Recover old files and generate a second set of log files
    val dataToWrite2 = generateRandomData()
    manualClock.advance(100000)
    writeDataUsingWriteAheadLog(testDir, dataToWrite2, closeFileAfterWrite, allowBatching ,
      manualClock)
    val logFiles2 = getLogFilesInDirectory(testDir)
    assert(logFiles2.size > logFiles1.size)

    // Read the files and verify that all the written data can be read
    val readData1 = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData1 === (dataToWrite1 ++ dataToWrite2))

    // Corrupt the first set of files so that they are basically unreadable
    logFiles1.foreach { f =>
      val raf = new FileOutputStream(f, true).getChannel()
      raf.truncate(1)
      raf.close()
    }

    // Verify that the corrupted files do not prevent reading of the second set of data
    val readData = readDataUsingWriteAheadLog(testDir, closeFileAfterWrite, allowBatching)
    assert(readData === dataToWrite2)
  }

  test(testPrefix + "do not create directories or files unless write") {
    val nonexistentTempPath = File.createTempFile("test", "")
    nonexistentTempPath.delete()
    assert(!nonexistentTempPath.exists())

    val writtenSegment = writeDataManually(generateRandomData(), testFile)
    val wal = new FileBasedWriteAheadLog(new SparkConf(), tempDir.getAbsolutePath,
      new Configuration(), 1, 1, closeFileAfterWrite = false)
    assert(!nonexistentTempPath.exists(), "Directory created just by creating log object")
    wal.read(writtenSegment.head)
    assert(!nonexistentTempPath.exists(), "Directory created just by attempting to read segment")
  }
}

class FileBasedWriteAheadLogSuite
  extends CommonWriteAheadLogTests(false, false, "FileBasedWriteAheadLog") {

  import WriteAheadLogSuite._

  test("FileBasedWriteAheadLogWriter - writing data") {
    val dataToWrite = generateRandomData()
    val segments = writeDataUsingWriter(testFile, dataToWrite)
    val writtenData = readDataManually(segments)
    assert(writtenData === dataToWrite)
  }

  test("FileBasedWriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val dataToWrite = generateRandomData()
    val writer = new FileBasedWriteAheadLogWriter(testFile, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(stringToByteBuffer(data))
      val dataRead = readDataManually(Seq(segment)).head
      assert(data === dataRead)
    }
    writer.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data") {
    val writtenData = generateRandomData()
    writeDataManually(writtenData, testFile)
    val reader = new FileBasedWriteAheadLogReader(testFile, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData === writtenData)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("FileBasedWriteAheadLogReader - sequentially reading data written with writer") {
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val readData = readDataUsingReader(testFile)
    assert(readData === dataToWrite)
  }

  test("FileBasedWriteAheadLogReader - reading data written with writer after corrupted write") {
    // Write data manually for testing the sequential reader
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val fileLength = new File(testFile).length()

    // Append some garbage data to get the effect of a corrupted write
    val fw = new FileWriter(testFile, true)
    fw.append("This line appended to file!")
    fw.close()

    // Verify the data can be read and is same as the one correctly written
    assert(readDataUsingReader(testFile) === dataToWrite)

    // Corrupt the last correctly written file
    val raf = new FileOutputStream(testFile, true).getChannel()
    raf.truncate(fileLength - 1)
    raf.close()

    // Verify all the data except the last can be read
    assert(readDataUsingReader(testFile) === (dataToWrite.dropRight(1)))
  }

  test("FileBasedWriteAheadLogRandomReader - reading data using random reader") {
    // Write data manually for testing the random reader
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, testFile)

    // Get a random order of these segments and read them back
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("FileBasedWriteAheadLogRandomReader- reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val data = generateRandomData()
    val segments = writeDataUsingWriter(testFile, data)

    // Read a random sequence of segments and verify read data
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new FileBasedWriteAheadLogRandomReader(testFile, hadoopConf)
    dataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }
}

abstract class CloseFileAfterWriteTests(allowBatching: Boolean, testTag: String)
  extends CommonWriteAheadLogTests(allowBatching, closeFileAfterWrite = true, testTag) {

  import WriteAheadLogSuite._
  test(testPrefix + "close after write flag") {
    // Write data with rotation using WriteAheadLog class
    val numFiles = 3
    val dataToWrite = Seq.tabulate(numFiles)(_.toString)
    // total advance time is less than 1000, therefore log shouldn't be rolled, but manually closed
    writeDataUsingWriteAheadLog(testDir, dataToWrite, closeLog = false, clockAdvanceTime = 100,
      closeFileAfterWrite = true, allowBatching = allowBatching)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size === numFiles)
    val writtenData: Seq[String] = readAndDeserializeDataManually(logFiles, allowBatching)
    assert(writtenData === dataToWrite)
  }
}

class FileBasedWriteAheadLogWithFileCloseAfterWriteSuite
  extends CloseFileAfterWriteTests(allowBatching = false, "FileBasedWriteAheadLog")

class BatchedWriteAheadLogSuite extends CommonWriteAheadLogTests(
    allowBatching = true,
    closeFileAfterWrite = false,
    "BatchedWriteAheadLog") with MockitoSugar with BeforeAndAfterEach with PrivateMethodTester {

  import BatchedWriteAheadLog._
  import WriteAheadLogSuite._

  private var fileBasedWAL: FileBasedWriteAheadLog = _
  private var walHandle: FileBasedWriteAheadLogSegment = _
  private var walBatchingThreadPool: ExecutionContextExecutorService = _

  override def beforeEach(): Unit = {
    fileBasedWAL = mock[FileBasedWriteAheadLog]
    walHandle = mock[FileBasedWriteAheadLogSegment]
    walBatchingThreadPool = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonFixedThreadPool(8, "wal-test-thread-pool"))
  }

  override def afterEach(): Unit = {
    if (walBatchingThreadPool != null) {
      walBatchingThreadPool.shutdownNow()
    }
  }

  test("BatchedWriteAheadLog - serializing and deserializing batched records") {
    val events = Seq(
      BlockAdditionEvent(ReceivedBlockInfo(0, None, None, null)),
      BatchAllocationEvent(null, null),
      BatchCleanupEvent(Nil)
    )

    val buffers = events.map(e => Record(ByteBuffer.wrap(Utils.serialize(e)), 0L, null))
    val batched = BatchedWriteAheadLog.aggregate(buffers)
    val deaggregate = BatchedWriteAheadLog.deaggregate(batched).map(buffer =>
      Utils.deserialize[ReceivedBlockTrackerLogEvent](buffer.array()))

    assert(deaggregate.toSeq === events)
  }

  test("BatchedWriteAheadLog - failures in wrappedLog get bubbled up") {
    when(fileBasedWAL.write(any[ByteBuffer], anyLong)).thenThrow(new RuntimeException("Hello!"))
    // the BatchedWriteAheadLog should bubble up any exceptions that may have happened during writes
    val wal = new BatchedWriteAheadLog(fileBasedWAL)

    intercept[RuntimeException] {
      val buffer = mock[ByteBuffer]
      wal.write(buffer, 2L)
    }
  }

  // we make the write requests in separate threads so that we don't block the test thread
  private def eventFuture(
      wal: WriteAheadLog,
      event: String,
      time: Long,
      numSuccess: AtomicInteger = null,
      numFail: AtomicInteger = null): Unit = {
    val f = Future(wal.write(event, time))(walBatchingThreadPool)
    f.onComplete {
      case Success(v) =>
        assert(v === walHandle) // return our mock handle after the write
        if (numSuccess != null) numSuccess.incrementAndGet()
      case Failure(v) => if (numFail != null) numFail.incrementAndGet()
    }(walBatchingThreadPool)
  }

  /**
   * In order to block the writes on the writer thread, we mock the write method, and block it
   * for some time with a promise.
   */
  private def writeBlockingPromise(wal: WriteAheadLog): Promise[Any] = {
    // we would like to block the write so that we can queue requests
    val promise = Promise[Any]()
    when(wal.write(any[ByteBuffer], any[Long])).thenAnswer(
      new Answer[FileBasedWriteAheadLogSegment] {
        override def answer(invocation: InvocationOnMock): FileBasedWriteAheadLogSegment = {
          Await.ready(promise.future, 4.seconds)
          walHandle
        }
      }
    )
    promise
  }

  test("BatchedWriteAheadLog - records get added to a queue") {
    val numSuccess = new AtomicInteger()
    val numFail = new AtomicInteger()
    val wal = new BatchedWriteAheadLog(fileBasedWAL)

    val promise = writeBlockingPromise(fileBasedWAL)

    // make sure queue is empty initially
    assert(wal.getQueueLength === 0)
    val event1 = "hello"
    val event2 = "world"
    val event3 = "this"
    val event4 = "is"
    val event5 = "doge"

    eventFuture(wal, event1, 5L, numSuccess, numFail)
    eventFuture(wal, event2, 10L, numSuccess, numFail)
    eventFuture(wal, event3, 11L, numSuccess, numFail)
    eventFuture(wal, event4, 12L, numSuccess, numFail)
    eventFuture(wal, event5, 20L, numSuccess, numFail)

    eventually(Eventually.timeout(2 seconds)) {
      // the first element will immediately be taken and the rest will get queued
      assert(wal.getQueueLength() == 4)
    }
    assert(numSuccess.get() === 0)
    assert(numFail.get() === 0)
    // remove block so that the writes are made
    promise.success(null)

    eventually(Eventually.timeout(2 seconds)) {
      assert(wal.getQueueLength() == 0)
      assert(numSuccess.get() === 5)
      assert(numFail.get() == 0)
    }
  }

  test("BatchedWriteAheadLog - name log with aggregated entries with the timestamp of last entry") {
    val wal = new BatchedWriteAheadLog(fileBasedWAL)
    // block the write so that we can batch some records
    val promise = writeBlockingPromise(fileBasedWAL)

    val event1 = "hello"
    val event2 = "world"
    val event3 = "this"
    val event4 = "is"
    val event5 = "doge"

    eventFuture(wal, event1, 3L) // 3 will automatically be flushed for the first write
    // rest of the records will be batched while it takes 3 to get written
    eventFuture(wal, event2, 5L)
    eventFuture(wal, event3, 8L)
    eventFuture(wal, event4, 12L)
    eventFuture(wal, event5, 10L)
    promise.success(true)

    verify(fileBasedWAL, times(1)).write(any[ByteBuffer], meq(3L))

    eventually(Eventually.timeout(1 second)) {
      // the file name should be the timestamp of the last record, as events should be naturally
      // in order of timestamp, and we need the last element.
      verify(fileBasedWAL, times(1)).write(any[ByteBuffer], meq(10L))
    }
  }

  test("BatchedWriteAheadLog - shutdown properly") {
    val wal = new BatchedWriteAheadLog(fileBasedWAL)
    wal.close()
    verify(fileBasedWAL, times(1)).close()
  }
}

class BatchedWriteAheadLogWithCloseFileAfterWriteSuite
  extends CloseFileAfterWriteTests(allowBatching = true, "BatchedWriteAheadLog")

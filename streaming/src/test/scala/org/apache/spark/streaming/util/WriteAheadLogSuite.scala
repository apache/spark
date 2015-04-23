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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import WriteAheadLogSuite._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.{ManualClock, Utils}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually._

class WriteAheadLogSuite extends FunSuite with BeforeAndAfter {

  val hadoopConf = new Configuration()
  var tempDir: File = null
  var testDir: String = null
  var testFile: String = null
  var manager: WriteAheadLogManager = null

  before {
    tempDir = Utils.createTempDir()
    testDir = tempDir.toString
    testFile = new File(tempDir, "testFile").toString
    if (manager != null) {
      manager.stop()
      manager = null
    }
  }

  after {
    Utils.deleteRecursively(tempDir)
  }

  test("WriteAheadLogWriter - writing data") {
    val dataToWrite = generateRandomData()
    val segments = writeDataUsingWriter(testFile, dataToWrite)
    val writtenData = readDataManually(segments)
    assert(writtenData === dataToWrite)
  }

  test("WriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter(testFile, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(stringToByteBuffer(data))
      val dataRead = readDataManually(Seq(segment)).head
      assert(data === dataRead)
    }
    writer.close()
  }

  test("WriteAheadLogReader - sequentially reading data") {
    val writtenData = generateRandomData()
    writeDataManually(writtenData, testFile)
    val reader = new WriteAheadLogReader(testFile, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData === writtenData)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("WriteAheadLogReader - sequentially reading data written with writer") {
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(testFile, dataToWrite)
    val readData = readDataUsingReader(testFile)
    assert(readData === dataToWrite)
  }

  test("WriteAheadLogReader - reading data written with writer after corrupted write") {
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

  test("WriteAheadLogRandomReader - reading data using random reader") {
    // Write data manually for testing the random reader
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, testFile)

    // Get a random order of these segments and read them back
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader(testFile, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val data = generateRandomData()
    val segments = writeDataUsingWriter(testFile, data)

    // Read a random sequence of segments and verify read data
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader(testFile, hadoopConf)
    dataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogManager - write rotating logs") {
    // Write data using manager
    val dataToWrite = generateRandomData()
    writeDataUsingManager(testDir, dataToWrite)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val writtenData = logFiles.flatMap { file => readDataManually(file)}
    assert(writtenData === dataToWrite)
  }

  test("WriteAheadLogManager - read rotating logs") {
    // Write data manually for testing reading through manager
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
    val readData = readDataUsingManager(testDir)
    assert(readData === writtenData)
  }

  test("WriteAheadLogManager - recover past logs when creating new manager") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData()
    writeDataUsingManager(testDir, dataToWrite)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)
    val readData = readDataUsingManager(testDir)
    assert(dataToWrite === readData)
  }

  test("WriteAheadLogManager - cleanup old logs") {
    logCleanUpTest(waitForCompletion = false)
  }

  test("WriteAheadLogManager - cleanup old logs synchronously") {
    logCleanUpTest(waitForCompletion = true)
  }

  private def logCleanUpTest(waitForCompletion: Boolean): Unit = {
    // Write data with manager, recover with new manager and verify
    val manualClock = new ManualClock
    val dataToWrite = generateRandomData()
    manager = writeDataUsingManager(testDir, dataToWrite, manualClock, stopManager = false)
    val logFiles = getLogFilesInDirectory(testDir)
    assert(logFiles.size > 1)

    manager.cleanupOldLogs(manualClock.getTimeMillis() / 2, waitForCompletion)

    if (waitForCompletion) {
      assert(getLogFilesInDirectory(testDir).size < logFiles.size)
    } else {
      eventually(timeout(1 second), interval(10 milliseconds)) {
        assert(getLogFilesInDirectory(testDir).size < logFiles.size)
      }
    }
  }

  test("WriteAheadLogManager - handling file errors while reading rotating logs") {
    // Generate a set of log files
    val manualClock = new ManualClock
    val dataToWrite1 = generateRandomData()
    writeDataUsingManager(testDir, dataToWrite1, manualClock)
    val logFiles1 = getLogFilesInDirectory(testDir)
    assert(logFiles1.size > 1)


    // Recover old files and generate a second set of log files
    val dataToWrite2 = generateRandomData()
    manualClock.advance(100000)
    writeDataUsingManager(testDir, dataToWrite2, manualClock)
    val logFiles2 = getLogFilesInDirectory(testDir)
    assert(logFiles2.size > logFiles1.size)

    // Read the files and verify that all the written data can be read
    val readData1 = readDataUsingManager(testDir)
    assert(readData1 === (dataToWrite1 ++ dataToWrite2))

    // Corrupt the first set of files so that they are basically unreadable
    logFiles1.foreach { f =>
      val raf = new FileOutputStream(f, true).getChannel()
      raf.truncate(1)
      raf.close()
    }

    // Verify that the corrupted files do not prevent reading of the second set of data
    val readData = readDataUsingManager(testDir)
    assert(readData === dataToWrite2)
  }
}

object WriteAheadLogSuite {

  private val hadoopConf = new Configuration()

  /** Write data to a file directly and return an array of the file segments written. */
  def writeDataManually(data: Seq[String], file: String): Seq[WriteAheadLogFileSegment] = {
    val segments = new ArrayBuffer[WriteAheadLogFileSegment]()
    val writer = HdfsUtils.getOutputStream(file, hadoopConf)
    data.foreach { item =>
      val offset = writer.getPos
      val bytes = Utils.serialize(item)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += WriteAheadLogFileSegment(file, offset, bytes.size)
    }
    writer.close()
    segments
  }

  /**
   * Write data to a file using the writer class and return an array of the file segments written.
   */
  def writeDataUsingWriter(filePath: String, data: Seq[String]): Seq[WriteAheadLogFileSegment] = {
    val writer = new WriteAheadLogWriter(filePath, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  /** Write data to rotating files in log directory using the manager class. */
  def writeDataUsingManager(
      logDirectory: String,
      data: Seq[String],
      manualClock: ManualClock = new ManualClock,
      stopManager: Boolean = true
    ): WriteAheadLogManager = {
    if (manualClock.getTimeMillis() < 100000) manualClock.setTime(10000)
    val manager = new WriteAheadLogManager(logDirectory, hadoopConf,
      rollingIntervalSecs = 1, callerName = "WriteAheadLogSuite", clock = manualClock)
    // Ensure that 500 does not get sorted after 2000, so put a high base value.
    data.foreach { item =>
      manualClock.advance(500)
      manager.writeToLog(item)
    }
    if (stopManager) manager.stop()
    manager
  }

  /** Read data from a segments of a log file directly and return the list of byte buffers. */
  def readDataManually(segments: Seq[WriteAheadLogFileSegment]): Seq[String] = {
    segments.map { segment =>
      val reader = HdfsUtils.getInputStream(segment.path, hadoopConf)
      try {
        reader.seek(segment.offset)
        val bytes = new Array[Byte](segment.length)
        reader.readInt()
        reader.readFully(bytes)
        val data = Utils.deserialize[String](bytes)
        reader.close()
        data
      } finally {
        reader.close()
      }
    }
  }

  /** Read all the data from a log file directly and return the list of byte buffers. */
  def readDataManually(file: String): Seq[String] = {
    val reader = HdfsUtils.getInputStream(file, hadoopConf)
    val buffer = new ArrayBuffer[String]
    try {
      while (true) {
        // Read till EOF is thrown
        val length = reader.readInt()
        val bytes = new Array[Byte](length)
        reader.read(bytes)
        buffer += Utils.deserialize[String](bytes)
      }
    } catch {
      case ex: EOFException =>
    } finally {
      reader.close()
    }
    buffer
  }

  /** Read all the data from a log file using reader class and return the list of byte buffers. */
  def readDataUsingReader(file: String): Seq[String] = {
    val reader = new WriteAheadLogReader(file, hadoopConf)
    val readData = reader.toList.map(byteBufferToString)
    reader.close()
    readData
  }

  /** Read all the data in the log file in a directory using the manager class. */
  def readDataUsingManager(logDirectory: String): Seq[String] = {
    val manager = new WriteAheadLogManager(logDirectory, hadoopConf,
      callerName = "WriteAheadLogSuite")
    val data = manager.readFromLog().map(byteBufferToString).toSeq
    manager.stop()
    data
  }

  /** Get the log files in a direction */
  def getLogFilesInDirectory(directory: String): Seq[String] = {
    val logDirectoryPath = new Path(directory)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      fileSystem.listStatus(logDirectoryPath).map { _.getPath() }.sortBy {
        _.getName().split("-")(1).toLong
      }.map {
        _.toString.stripPrefix("file:")
      }
    } else {
      Seq.empty
    }
  }

  def generateRandomData(): Seq[String] = {
    (1 to 100).map { _.toString }
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}

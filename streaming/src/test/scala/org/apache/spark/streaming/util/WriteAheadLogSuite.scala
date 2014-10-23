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

import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps
import scala.util.Random

import WriteAheadLogSuite._
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.util.Utils
import org.scalatest.concurrent.Eventually._

class WriteAheadLogSuite extends FunSuite with BeforeAndAfter {

  val hadoopConf = new Configuration()
  var tempDir: File = null
  var dirForTest: String = null
  var fileForTest: String = null

  before {
    tempDir = Files.createTempDir()
    dirForTest = "file:///" + tempDir.toString
    fileForTest = "file:///" + new File(tempDir, getRandomString()).toString
  }

  after {
    FileUtils.deleteDirectory(tempDir)
  }

  test("WriteAheadLogWriter - writing data") {
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter(fileForTest, hadoopConf)
    val segments = dataToWrite.map(data => writer.write(data))
    writer.close()
    val writtenData = readDataManually(fileForTest, segments)
    assert(writtenData.toArray === dataToWrite.toArray)
  }

  test("WriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter(fileForTest, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(stringToByteBuffer(data))
      val reader = new WriteAheadLogRandomReader(fileForTest, hadoopConf)
      val dataRead = reader.read(segment)
      assert(data === byteBufferToString(dataRead))
    }
    writer.close()
  }

  test("WriteAheadLogReader - sequentially reading data") {
    // Write data manually for testing the sequential reader
    val writtenData = generateRandomData()
    writeDataManually(writtenData, fileForTest)
    val reader = new WriteAheadLogReader(fileForTest, hadoopConf)
    val readData = reader.toSeq.map(byteBufferToString)
    assert(readData.toList === writtenData.toList)
    assert(reader.hasNext === false)
    intercept[Exception] {
      reader.next()
    }
    reader.close()
  }

  test("WriteAheadLogReader - sequentially reading data written with writer") {
    // Write data manually for testing the sequential reader
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(fileForTest, dataToWrite)
    val iter = dataToWrite.iterator
    val reader = new WriteAheadLogReader(fileForTest, hadoopConf)
    reader.foreach { byteBuffer =>
      assert(byteBufferToString(byteBuffer) === iter.next())
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader") {
    // Write data manually for testing the random reader
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, fileForTest)

    // Get a random order of these segments and read them back
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader(fileForTest, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val data = generateRandomData()
    val segments = writeDataUsingWriter(fileForTest, data)

    // Read a random sequence of segments and verify read data
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader(fileForTest, hadoopConf)
    dataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogManager - write rotating logs") {
    // Write data using manager
    val dataToWrite = generateRandomData()
    writeDataUsingManager(dirForTest, dataToWrite)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(dirForTest)
    assert(logFiles.size > 1)
    val writtenData = logFiles.flatMap { file => readDataManually(file)}
    assert(writtenData.toList === dataToWrite.toList)
  }

  test("WriteAheadLogManager - read rotating logs") {
    // Write data manually for testing reading through manager
    val writtenData = (1 to 10).map { i =>
      val data = generateRandomData()
      val file = dirForTest + s"/log-$i-$i"
      writeDataManually(data, file)
      data
    }.flatten

    val logDirectoryPath = new Path(dirForTest)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)
    assert(fileSystem.exists(logDirectoryPath) === true)

    // Read data using manager and verify
    val readData = readDataUsingManager(dirForTest)
    assert(readData.toList === writtenData.toList)
  }

  test("WriteAheadLogManager - recover past logs when creating new manager") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData()
    writeDataUsingManager(dirForTest, dataToWrite)
    val logFiles = getLogFilesInDirectory(dirForTest)
    assert(logFiles.size > 1)
    val readData = readDataUsingManager(dirForTest)
    assert(dataToWrite.toList === readData.toList)
  }

  test("WriteAheadLogManager - cleanup old logs") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData()
    val fakeClock = new ManualClock
    val manager = new WriteAheadLogManager(dirForTest, hadoopConf,
      rollingIntervalSecs = 1, callerName = "WriteAheadLogSuite", clock = fakeClock)
    dataToWrite.foreach { item =>
      fakeClock.addToTime(500) // half second for each
      manager.writeToLog(item)
    }
    val logFiles = getLogFilesInDirectory(dirForTest)
    assert(logFiles.size > 1)
    manager.cleanupOldLogs(fakeClock.currentTime() / 2)
    eventually(timeout(1 second), interval(10 milliseconds)) {
      assert(getLogFilesInDirectory(dirForTest).size < logFiles.size)
    }
  }
  // TODO (Hari, TD): Test different failure conditions of writers and readers.
  //  - Failure while reading incomplete/corrupt file
}

object WriteAheadLogSuite {

  private val hadoopConf = new Configuration()

  /**
   * Write data to the file and returns the an array of the bytes written.
   * This is used to test the WAL reader independently of the WAL writer.
   */
  def writeDataManually(data: Seq[String], file: String): Seq[FileSegment] = {
    val segments = new ArrayBuffer[FileSegment]()
    val writer = HdfsUtils.getOutputStream(file, hadoopConf)
    data.foreach { item =>
      val offset = writer.getPos
      val bytes = Utils.serialize(item)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += FileSegment(file, offset, bytes.size)
    }
    writer.close()
    segments
  }

  def getRandomString(): String = {
    new String(Random.alphanumeric.take(6).toArray)
  }

  def writeDataUsingWriter(filePath: String, data: Seq[String]): Seq[FileSegment] = {
    val writer = new WriteAheadLogWriter(filePath, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  def writeDataUsingManager(logDirectory: String, data: Seq[String]) {
    val fakeClock = new ManualClock
    fakeClock.setTime(1000000)
    val manager = new WriteAheadLogManager(logDirectory, hadoopConf,
      rollingIntervalSecs = 1, callerName = "WriteAheadLogSuite", clock = fakeClock)
    // Ensure that 500 does not get sorted after 2000, so put a high base value.
    data.foreach { item =>
      fakeClock.addToTime(500)
      manager.writeToLog(item)
    }
    manager.stop()
  }

  /**
   * Read data from the given segments of log file and returns the read list of byte buffers.
   * This is used to test the WAL writer independently of the WAL reader.
   */
  def readDataManually(file: String, segments: Seq[FileSegment]): Seq[String] = {
    val reader = HdfsUtils.getInputStream(file, hadoopConf)
    segments.map { x =>
      reader.seek(x.offset)
      val data = new Array[Byte](x.length)
      reader.readInt()
      reader.readFully(data)
      Utils.deserialize[String](data)
    }
  }

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

  def readDataUsingManager(logDirectory: String): Seq[String] = {
    val manager = new WriteAheadLogManager(logDirectory, hadoopConf,
      callerName = "WriteAheadLogSuite")
    val data = manager.readFromLog().map(byteBufferToString).toSeq
    manager.stop()
    data
  }

  def generateRandomData(): Seq[String] = {
    (1 to 50).map {
      _.toString
    }
  }

  def getLogFilesInDirectory(directory: String): Seq[String] = {
    val logDirectoryPath = new Path(directory)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      fileSystem.listStatus(logDirectoryPath).map {
        _.getPath.toString
      }.sorted
    } else {
      Seq.empty
    }
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}

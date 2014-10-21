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
package org.apache.spark.streaming.storage

import java.io.{DataInputStream, FileInputStream, File, RandomAccessFile}
import java.nio.ByteBuffer

import scala.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.util.ManualClock
import org.apache.spark.util.Utils
import WriteAheadLogSuite._

class WriteAheadLogSuite extends FunSuite with BeforeAndAfter {

  val hadoopConf = new Configuration()
  var tempDirectory: File = null

  before {
    tempDirectory = Files.createTempDir()
  }

  after {
    if (tempDirectory != null && tempDirectory.exists()) {
      FileUtils.deleteDirectory(tempDirectory)
      tempDirectory = null
    }
  }

  test("WriteAheadLogWriter - writing data") {
    val file = new File(tempDirectory, Random.nextString(10))
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter("file:///" + file, hadoopConf)
    val segments = dataToWrite.map(data => writer.write(data))
    writer.close()
    val writtenData = readDataManually(file, segments)
    assert(writtenData.toArray === dataToWrite.toArray)
  }

  test("WriteAheadLogWriter - syncing of data by writing and reading immediately") {
    val file = new File(tempDirectory, Random.nextString(10))
    val dataToWrite = generateRandomData()
    val writer = new WriteAheadLogWriter("file:///" + file, hadoopConf)
    dataToWrite.foreach { data =>
      val segment = writer.write(data)
      assert(readDataManually(file, Seq(segment)).head === data)
    }
    writer.close()
  }

  test("WriteAheadLogReader - sequentially reading data") {
    // Write data manually for testing the sequential reader
    val file = File.createTempFile("TestSequentialReads", "", tempDirectory)
    val writtenData = generateRandomData()
    writeDataManually(writtenData, file)
    val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
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
    val file = new File(tempDirectory, "TestWriter")
    val dataToWrite = generateRandomData()
    writeDataUsingWriter(file, dataToWrite)
    val iter = dataToWrite.iterator
    val reader = new WriteAheadLogReader("file:///" + file.toString, hadoopConf)
    reader.foreach { byteBuffer =>
      assert(byteBufferToString(byteBuffer) === iter.next())
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader") {
    // Write data manually for testing the random reader
    val file = File.createTempFile("TestRandomReads", "", tempDirectory)
    val writtenData = generateRandomData()
    val segments = writeDataManually(writtenData, file)

    // Get a random order of these segments and read them back
    val writtenDataAndSegments = writtenData.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
    writtenDataAndSegments.foreach { case (data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogRandomReader - reading data using random reader written with writer") {
    // Write data using writer for testing the random reader
    val file = new File(tempDirectory, "TestRandomReads")
    val data = generateRandomData()
    val segments = writeDataUsingWriter(file, data)

    // Read a random sequence of segments and verify read data
    val dataAndSegments = data.zip(segments).toSeq.permutations.take(10).flatten
    val reader = new WriteAheadLogRandomReader("file:///" + file.toString, hadoopConf)
    dataAndSegments.foreach { case(data, segment) =>
      assert(data === byteBufferToString(reader.read(segment)))
    }
    reader.close()
  }

  test("WriteAheadLogManager - write rotating logs") {
    // Write data using manager
    val dataToWrite = generateRandomData(10)
    writeDataUsingManager(tempDirectory, dataToWrite)

    // Read data manually to verify the written data
    val logFiles = getLogFilesInDirectory(tempDirectory)
    assert(logFiles.size > 1)
    val writtenData = logFiles.flatMap { file => readDataManually(file) }
    assert(writtenData.toList === dataToWrite.toList)
  }

  test("WriteAheadLogManager - read rotating logs") {
    // Write data manually for testing reading through manager
    val writtenData = (1 to 10).map { i =>
      val data = generateRandomData(10)
      val file = new File(tempDirectory, s"log-$i-${i + 1}")
      writeDataManually(data, file)
      data
    }.flatten

    // Read data using manager and verify
    val readData = readDataUsingManager(tempDirectory)
    assert(readData.toList === writtenData.toList)
  }

  test("WriteAheadLogManager - recover past logs when creating new manager") {
    // Write data with manager, recover with new manager and verify
    val dataToWrite = generateRandomData(100)
    writeDataUsingManager(tempDirectory, dataToWrite)
    val logFiles = getLogFilesInDirectory(tempDirectory)
    assert(logFiles.size > 1)
    val readData = readDataUsingManager(tempDirectory)
    assert(dataToWrite.toList === readData.toList)
  }

  // TODO (Hari, TD): Test different failure conditions of writers and readers.
  //  - Failure in the middle of write
  //  - Failure while reading incomplete/corrupt file
}

object WriteAheadLogSuite {

  private val hadoopConf = new Configuration()

  /**
   * Write data to the file and returns the an array of the bytes written.
   * This is used to test the WAL reader independently of the WAL writer.
   */
  def writeDataManually(data: Seq[String], file: File): Seq[FileSegment] = {
    val segments = new ArrayBuffer[FileSegment]()
    val writer = new RandomAccessFile(file, "rw")
    data.foreach { item =>
      val offset = writer.getFilePointer()
      val bytes = Utils.serialize(item)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += FileSegment(file.toString, offset, bytes.size)
    }
    writer.close()
    segments
  }

  def writeDataUsingWriter(file: File, data: Seq[String]): Seq[FileSegment] = {
    val writer = new WriteAheadLogWriter(file.toString, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  def writeDataUsingManager(logDirectory: File, data: Seq[String]) {
    val fakeClock = new ManualClock
    val manager = new WriteAheadLogManager(logDirectory.toString, hadoopConf,
      rollingIntervalSecs = 1, callerName = "WriteAheadLogSuite", clock = fakeClock)
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
  def readDataManually(file: File, segments: Seq[FileSegment]): Seq[String] = {
    val reader = new RandomAccessFile(file, "r")
    segments.map { x =>
      reader.seek(x.offset)
      val data = new Array[Byte](x.length)
      reader.readInt()
      reader.readFully(data)
      Utils.deserialize[String](data)
    }
  }

  def readDataManually(file: File): Seq[String] = {
    val reader = new DataInputStream(new FileInputStream(file))
    val buffer = new ArrayBuffer[String]
    try {
      while (reader.available > 0) {
        val length = reader.readInt()
        val bytes = new Array[Byte](length)
        reader.read(bytes)
        buffer += Utils.deserialize[String](bytes)
      }
    } finally {
      reader.close()
    }
    buffer
  }

  def readDataUsingManager(logDirectory: File): Seq[String] = {
    val manager = new WriteAheadLogManager(logDirectory.toString, hadoopConf,
      callerName = "WriteAheadLogSuite")
    val data = manager.readFromLog().map(byteBufferToString).toSeq
    manager.stop()
    data
  }

  def generateRandomData(numItems: Int = 50, itemSize: Int = 50): Seq[String] = {
    (1 to numItems).map { _.toString }
  }

  def getLogFilesInDirectory(directory: File): Seq[File] = {
    if (directory.exists) {
      directory.listFiles().filter(_.getName().startsWith("log-"))
        .sortBy(_.getName.split("-")(1).toLong)
    } else {
      Seq.empty
    }
  }

  def printData(data: Seq[String]) {
    println("# items in data = " + data.size)
    println(data.mkString("\n"))
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}

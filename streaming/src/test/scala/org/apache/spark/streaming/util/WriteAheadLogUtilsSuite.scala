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

import java.io.EOFException
import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkException, SparkConf, SparkFunSuite}
import org.apache.spark.util.{ManualClock, Utils}

class WriteAheadLogUtilsSuite extends SparkFunSuite {
  import WriteAheadLogSuite._

  private val logDir = Utils.createTempDir().getAbsolutePath()
  private val hadoopConf = new Configuration()

  def assertDriverLogClass[T <: WriteAheadLog: ClassTag](
      conf: SparkConf,
      isBatched: Boolean = false): WriteAheadLog = {
    val log = WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    if (isBatched) {
      assert(log.isInstanceOf[BatchedWriteAheadLog])
      val parentLog = log.asInstanceOf[BatchedWriteAheadLog].wrappedLog
      assert(parentLog.getClass === implicitly[ClassTag[T]].runtimeClass)
    } else {
      assert(log.getClass === implicitly[ClassTag[T]].runtimeClass)
    }
    log
  }

  def assertReceiverLogClass[T <: WriteAheadLog: ClassTag](conf: SparkConf): WriteAheadLog = {
    val log = WriteAheadLogUtils.createLogForReceiver(conf, logDir, hadoopConf)
    assert(log.getClass === implicitly[ClassTag[T]].runtimeClass)
    log
  }

  test("log selection and creation") {

    val emptyConf = new SparkConf()  // no log configuration
    assertDriverLogClass[FileBasedWriteAheadLog](emptyConf)
    assertReceiverLogClass[FileBasedWriteAheadLog](emptyConf)

    // Verify setting driver WAL class
    val driverWALConf = new SparkConf().set("spark.streaming.driver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[MockWriteAheadLog0](driverWALConf)
    assertReceiverLogClass[FileBasedWriteAheadLog](driverWALConf)

    // Verify setting receiver WAL class
    val receiverWALConf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[FileBasedWriteAheadLog](receiverWALConf)
    assertReceiverLogClass[MockWriteAheadLog0](receiverWALConf)

    // Verify setting receiver WAL class with 1-arg constructor
    val receiverWALConf2 = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog1].getName())
    assertReceiverLogClass[MockWriteAheadLog1](receiverWALConf2)

    // Verify failure setting receiver WAL class with 2-arg constructor
    intercept[SparkException] {
      val receiverWALConf3 = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
        classOf[MockWriteAheadLog2].getName())
      assertReceiverLogClass[MockWriteAheadLog1](receiverWALConf3)
    }
  }

  test("wrap WriteAheadLog in BatchedWriteAheadLog when batching is enabled") {
    def getBatchedSparkConf: SparkConf =
      new SparkConf().set("spark.streaming.driver.writeAheadLog.allowBatching", "true")

    val justBatchingConf = getBatchedSparkConf
    assertDriverLogClass[FileBasedWriteAheadLog](justBatchingConf, isBatched = true)
    assertReceiverLogClass[FileBasedWriteAheadLog](justBatchingConf)

    // Verify setting driver WAL class
    val driverWALConf = getBatchedSparkConf.set("spark.streaming.driver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[MockWriteAheadLog0](driverWALConf, isBatched = true)
    assertReceiverLogClass[FileBasedWriteAheadLog](driverWALConf)

    // Verify receivers are not wrapped
    val receiverWALConf = getBatchedSparkConf.set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[FileBasedWriteAheadLog](receiverWALConf, isBatched = true)
    assertReceiverLogClass[MockWriteAheadLog0](receiverWALConf)
  }
}

object WriteAheadLogSuite {

  class MockWriteAheadLog0() extends WriteAheadLog {
    override def write(record: ByteBuffer, time: Long): WriteAheadLogRecordHandle = { null }
    override def read(handle: WriteAheadLogRecordHandle): ByteBuffer = { null }
    override def readAll(): util.Iterator[ByteBuffer] = { null }
    override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = { }
    override def close(): Unit = { }
  }

  class MockWriteAheadLog1(val conf: SparkConf) extends MockWriteAheadLog0()

  class MockWriteAheadLog2(val conf: SparkConf, x: Int) extends MockWriteAheadLog0()

  private val hadoopConf = new Configuration()

  /** Write data to a file directly and return an array of the file segments written. */
  def writeDataManually(data: Seq[String], file: String): Seq[FileBasedWriteAheadLogSegment] = {
    val segments = new ArrayBuffer[FileBasedWriteAheadLogSegment]()
    val writer = HdfsUtils.getOutputStream(file, hadoopConf)
    data.foreach { item =>
      val offset = writer.getPos
      val bytes = Utils.serialize(item)
      writer.writeInt(bytes.size)
      writer.write(bytes)
      segments += FileBasedWriteAheadLogSegment(file, offset, bytes.size)
    }
    writer.close()
    segments
  }

  /**
   * Write data to a file using the writer class and return an array of the file segments written.
   */
  def writeDataUsingWriter(
      filePath: String,
      data: Seq[String]): Seq[FileBasedWriteAheadLogSegment] = {
    val writer = new FileBasedWriteAheadLogWriter(filePath, hadoopConf)
    val segments = data.map {
      item => writer.write(item)
    }
    writer.close()
    segments
  }

  /** Write data to rotating files in log directory using the WriteAheadLog class. */
  def writeDataUsingWriteAheadLog(
      logDirectory: String,
      data: Seq[String],
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean,
      manualClock: ManualClock = new ManualClock,
      closeLog: Boolean = true,
      clockAdvanceTime: Int = 500): WriteAheadLog = {
    if (manualClock.getTimeMillis() < 100000) manualClock.setTime(10000)
    val wal = createWriteAheadLog(logDirectory, closeFileAfterWrite, allowBatching)

    // Ensure that 500 does not get sorted after 2000, so put a high base value.
    data.foreach { item =>
      manualClock.advance(clockAdvanceTime)
      wal.write(item, manualClock.getTimeMillis())
    }
    if (closeLog) wal.close()
    wal
  }

  /** Read data from a segments of a log file directly and return the list of byte buffers. */
  def readDataManually(segments: Seq[FileBasedWriteAheadLogSegment]): Seq[String] = {
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
  def readDataManually[T](file: String): Seq[T] = {
    val reader = HdfsUtils.getInputStream(file, hadoopConf)
    val buffer = new ArrayBuffer[T]
    try {
      while (true) {
        // Read till EOF is thrown
        val length = reader.readInt()
        val bytes = new Array[Byte](length)
        reader.read(bytes)
        buffer += Utils.deserialize[T](bytes)
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
    val reader = new FileBasedWriteAheadLogReader(file, hadoopConf)
    val readData = reader.toList.map(byteBufferToString)
    reader.close()
    readData
  }

  /** Read all the data in the log file in a directory using the WriteAheadLog class. */
  def readDataUsingWriteAheadLog(
      logDirectory: String,
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean): Seq[String] = {
    val wal = createWriteAheadLog(logDirectory, closeFileAfterWrite, allowBatching)
    val data = wal.readAll().asScala.map(byteBufferToString).toSeq
    wal.close()
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

  def createWriteAheadLog(
      logDirectory: String,
      closeFileAfterWrite: Boolean,
      allowBatching: Boolean): WriteAheadLog = {
    val sparkConf = new SparkConf
    val wal = new FileBasedWriteAheadLog(sparkConf, logDirectory, hadoopConf, 1, 1,
      closeFileAfterWrite)
    if (allowBatching) new BatchedWriteAheadLog(wal, sparkConf) else wal
  }

  def generateRandomData(): Seq[String] = {
    (1 to 100).map { _.toString }
  }

  def readAndDeserializeDataManually(logFiles: Seq[String], allowBatching: Boolean): Seq[String] = {
    if (allowBatching) {
      logFiles.flatMap { file =>
        val data = readDataManually[Array[Array[Byte]]](file)
        data.flatMap(byteArray => byteArray.map(Utils.deserialize[String]))
      }
    } else {
      logFiles.flatMap { file => readDataManually[String](file)}
    }
  }

  implicit def stringToByteBuffer(str: String): ByteBuffer = {
    ByteBuffer.wrap(Utils.serialize(str))
  }

  implicit def byteBufferToString(byteBuffer: ByteBuffer): String = {
    Utils.deserialize[String](byteBuffer.array)
  }
}

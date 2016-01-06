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

import java.nio.ByteBuffer
import java.util

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.util.Utils

class WriteAheadLogUtilsSuite extends SparkFunSuite {
  import WriteAheadLogUtilsSuite._

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
    assertDriverLogClass[FileBasedWriteAheadLog](emptyConf, isBatched = true)
    assertReceiverLogClass[FileBasedWriteAheadLog](emptyConf)

    // Verify setting driver WAL class
    val driverWALConf = new SparkConf().set("spark.streaming.driver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[MockWriteAheadLog0](driverWALConf, isBatched = true)
    assertReceiverLogClass[FileBasedWriteAheadLog](driverWALConf)

    // Verify setting receiver WAL class
    val receiverWALConf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.class",
      classOf[MockWriteAheadLog0].getName())
    assertDriverLogClass[FileBasedWriteAheadLog](receiverWALConf, isBatched = true)
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

  test("batching is enabled by default in WriteAheadLog") {
    val conf = new SparkConf()
    assert(WriteAheadLogUtils.isBatchingEnabled(conf, isDriver = true))
    // batching is not valid for receiver WALs
    assert(!WriteAheadLogUtils.isBatchingEnabled(conf, isDriver = false))
  }

  test("closeFileAfterWrite is disabled by default in WriteAheadLog") {
    val conf = new SparkConf()
    assert(!WriteAheadLogUtils.shouldCloseFileAfterWrite(conf, isDriver = true))
    assert(!WriteAheadLogUtils.shouldCloseFileAfterWrite(conf, isDriver = false))
  }
}

object WriteAheadLogUtilsSuite {

  class MockWriteAheadLog0() extends WriteAheadLog {
    override def write(record: ByteBuffer, time: Long): WriteAheadLogRecordHandle = { null }
    override def read(handle: WriteAheadLogRecordHandle): ByteBuffer = { null }
    override def readAll(): util.Iterator[ByteBuffer] = { null }
    override def clean(threshTime: Long, waitForCompletion: Boolean): Unit = { }
    override def close(): Unit = { }
  }

  class MockWriteAheadLog1(val conf: SparkConf) extends MockWriteAheadLog0()

  class MockWriteAheadLog2(val conf: SparkConf, x: Int) extends MockWriteAheadLog0()
}

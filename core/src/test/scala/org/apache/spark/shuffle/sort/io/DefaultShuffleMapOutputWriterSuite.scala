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

package org.apache.spark.shuffle.sort.io

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mock
import org.mockito.Mockito.{doAnswer, doNothing, when}
import org.mockito.MockitoAnnotations
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.api.shuffle.SupportsTransferTo
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils

class DefaultShuffleMapOutputWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleWriteMetrics: ShuffleWriteMetrics = _

  private val NUM_PARTITIONS = 4
  private val D_LEN = 10
  private val data: Array[Array[Int]] = (0 until NUM_PARTITIONS).map {
    p => (1 to D_LEN).map(_ + p).toArray }.toArray

  private var tempFile: File = _
  private var mergedOutputFile: File = _
  private var tempDir: File = _
  private var partitionSizesInMergedFile: Array[Long] = _
  private var conf: SparkConf = _
  private var mapOutputWriter: DefaultShuffleMapOutputWriter = _

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def beforeEach(): Unit = {
    MockitoAnnotations.initMocks(this)
    tempDir = Utils.createTempDir(null, "test")
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir)
    tempFile = File.createTempFile("tempfile", "", tempDir)
    partitionSizesInMergedFile = null
    conf = new SparkConf()
      .set("spark.app.id", "example.spark.app")
      .set("spark.shuffle.unsafe.file.output.buffer", "16k")
    when(blockResolver.getDataFile(anyInt, anyInt)).thenReturn(mergedOutputFile)

    doNothing().when(shuffleWriteMetrics).incWriteTime(anyLong)

    doAnswer(new Answer[Void] {
      def answer(invocationOnMock: InvocationOnMock): Void = {
        partitionSizesInMergedFile = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
        val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
        if (tmp != null) {
          mergedOutputFile.delete
          tmp.renameTo(mergedOutputFile)
        }
        null
      }
    }).when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[File]))
    mapOutputWriter = new DefaultShuffleMapOutputWriter(
      0,
      0,
      NUM_PARTITIONS,
      BlockManagerId("0", "localhost", 9099),
      shuffleWriteMetrics,
      blockResolver,
      conf)
  }

  private def readRecordsFromFile(fromByte: Boolean): Array[Array[Int]] = {
    var startOffset = 0L
    val result = new Array[Array[Int]](NUM_PARTITIONS)
    (0 until NUM_PARTITIONS).foreach { p =>
      val partitionSize = partitionSizesInMergedFile(p).toInt
      lazy val inner = new Array[Int](partitionSize)
      lazy val innerBytebuffer = ByteBuffer.allocate(partitionSize)
      if (partitionSize > 0) {
        val in = new FileInputStream(mergedOutputFile)
        in.getChannel.position(startOffset)
        val lin = new LimitedInputStream(in, partitionSize)
        var nonEmpty = true
        var count = 0
        while (nonEmpty) {
          try {
            val readBit = lin.read()
            if (fromByte) {
              innerBytebuffer.put(readBit.toByte)
            } else {
              inner(count) = readBit
            }
            count += 1
          } catch {
            case _: Exception =>
              nonEmpty = false
          }
        }
        in.close()
      }
      if (fromByte) {
        result(p) = innerBytebuffer.array().sliding(4, 4).map { b =>
          new BigInteger(b).intValue()
        }.toArray
      } else {
        result(p) = inner
      }
      startOffset += partitionSize
    }
    result
  }

  test("writing to an outputstream") {
    (0 until NUM_PARTITIONS).foreach{ p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val stream = writer.openStream()
      data(p).foreach { i => stream.write(i)}
      stream.close()
      intercept[IllegalStateException] {
        stream.write(p)
      }
      assert(writer.getNumBytesWritten() == D_LEN)
    }
    mapOutputWriter.commitAllPartitions()
    val partitionLengths = (0 until NUM_PARTITIONS).map { _ => D_LEN.toDouble}.toArray
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile(false))
  }

  test("writing to a channel") {
    (0 until NUM_PARTITIONS).foreach{ p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val channel = writer.asInstanceOf[SupportsTransferTo].openTransferrableChannel()
      val byteBuffer = ByteBuffer.allocate(D_LEN * 4)
      val intBuffer = byteBuffer.asIntBuffer()
      intBuffer.put(data(p))
      val numBytes = byteBuffer.remaining()
      val outputTempFile = File.createTempFile("channelTemp", "", tempDir)
      val outputTempFileStream = new FileOutputStream(outputTempFile)
      Utils.copyStream(
        new ByteBufferInputStream(byteBuffer),
        outputTempFileStream,
        closeStreams = true)
      val tempFileInput = new FileInputStream(outputTempFile)
      channel.transferFrom(tempFileInput.getChannel, 0L, numBytes)
      // Bytes require * 4
      channel.close()
      tempFileInput.close()
      assert(writer.getNumBytesWritten == D_LEN * 4)
    }
    mapOutputWriter.commitAllPartitions()
    val partitionLengths = (0 until NUM_PARTITIONS).map { _ => (D_LEN * 4).toDouble}.toArray
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile(true))
  }

  test("copyStreams with an outputstream") {
    (0 until NUM_PARTITIONS).foreach{ p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val stream = writer.openStream()
      val byteBuffer = ByteBuffer.allocate(D_LEN * 4)
      val intBuffer = byteBuffer.asIntBuffer()
      intBuffer.put(data(p))
      val in = new ByteArrayInputStream(byteBuffer.array())
      Utils.copyStream(in, stream, false, false)
      in.close()
      stream.close()
      assert(writer.getNumBytesWritten == D_LEN * 4)
    }
    mapOutputWriter.commitAllPartitions()
    val partitionLengths = (0 until NUM_PARTITIONS).map { _ => (D_LEN * 4).toDouble}.toArray
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile(true))
  }

  test("copyStreamsWithNIO with a channel") {
    (0 until NUM_PARTITIONS).foreach{ p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val channel = writer.asInstanceOf[SupportsTransferTo].openTransferrableChannel()
      val byteBuffer = ByteBuffer.allocate(D_LEN * 4)
      val intBuffer = byteBuffer.asIntBuffer()
      intBuffer.put(data(p))
      val out = new FileOutputStream(tempFile)
      out.write(byteBuffer.array())
      out.close()
      val in = new FileInputStream(tempFile)
      channel.transferFrom(in.getChannel, 0L, byteBuffer.remaining())
      channel.close()
      assert(writer.getNumBytesWritten == D_LEN * 4)
    }
    mapOutputWriter.commitAllPartitions()
    val partitionLengths = (0 until NUM_PARTITIONS).map { _ => (D_LEN * 4).toDouble}.toArray
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile(true))
  }
}

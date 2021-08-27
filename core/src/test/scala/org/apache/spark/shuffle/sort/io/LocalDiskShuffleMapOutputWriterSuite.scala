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

import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.util.Arrays

import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mock
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.util.Utils

class LocalDiskShuffleMapOutputWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS)
  private var blockResolver: IndexShuffleBlockResolver = _

  private val NUM_PARTITIONS = 4
  private val data: Array[Array[Byte]] = (0 until NUM_PARTITIONS).map { p =>
    if (p == 3) {
      Array.emptyByteArray
    } else {
      (0 to p * 10).map(_ + p).map(_.toByte).toArray
    }
  }.toArray

  private val partitionLengths = data.map(_.length)

  private var tempFile: File = _
  private var mergedOutputFile: File = _
  private var tempDir: File = _
  private var partitionSizesInMergedFile: Array[Long] = _
  private var conf: SparkConf = _
  private var mapOutputWriter: LocalDiskShuffleMapOutputWriter = _

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def beforeEach(): Unit = {
    MockitoAnnotations.openMocks(this).close()
    tempDir = Utils.createTempDir()
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir)
    tempFile = File.createTempFile("tempfile", "", tempDir)
    partitionSizesInMergedFile = null
    conf = new SparkConf()
      .set("spark.app.id", "example.spark.app")
      .set("spark.shuffle.unsafe.file.output.buffer", "16k")
    when(blockResolver.getDataFile(anyInt, anyLong)).thenReturn(mergedOutputFile)
    when(blockResolver.writeMetadataFileAndCommit(
      anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[Array[Long]]), any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        partitionSizesInMergedFile = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
        val tmp: File = invocationOnMock.getArguments()(4).asInstanceOf[File]
        if (tmp != null) {
          mergedOutputFile.delete()
          tmp.renameTo(mergedOutputFile)
        }
        null
      }
    mapOutputWriter = new LocalDiskShuffleMapOutputWriter(
      0,
      0,
      NUM_PARTITIONS,
      blockResolver,
      conf)
  }

  test("writing to an outputstream") {
    (0 until NUM_PARTITIONS).foreach { p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val stream = writer.openStream()
      data(p).foreach { i => stream.write(i) }
      stream.close()
      intercept[IllegalStateException] {
        stream.write(p)
      }
    }
    verifyWrittenRecords()
  }

  test("writing to a channel") {
    (0 until NUM_PARTITIONS).foreach { p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val outputTempFile = File.createTempFile("channelTemp", "", tempDir)
      Files.write(outputTempFile.toPath, data(p))
      val tempFileInput = new FileInputStream(outputTempFile)
      val channel = writer.openChannelWrapper()
      Utils.tryWithResource(new FileInputStream(outputTempFile)) { tempFileInput =>
        Utils.tryWithResource(writer.openChannelWrapper().get) { channelWrapper =>
          assert(channelWrapper.channel().isInstanceOf[FileChannel],
            "Underlying channel should be a file channel")
          Utils.copyFileStreamNIO(
            tempFileInput.getChannel, channelWrapper.channel(), 0L, data(p).length)
        }
      }
    }
    verifyWrittenRecords()
  }

  private def readRecordsFromFile() = {
    val mergedOutputBytes = Files.readAllBytes(mergedOutputFile.toPath)
    val result = (0 until NUM_PARTITIONS).map { part =>
      val startOffset = data.slice(0, part).map(_.length).sum
      val partitionSize = data(part).length
      Arrays.copyOfRange(mergedOutputBytes, startOffset, startOffset + partitionSize)
    }.toArray
    result
  }

  private def verifyWrittenRecords(): Unit = {
    val committedLengths =
      mapOutputWriter.commitAllPartitions(Array.empty[Long]).getPartitionLengths
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(committedLengths === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile())
  }
}

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

package org.apache.spark.shuffle.hash

import java.io._
import java.nio.ByteBuffer

import scala.language.reflectiveCalls

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.executor.{ShuffleReadMetrics, TaskMetrics, ShuffleWriteMetrics}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.serializer._
import org.apache.spark.shuffle.{BaseShuffleHandle, FileShuffleBlockResolver}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId, FileSegment}

class HashShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {
  private val testConf = new SparkConf(false)

  private def checkSegments(expected: FileSegment, buffer: ManagedBuffer) {
    assert(buffer.isInstanceOf[FileSegmentManagedBuffer])
    val segment = buffer.asInstanceOf[FileSegmentManagedBuffer]
    assert(expected.file.getCanonicalPath === segment.getFile.getCanonicalPath)
    assert(expected.offset === segment.getOffset)
    assert(expected.length === segment.getLength)
  }

  test("consolidated shuffle can write to shuffle group without messing existing offsets/lengths") {

    val conf = new SparkConf(false)
    // reset after EACH object write. This is to ensure that there are bytes appended after
    // an object is written. So if the codepaths assume writeObject is end of data, this should
    // flush those bugs out. This was common bug in ExternalAppendOnlyMap, etc.
    conf.set("spark.serializer.objectStreamReset", "1")
    conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.hash.HashShuffleManager")

    sc = new SparkContext("local", "test", conf)

    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[FileShuffleBlockResolver]

    val shuffle1 = shuffleBlockResolver.forMapTask(1, 1, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle1.writers) {
      writer.write("test1", "value")
      writer.write("test2", "value")
    }
    for (writer <- shuffle1.writers) {
      writer.commitAndClose()
    }

    val shuffle1Segment = shuffle1.writers(0).fileSegment()
    shuffle1.releaseWriters(success = true)

    val shuffle2 = shuffleBlockResolver.forMapTask(1, 2, 1, new JavaSerializer(conf),
      new ShuffleWriteMetrics)

    for (writer <- shuffle2.writers) {
      writer.write("test3", "value")
      writer.write("test4", "vlue")
    }
    for (writer <- shuffle2.writers) {
      writer.commitAndClose()
    }
    val shuffle2Segment = shuffle2.writers(0).fileSegment()
    shuffle2.releaseWriters(success = true)

    // Now comes the test :
    // Write to shuffle 3; and close it, but before registering it, check if the file lengths for
    // previous task (forof shuffle1) is the same as 'segments'. Earlier, we were inferring length
    // of block based on remaining data in file : which could mess things up when there is
    // concurrent read and writes happening to the same shuffle group.

    val shuffle3 = shuffleBlockResolver.forMapTask(1, 3, 1, new JavaSerializer(testConf),
      new ShuffleWriteMetrics)
    for (writer <- shuffle3.writers) {
      writer.write("test3", "value")
      writer.write("test4", "value")
    }
    for (writer <- shuffle3.writers) {
      writer.commitAndClose()
    }
    // check before we register.
    checkSegments(shuffle2Segment, shuffleBlockResolver.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffle3.releaseWriters(success = true)
    checkSegments(shuffle2Segment, shuffleBlockResolver.getBlockData(ShuffleBlockId(1, 2, 0)))
    shuffleBlockResolver.removeShuffle(1)
  }

  def writeToFile(file: File, numBytes: Int) {
    val writer = new FileWriter(file, true)
    for (i <- 0 until numBytes) writer.write(i)
    writer.close()
  }

  test("HashShuffleReader.read() releases resources and tracks metrics") {
    val shuffleId = 1
    val numMaps = 2
    val numKeyValuePairs = 10

    val mockContext = mock(classOf[TaskContext])

    val mockTaskMetrics = mock(classOf[TaskMetrics])
    val mockReadMetrics = mock(classOf[ShuffleReadMetrics])
    when(mockTaskMetrics.createShuffleReadMetricsForDependency()).thenReturn(mockReadMetrics)
    when(mockContext.taskMetrics()).thenReturn(mockTaskMetrics)

    val mockShuffleFetcher = mock(classOf[BlockStoreShuffleFetcher])

    val mockDep = mock(classOf[ShuffleDependency[_, _, _]])
    when(mockDep.keyOrdering).thenReturn(None)
    when(mockDep.aggregator).thenReturn(None)
    when(mockDep.serializer).thenReturn(Some(new Serializer {
      override def newInstance(): SerializerInstance = new SerializerInstance {

        override def deserializeStream(s: InputStream): DeserializationStream =
          new DeserializationStream {
            override def readObject[T: ClassManifest](): T = null.asInstanceOf[T]

            override def close(): Unit = s.close()

            private val values = {
              for (i <- 0 to numKeyValuePairs * 2) yield i
            }.iterator

            private def getValueOrEOF(): Int = {
              if (values.hasNext) {
                values.next()
              } else {
                throw new EOFException("End of the file: mock deserializeStream")
              }
            }

            // NOTE: the readKey and readValue methods are called by asKeyValueIterator()
            // which is wrapped in a NextIterator
            override def readKey[T: ClassManifest](): T = getValueOrEOF().asInstanceOf[T]

            override def readValue[T: ClassManifest](): T = getValueOrEOF().asInstanceOf[T]
          }

        override def deserialize[T: ClassManifest](bytes: ByteBuffer, loader: ClassLoader): T =
          null.asInstanceOf[T]

        override def serialize[T: ClassManifest](t: T): ByteBuffer = ByteBuffer.allocate(0)

        override def serializeStream(s: OutputStream): SerializationStream =
          null.asInstanceOf[SerializationStream]

        override def deserialize[T: ClassManifest](bytes: ByteBuffer): T = null.asInstanceOf[T]
      }
    }))

    val mockBlockManager = {
      // Create a block manager that isn't configured for compression, just returns input stream
      val blockManager = mock(classOf[BlockManager])
      when(blockManager.wrapForCompression(any[BlockId](), any[InputStream]()))
        .thenAnswer(new Answer[InputStream] {
        override def answer(invocation: InvocationOnMock): InputStream = {
          val blockId = invocation.getArguments()(0).asInstanceOf[BlockId]
          val inputStream = invocation.getArguments()(1).asInstanceOf[InputStream]
          inputStream
        }
      })
      blockManager
    }

    val mockInputStream = mock(classOf[InputStream])
    when(mockShuffleFetcher.fetchBlockStreams(any[Int](), any[Int](), any[TaskContext]()))
      .thenReturn(Iterator.single((mock(classOf[BlockId]), mockInputStream)))

    val shuffleHandle = new BaseShuffleHandle(shuffleId, numMaps, mockDep)

    val reader = new HashShuffleReader(shuffleHandle, 0, 1,
      mockContext, mockBlockManager, mockShuffleFetcher)

    val values = reader.read()
    // Verify that we're reading the correct values
    var numValuesRead = 0
    for (((key: Int, value: Int), i) <- values.zipWithIndex) {
      assert(key == i * 2)
      assert(value == i * 2 + 1)
      numValuesRead += 1
    }
    // Verify that we read the correct number of values
    assert(numKeyValuePairs == numValuesRead)
    // Verify that our input stream was closed
    verify(mockInputStream, times(1)).close()
    // Verify that we collected metrics for each key/value pair
    verify(mockReadMetrics, times(numKeyValuePairs)).incRecordsRead(1)
  }
}

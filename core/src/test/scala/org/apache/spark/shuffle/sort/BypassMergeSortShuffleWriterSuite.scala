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

package org.apache.spark.shuffle.sort

import java.io.File
import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.executor.{TaskMetrics, ShuffleWriteMetrics}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class BypassMergeSortShuffleWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
  private val temporaryFilesCreated: mutable.Buffer[File] = new ArrayBuffer[File]()
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: BypassMergeSortShuffleHandle[Int, Int] = _

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = new TaskMetrics
    MockitoAnnotations.initMocks(this)
    shuffleHandle = new BypassMergeSortShuffleHandle[Int, Int](
      shuffleId = 0,
      numMaps = 2,
      dependency = dependency
    )
    when(dependency.partitioner).thenReturn(new HashPartitioner(7))
    when(dependency.serializer).thenReturn(Some(new JavaSerializer(conf)))
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    doAnswer(new Answer[Void] {
      def answer(invocationOnMock: InvocationOnMock): Void = {
        val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
        if (tmp != null) {
          outputFile.delete
          tmp.renameTo(outputFile)
        }
        null
      }
    }).when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[File]))
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      anyInt(),
      any[ShuffleWriteMetrics]
    )).thenAnswer(new Answer[DiskBlockObjectWriter] {
      override def answer(invocation: InvocationOnMock): DiskBlockObjectWriter = {
        val args = invocation.getArguments
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File],
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int],
          compressStream = identity,
          syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          blockId = args(0).asInstanceOf[BlockId]
        )
      }
    })
    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      new Answer[(TempShuffleBlockId, File)] {
        override def answer(invocation: InvocationOnMock): (TempShuffleBlockId, File) = {
          val blockId = new TempShuffleBlockId(UUID.randomUUID)
          val file = File.createTempFile(blockId.toString, null, tempDir)
          blockIdToFileMap.put(blockId, file)
          temporaryFilesCreated.append(file)
          (blockId, file)
        }
      })
    when(diskBlockManager.getFile(any[BlockId])).thenAnswer(
      new Answer[File] {
        override def answer(invocation: InvocationOnMock): File = {
          blockIdToFileMap.get(invocation.getArguments.head.asInstanceOf[BlockId]).get
        }
    })
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
    blockIdToFileMap.clear()
    temporaryFilesCreated.clear()
  }

  test("write empty iterator") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf
    )
    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    assert(temporaryFilesCreated.isEmpty)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics.get
    assert(shuffleWriteMetrics.shuffleBytesWritten === 0)
    assert(shuffleWriteMetrics.shuffleRecordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("write with some empty partitions") {
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf
    )
    writer.write(records)
    writer.stop( /* success = */ true)
    assert(temporaryFilesCreated.nonEmpty)
    assert(writer.getPartitionLengths.sum === outputFile.length())
    assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temporary files were deleted
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics.get
    assert(shuffleWriteMetrics.shuffleBytesWritten === outputFile.length())
    assert(shuffleWriteMetrics.shuffleRecordsWritten === records.length)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  test("cleanup of intermediate files after errors") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      shuffleHandle,
      0, // MapId
      taskContext,
      conf
    )
    intercept[SparkException] {
      writer.write((0 until 100000).iterator.map(i => {
        if (i == 99990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(temporaryFilesCreated.nonEmpty)
    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0)
  }

}

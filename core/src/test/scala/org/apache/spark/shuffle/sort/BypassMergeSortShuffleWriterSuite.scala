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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.config
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleChecksumTestHelper}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class BypassMergeSortShuffleWriterSuite
  extends SparkFunSuite
    with BeforeAndAfterEach
    with ShuffleChecksumTestHelper {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency: ShuffleDependency[Int, Int, Int] = _

  private var taskMetrics: TaskMetrics = _
  private var tempDir: File = _
  private var outputFile: File = _
  private var shuffleExecutorComponents: ShuffleExecutorComponents = _
  private val conf: SparkConf = new SparkConf(loadDefaults = false)
    .set("spark.app.id", "sampleApp")
  private val temporaryFilesCreated: mutable.Buffer[File] = new ArrayBuffer[File]()
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: BypassMergeSortShuffleHandle[Int, Int] = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.openMocks(this).close()
    tempDir = Utils.createTempDir()
    outputFile = File.createTempFile("shuffle", null, tempDir)
    taskMetrics = new TaskMetrics
    shuffleHandle = new BypassMergeSortShuffleHandle[Int, Int](
      shuffleId = 0,
      dependency = dependency
    )
    val memoryManager = new TestMemoryManager(conf)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    when(dependency.partitioner).thenReturn(new HashPartitioner(7))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)

    when(blockResolver.writeMetadataFileAndCommit(
      anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[Array[Long]]), any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val tmp = invocationOnMock.getArguments()(4).asInstanceOf[File]
        if (tmp != null) {
          outputFile.delete
          tmp.renameTo(outputFile)
        }
        null
      }

    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      anyInt(),
      any[ShuffleWriteMetrics]))
      .thenAnswer { invocation =>
        val args = invocation.getArguments
        val manager = new SerializerManager(new JavaSerializer(conf), conf)
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File],
          manager,
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int],
          syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          blockId = args(0).asInstanceOf[BlockId])
      }

    when(blockResolver.createTempFile(any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val file = invocationOnMock.getArguments()(0).asInstanceOf[File]
        Utils.tempFileWith(file)
      }

    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer { _ =>
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        blockIdToFileMap.put(blockId, file)
        temporaryFilesCreated += file
        (blockId, file)
      }

    when(diskBlockManager.getFile(any[BlockId])).thenAnswer { invocation =>
      blockIdToFileMap(invocation.getArguments.head.asInstanceOf[BlockId])
    }

    shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(
      conf, blockManager, blockResolver)
  }

  override def afterEach(): Unit = {
    TaskContext.unset()
    try {
      Utils.deleteRecursively(tempDir)
      blockIdToFileMap.clear()
      temporaryFilesCreated.clear()
    } finally {
      super.afterEach()
    }
  }

  test("write empty iterator") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      shuffleExecutorComponents)

    writer.write(Iterator.empty)
    writer.stop( /* success = */ true)
    assert(writer.getPartitionLengths.sum === 0)
    assert(outputFile.exists())
    assert(outputFile.length() === 0)
    assert(temporaryFilesCreated.isEmpty)
    val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
    assert(shuffleWriteMetrics.bytesWritten === 0)
    assert(shuffleWriteMetrics.recordsWritten === 0)
    assert(taskMetrics.diskBytesSpilled === 0)
    assert(taskMetrics.memoryBytesSpilled === 0)
  }

  Seq(true, false).foreach { transferTo =>
    test(s"write with some empty partitions - transferTo $transferTo") {
      val transferConf = conf.clone.set("spark.file.transferTo", transferTo.toString)
      def records: Iterator[(Int, Int)] =
        Iterator((1, 1), (5, 5)) ++ (0 until 100000).iterator.map(x => (2, 2))
      val writer = new BypassMergeSortShuffleWriter[Int, Int](
        blockManager,
        shuffleHandle,
        0L, // MapId
        transferConf,
        taskContext.taskMetrics().shuffleWriteMetrics,
        shuffleExecutorComponents)
      writer.write(records)
      writer.stop( /* success = */ true)
      assert(temporaryFilesCreated.nonEmpty)
      assert(writer.getPartitionLengths.sum === outputFile.length())
      assert(writer.getPartitionLengths.count(_ == 0L) === 4) // should be 4 zero length files
      assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temp files were deleted
      val shuffleWriteMetrics = taskContext.taskMetrics().shuffleWriteMetrics
      assert(shuffleWriteMetrics.bytesWritten === outputFile.length())
      assert(shuffleWriteMetrics.recordsWritten === records.length)
      assert(taskMetrics.diskBytesSpilled === 0)
      assert(taskMetrics.memoryBytesSpilled === 0)
    }
  }

  test("only generate temp shuffle file for non-empty partition") {
    // Using exception to test whether only non-empty partition creates temp shuffle file,
    // because temp shuffle file will only be cleaned after calling stop(false) in the failure
    // case, so we could use it to validate the temp shuffle files.
    def records: Iterator[(Int, Int)] =
      Iterator((1, 1), (5, 5)) ++
        (0 until 100000).iterator.map { i =>
          if (i == 99990) {
            throw new SparkException("intentional failure")
          } else {
            (2, 2)
          }
        }

    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      shuffleExecutorComponents)

    intercept[SparkException] {
      writer.write(records)
    }

    assert(temporaryFilesCreated.nonEmpty)
    // Only 3 temp shuffle files will be created
    assert(temporaryFilesCreated.count(_.exists()) === 3)

    writer.stop( /* success = */ false)
    assert(temporaryFilesCreated.count(_.exists()) === 0) // check that temporary files were deleted
  }

  test("cleanup of intermediate files after errors") {
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      shuffleHandle,
      0L, // MapId
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      shuffleExecutorComponents)
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

  test("write checksum file") {
    val blockResolver = new IndexShuffleBlockResolver(conf, blockManager)
    val shuffleId = shuffleHandle.shuffleId
    val mapId = 0
    val checksumBlockId = ShuffleChecksumBlockId(shuffleId, mapId, 0)
    val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, 0)
    val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, 0)
    val checksumAlgorithm = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)
    val checksumFileName = ShuffleChecksumHelper.getChecksumFileName(
      checksumBlockId.name, checksumAlgorithm)
    val checksumFile = new File(tempDir, checksumFileName)
    val dataFile = new File(tempDir, dataBlockId.name)
    val indexFile = new File(tempDir, indexBlockId.name)
    reset(diskBlockManager)
    when(diskBlockManager.getFile(checksumFileName)).thenAnswer(_ => checksumFile)
    when(diskBlockManager.getFile(dataBlockId)).thenAnswer(_ => dataFile)
    when(diskBlockManager.getFile(indexBlockId)).thenAnswer(_ => indexFile)
    when(diskBlockManager.createTempShuffleBlock())
      .thenAnswer { _ =>
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        temporaryFilesCreated += file
        (blockId, file)
      }
    when(diskBlockManager.createTempFileWith(any(classOf[File])))
      .thenAnswer { invocationOnMock =>
        val file = invocationOnMock.getArguments()(0).asInstanceOf[File]
        Utils.tempFileWith(file)
      }

    val numPartition = shuffleHandle.dependency.partitioner.numPartitions
    val writer = new BypassMergeSortShuffleWriter[Int, Int](
      blockManager,
      shuffleHandle,
      mapId,
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics,
      new LocalDiskShuffleExecutorComponents(conf, blockManager, blockResolver))

    writer.write(Iterator((0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))
    writer.stop( /* success = */ true)
    assert(checksumFile.exists())
    assert(checksumFile.length() === 8 * numPartition)
    compareChecksums(numPartition, checksumAlgorithm, checksumFile, dataFile, indexFile)
  }
}

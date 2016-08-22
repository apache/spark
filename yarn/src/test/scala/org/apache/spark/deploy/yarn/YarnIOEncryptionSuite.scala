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
package org.apache.spark.deploy.yarn

import java.io._
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction
import java.util.{ArrayList => JArrayList, LinkedList => JLinkedList, UUID}


import scala.runtime.AbstractFunction1

import com.google.common.collect.HashMultiset
import com.google.common.io.ByteStreams
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.junit.Assert.assertEquals
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.{SerializedShuffleHandle, UnsafeShuffleWriter}
import org.apache.spark.storage._
import org.apache.spark.util.Utils

private[spark] class YarnIOEncryptionSuite extends SparkFunSuite with Matchers with
  BeforeAndAfterAll with BeforeAndAfterEach {
  @Mock(answer = RETURNS_SMART_NULLS) private[this] var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private[this] var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private[this] var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private[this] var serializerManager: SerializerManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private[this] var taskContext: TaskContext = _
  @Mock(
    answer = RETURNS_SMART_NULLS) private[this] var shuffleDep: ShuffleDependency[Int, Int, Int] = _

  private[this] val NUM_MAPS = 1
  private[this] val NUM_PARTITITONS = 4
  private[this] val REDUCE_ID = 1
  private[this] val SHUFFLE_ID = 0
  private[this] val conf = new SparkConf()
  private[this] val memoryManager = new TestMemoryManager(conf)
  private[this] val hashPartitioner = new HashPartitioner(NUM_PARTITITONS)
  private[this] val serializer = new KryoSerializer(conf)
  private[this] val spillFilesCreated = new JLinkedList[File]()
  private[this] val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
  private[this] val taskMetrics = new TaskMetrics()

  private[this] var tempDir: File = _
  private[this] var mergedOutputFile: File = _
  private[this] var partitionSizesInMergedFile: Array[Long] = _
  private[this] val ugi = UserGroupInformation.createUserForTesting("testuser", Array("testgroup"))

  // Create a mocked shuffle handle to pass into HashShuffleReader.
  private[this] val shuffleHandle = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.aggregator).thenReturn(None)
    when(dependency.keyOrdering).thenReturn(None)
    new BaseShuffleHandle(SHUFFLE_ID, NUM_MAPS, dependency)
  }


  // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
  // shuffle data to read.
  private[this] val mapOutputTracker = mock(classOf[MapOutputTracker])
  private[this] val sparkEnv = mock(classOf[SparkEnv])

  override def beforeAll(): Unit = {
    when(sparkEnv.conf).thenReturn(conf)
    SparkEnv.set(sparkEnv)

    System.setProperty("SPARK_YARN_MODE", "true")
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        conf.set(SPARK_IO_ENCRYPTION_ENABLED, true)
        val creds = new Credentials()
        SerializerManager.initShuffleEncryptionKey(conf, creds)
        SparkHadoopUtil.get.addCurrentUserCredentials(creds)
      }
    })
  }

  override def afterAll(): Unit = {
    SparkEnv.set(null)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    MockitoAnnotations.initMocks(this)
    tempDir = Utils.createTempDir()
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    conf.set("spark.shuffle.compress", false.toString)
    conf.set("spark.shuffle.spill.compress", false.toString)
    Utils.deleteRecursively(tempDir)
    val leakedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
    assert (leakedMemory === 0)
  }

  test("Yarn IO encryption read and write") {
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        conf.set("spark.shuffle.compress", false.toString)
        conf.set("spark.shuffle.spill.compress", false.toString)
        testYarnIOEncryptionWriteRead()
      }
    })
  }

  test("Yarn IO encryption read and write with shuffle compression enabled") {
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        conf.set("spark.shuffle.compress", true.toString)
        conf.set("spark.shuffle.spill.compress", true.toString)
        testYarnIOEncryptionWriteRead()
      }
    })
  }

  private[this] def testYarnIOEncryptionWriteRead(): Unit = {
    val dataToWrite = new JArrayList[Product2[Int, Int]]()
    for (i <- 0 to NUM_PARTITITONS) {
      dataToWrite.add((i, i))
    }
    val shuffleWriter = createWriter()
    shuffleWriter.write(dataToWrite.iterator())
    shuffleWriter.stop(true)

    val shuffleReader = createReader()
    val iter = shuffleReader.read()
    val recordsList = new JArrayList[(Int, Int)]()
    while (iter.hasNext) {
      recordsList.add(iter.next().asInstanceOf[(Int, Int)])
    }

    assertEquals(HashMultiset.create(dataToWrite), HashMultiset.create(recordsList))
  }

  private[this] def createWriter(): UnsafeShuffleWriter[Int, Int] = {
    initialMocksForWriter()
    new UnsafeShuffleWriter[Int, Int](
      blockManager,
      blockResolver,
      taskMemoryManager,
      new SerializedShuffleHandle[Int, Int](SHUFFLE_ID, NUM_MAPS, shuffleDep),
      0, // map id
      taskContext,
      conf
    )
  }

  private[this] def createReader(): BlockStoreShuffleReader[Int, Int] = {
    initialMocksForReader()

    new BlockStoreShuffleReader(
      shuffleHandle,
      REDUCE_ID,
      REDUCE_ID + 1,
      TaskContext.empty(),
      serializerManager,
      blockManager,
      mapOutputTracker)
  }

  private[this] def initialMocksForWriter(): Unit = {
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(blockManager.conf).thenReturn(conf)
    when(blockManager.getDiskWriter(any(classOf[BlockId]), any(classOf[File]),
      any(classOf[SerializerInstance]), anyInt, any(classOf[ShuffleWriteMetrics]))).thenAnswer(
          new Answer[DiskBlockObjectWriter]() {
            override def answer(invocationOnMock: InvocationOnMock): DiskBlockObjectWriter = {
              val args = invocationOnMock.getArguments
              new DiskBlockObjectWriter(args(1).asInstanceOf[File],
                args(2).asInstanceOf[SerializerInstance],
                args(3).asInstanceOf[Integer], new WrapStream(), false,
                args(4).asInstanceOf[ShuffleWriteMetrics], args(0).asInstanceOf[BlockId])
            }
          })

    when(blockResolver.getDataFile(anyInt(), anyInt())).thenReturn(mergedOutputFile)
    doAnswer(new Answer[Unit]() {
      override def answer(invocationOnMock: InvocationOnMock): Unit = {
        partitionSizesInMergedFile = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
        val tmp = invocationOnMock.getArguments()(3)
        mergedOutputFile.delete()
        tmp.asInstanceOf[File].renameTo(mergedOutputFile)
      }
    }).when(blockResolver).writeIndexFileAndCommit(anyInt(), anyInt(), any(classOf[Array[Long]]),
         any(classOf[File]))

    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      new Answer[(TempShuffleBlockId, File)]() {
        override def answer(invocationOnMock: InvocationOnMock): (TempShuffleBlockId, File) = {
          val blockId = new TempShuffleBlockId(UUID.randomUUID())
          val file = File.createTempFile("spillFile", ".spill", tempDir)
          spillFilesCreated.add(file)
          (blockId, file)
        }
      })

    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(shuffleDep.serializer).thenReturn(serializer)
    when(shuffleDep.partitioner).thenReturn(hashPartitioner)
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
  }

  private[this] def initialMocksForReader(): Unit = {
    // Setup the mocked BlockManager to return RecordingManagedBuffers.
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)

    // Create a return function to use for the mocked wrapStream method to initial an
    // encrypted and compressed input stream if encryption and compression enabled
    val wrapFunction = new Answer[InputStream] {
      override def answer(invocation: InvocationOnMock): InputStream = {
        val encryptedStream = if (conf.get(SPARK_IO_ENCRYPTION_ENABLED)) {
          CryptoStreamUtils.createCryptoInputStream(
            invocation.getArguments()(1).asInstanceOf[InputStream], conf)
        } else {
          invocation.getArguments()(1).asInstanceOf[InputStream]
        }
        if (conf.getBoolean("spark.shuffle.compress", false)) {
          CompressionCodec.createCodec(conf).compressedInputStream(encryptedStream)
        } else {
          encryptedStream
        }
      }
    }
    var startOffset = 0L
    for (mapId <- 0 until NUM_PARTITITONS) {
      val partitionSize: Long = partitionSizesInMergedFile(mapId)
      if (partitionSize > 0) {
        val bytes = new Array[Byte](partitionSize.toInt)
        var in: InputStream = new FileInputStream(mergedOutputFile)
        ByteStreams.skipFully(in, startOffset)
        in = new LimitedInputStream(in, partitionSize)
        try {
          in.read(bytes)
        } finally {
          in.close()
        }
        // Create a ManagedBuffer with the shuffle data.
        val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(bytes))
        val managedBuffer = new RecordingManagedBuffer(nioBuffer)
        startOffset += partitionSizesInMergedFile(mapId)
        // Setup the blockManager mock so the buffer gets returned when the shuffle code tries to
        // fetch shuffle data.
        val shuffleBlockId = ShuffleBlockId(SHUFFLE_ID, mapId, REDUCE_ID)
        when(blockManager.getBlockData(shuffleBlockId)).thenReturn(managedBuffer)
        when(serializerManager.wrapStream(meq(shuffleBlockId),
          isA(classOf[InputStream]))).thenAnswer(wrapFunction)
      }
    }

    // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
    // for the code to read data over the network.
    val shuffleBlockIdsAndSizes = (0 until NUM_PARTITITONS).map { mapId =>
      val shuffleBlockId = ShuffleBlockId(SHUFFLE_ID, mapId, REDUCE_ID)
      (shuffleBlockId, partitionSizesInMergedFile(mapId))
    }
    val mapSizesByExecutorId = Seq((localBlockManagerId, shuffleBlockIdsAndSizes))
    when(mapOutputTracker.getMapSizesByExecutorId(SHUFFLE_ID, REDUCE_ID, REDUCE_ID + 1))
      .thenReturn(mapSizesByExecutorId)
  }

  @throws(classOf[IOException])
  private def readRecordsFromFile: JArrayList[(Any, Any)] = {
    val recordsList: JArrayList[(Any, Any)] = new JArrayList[(Any, Any)]
    var startOffset = 0L
    for (mapId <- 0 until NUM_PARTITITONS) {
      val partitionSize: Long = partitionSizesInMergedFile(mapId)
      if (partitionSize > 0) {
        var in: InputStream = new FileInputStream(mergedOutputFile)
        ByteStreams.skipFully(in, startOffset)
        in = new LimitedInputStream(in, partitionSize)
        val recordsStream: DeserializationStream = serializer.newInstance.deserializeStream(in)
        val records: Iterator[(Any, Any)] = recordsStream.asKeyValueIterator
        while (records.hasNext) {
          val record: (Any, Any) = records.next
          assertEquals(mapId, hashPartitioner.getPartition(record._1))
          recordsList.add(record)
        }
        recordsStream.close
        startOffset += partitionSize
      }
    }
    recordsList
  }

  private[this] final class WrapStream extends AbstractFunction1[OutputStream, OutputStream] {
    override def apply(stream: OutputStream): OutputStream = {
      val encryptedStream = if (conf.get(SPARK_IO_ENCRYPTION_ENABLED)) {
        CryptoStreamUtils.createCryptoOutputStream(stream, conf)
      } else {
        stream
      }
      if (conf.getBoolean("spark.shuffle.compress", false)) {
        CompressionCodec.createCodec(conf).compressedOutputStream(encryptedStream)
      } else {
        encryptedStream
      }
    }
  }
}

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
package org.apache.spark.streaming.rdd

import java.io.File

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.util.{FileBasedWriteAheadLogSegment, FileBasedWriteAheadLogWriter}
import org.apache.spark.util.Utils

class WriteAheadLogBackedBlockRDDSuite
  extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)

  val hadoopConf = new Configuration()

  var sparkContext: SparkContext = null
  var blockManager: BlockManager = null
  var serializerManager: SerializerManager = null
  var dir: File = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    initSparkContext()
    dir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(dir)
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    try {
      stopSparkContext()
    } finally {
      super.afterAll()
    }
  }

  private def initSparkContext(_conf: Option[SparkConf] = None): Unit = {
    if (sparkContext == null) {
      sparkContext = new SparkContext(_conf.getOrElse(conf))
      blockManager = sparkContext.env.blockManager
      serializerManager = sparkContext.env.serializerManager
    }
  }

  private def stopSparkContext(): Unit = {
    // Copied from LocalSparkContext, simpler than to introduced test dependencies to core tests.
    try {
      if (sparkContext != null) {
        sparkContext.stop()
      }
      System.clearProperty("spark.driver.port")
      blockManager = null
      serializerManager = null
    } finally {
      sparkContext = null
    }
  }

  test("Read data available in both block manager and write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 5)
  }

  test("Read data available only in block manager, not in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 0)
  }

  test("Read data available only in write ahead log, not in block manager") {
    testRDD(numPartitions = 5, numPartitionsInBM = 0, numPartitionsInWAL = 5)
  }

  test("Read data with partially available in block manager, and rest in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 3, numPartitionsInWAL = 2)
  }

  test("Test isBlockValid skips block fetching from BlockManager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 0, testIsBlockValid = true)
  }

  test("Test whether RDD is valid after removing blocks from block manager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 5, testBlockRemove = true)
  }

  test("Test storing of blocks recovered from write ahead log back into block manager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 0, numPartitionsInWAL = 5, testStoreInBM = true)
  }

  test("read data in block manager and WAL with encryption on") {
    stopSparkContext()
    try {
      val testConf = conf.clone().set(IO_ENCRYPTION_ENABLED, true)
      initSparkContext(Some(testConf))
      testRDD(numPartitions = 5, numPartitionsInBM = 3, numPartitionsInWAL = 2)
    } finally {
      stopSparkContext()
    }
  }

  /**
   * Test the WriteAheadLogBackedRDD, by writing some partitions of the data to block manager
   * and the rest to a write ahead log, and then reading reading it all back using the RDD.
   * It can also test if the partitions that were read from the log were again stored in
   * block manager.
   *
   * @param numPartitions Number of partitions in RDD
   * @param numPartitionsInBM Number of partitions to write to the BlockManager.
   *                          Partitions 0 to (numPartitionsInBM-1) will be written to BlockManager
   * @param numPartitionsInWAL Number of partitions to write to the Write Ahead Log.
   *                           Partitions (numPartitions - 1 - numPartitionsInWAL) to
   *                           (numPartitions - 1) will be written to WAL
   * @param testIsBlockValid Test whether setting isBlockValid to false skips block fetching
   * @param testBlockRemove Test whether calling rdd.removeBlock() makes the RDD still usable with
   *                        reads falling back to the WAL
   * @param testStoreInBM   Test whether blocks read from log are stored back into block manager
   *
   * Example with numPartitions = 5, numPartitionsInBM = 3, and numPartitionsInWAL = 4
   *
   *   numPartitionsInBM = 3
   *   |------------------|
   *   |                  |
   *    0       1       2       3       4
   *           |                         |
   *           |-------------------------|
   *              numPartitionsInWAL = 4
   */
  private def testRDD(
      numPartitions: Int,
      numPartitionsInBM: Int,
      numPartitionsInWAL: Int,
      testIsBlockValid: Boolean = false,
      testBlockRemove: Boolean = false,
      testStoreInBM: Boolean = false
    ) {
    require(numPartitionsInBM <= numPartitions,
      "Can't put more partitions in BlockManager than that in RDD")
    require(numPartitionsInWAL <= numPartitions,
      "Can't put more partitions in write ahead log than that in RDD")
    val data = Seq.fill(numPartitions, 10)(scala.util.Random.nextString(50))

    // Put the necessary blocks in the block manager
    val blockIds = Array.fill(numPartitions)(StreamBlockId(Random.nextInt(), Random.nextInt()))
    data.zip(blockIds).take(numPartitionsInBM).foreach { case(block, blockId) =>
      blockManager.putIterator(blockId, block.iterator, StorageLevel.MEMORY_ONLY_SER)
    }

    // Generate write ahead log record handles
    val recordHandles = generateFakeRecordHandles(numPartitions - numPartitionsInWAL) ++
      generateWALRecordHandles(data.takeRight(numPartitionsInWAL),
        blockIds.takeRight(numPartitionsInWAL))

    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )
    require(
      blockIds.takeRight(numPartitions - numPartitionsInBM).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right `numPartitionsInWAL` blocks are in WALs, and other are not
    require(
      recordHandles.takeRight(numPartitionsInWAL).forall(s =>
        new File(s.path.stripPrefix("file://")).exists()),
      "Expected blocks not in write ahead log"
    )
    require(
      recordHandles.take(numPartitions - numPartitionsInWAL).forall(s =>
        !new File(s.path.stripPrefix("file://")).exists()),
      "Unexpected blocks in write ahead log"
    )

    // Create the RDD and verify whether the returned data is correct
    val rdd = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
      recordHandles.toArray, storeInBlockManager = false)
    assert(rdd.collect() === data.flatten)

    // Verify that the block fetching is skipped when isBlockValid is set to false.
    // This is done by using an RDD whose data is only in memory but is set to skip block fetching
    // Using that RDD will throw exception, as it skips block fetching even if the blocks are in
    // in BlockManager.
    if (testIsBlockValid) {
      require(numPartitionsInBM === numPartitions, "All partitions must be in BlockManager")
      require(numPartitionsInWAL === 0, "No partitions must be in WAL")
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        recordHandles.toArray, isBlockIdValid = Array.fill(blockIds.length)(false))
      intercept[SparkException] {
        rdd2.collect()
      }
    }

    // Verify that the RDD is not invalid after the blocks are removed and can still read data
    // from write ahead log
    if (testBlockRemove) {
      require(numPartitions === numPartitionsInWAL, "All partitions must be in WAL for this test")
      require(numPartitionsInBM > 0, "Some partitions must be in BlockManager for this test")
      rdd.removeBlocks()
      assert(rdd.collect() === data.flatten)
    }

    if (testStoreInBM) {
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        recordHandles.toArray, storeInBlockManager = true, storageLevel = StorageLevel.MEMORY_ONLY)
      assert(rdd2.collect() === data.flatten)
      assert(
        blockIds.forall(blockManager.get(_).nonEmpty),
        "All blocks not found in block manager"
      )
    }
  }

  private def generateWALRecordHandles(
      blockData: Seq[Seq[String]],
      blockIds: Seq[BlockId]
    ): Seq[FileBasedWriteAheadLogSegment] = {
    require(blockData.size === blockIds.size)
    val writer = new FileBasedWriteAheadLogWriter(new File(dir, "logFile").toString, hadoopConf)
    val segments = blockData.zip(blockIds).map { case (data, id) =>
      writer.write(serializerManager.dataSerialize(id, data.iterator, allowEncryption = false)
        .toByteBuffer)
    }
    writer.close()
    segments
  }

  private def generateFakeRecordHandles(count: Int): Seq[FileBasedWriteAheadLogSegment] = {
    Array.fill(count)(new FileBasedWriteAheadLogSegment("random", 0L, 0))
  }
}

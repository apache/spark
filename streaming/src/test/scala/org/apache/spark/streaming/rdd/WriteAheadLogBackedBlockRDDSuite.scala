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
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.util.{WriteAheadLogFileSegment, WriteAheadLogWriter}
import org.apache.spark.util.Utils

class WriteAheadLogBackedBlockRDDSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)
  val hadoopConf = new Configuration()

  var sparkContext: SparkContext = null
  var blockManager: BlockManager = null
  var dir: File = null

  override def beforeEach(): Unit = {
    dir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(dir)
  }

  override def beforeAll(): Unit = {
    sparkContext = new SparkContext(conf)
    blockManager = sparkContext.env.blockManager
  }

  override def afterAll(): Unit = {
    // Copied from LocalSparkContext, simpler than to introduced test dependencies to core tests.
    sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }

  test("Read data available in block manager and write ahead log") {
    testRDD(5, 5)
  }

  test("Read data available only in block manager, not in write ahead log") {
    testRDD(5, 0)
  }

  test("Read data available only in write ahead log, not in block manager") {
    testRDD(0, 5)
  }

  test("Read data available only in write ahead log, and test storing in block manager") {
    testRDD(0, 5, testStoreInBM = true)
  }

  test("Read data with partially available in block manager, and rest in write ahead log") {
    testRDD(3, 2)
  }

  /**
   * Test the WriteAheadLogBackedRDD, by writing some partitions of the data to block manager
   * and the rest to a write ahead log, and then reading reading it all back using the RDD.
   * It can also test if the partitions that were read from the log were again stored in
   * block manager.
   * @param numPartitionsInBM Number of partitions to write to the Block Manager
   * @param numPartitionsInWAL Number of partitions to write to the Write Ahead Log
   * @param testStoreInBM Test whether blocks read from log are stored back into block manager
   */
  private def testRDD(numPartitionsInBM: Int, numPartitionsInWAL: Int, testStoreInBM: Boolean = false) {
    val numBlocks = numPartitionsInBM + numPartitionsInWAL
    val data = Seq.fill(numBlocks, 10)(scala.util.Random.nextString(50))

    // Put the necessary blocks in the block manager
    val blockIds = Array.fill(numBlocks)(StreamBlockId(Random.nextInt(), Random.nextInt()))
    data.zip(blockIds).take(numPartitionsInBM).foreach { case(block, blockId) =>
      blockManager.putIterator(blockId, block.iterator, StorageLevel.MEMORY_ONLY_SER)
    }

    // Generate write ahead log segments
    val segments = generateFakeSegments(numPartitionsInBM) ++
      writeLogSegments(data.takeRight(numPartitionsInWAL), blockIds.takeRight(numPartitionsInWAL))

    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )
    require(
      blockIds.takeRight(numPartitionsInWAL).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right `numPartitionsInWAL` blocks are in write ahead logs, and other are not
    require(
      segments.takeRight(numPartitionsInWAL).forall(s =>
        new File(s.path.stripPrefix("file://")).exists()),
      "Expected blocks not in write ahead log"
    )
    require(
      segments.take(numPartitionsInBM).forall(s =>
        !new File(s.path.stripPrefix("file://")).exists()),
      "Unexpected blocks in write ahead log"
    )

    // Create the RDD and verify whether the returned data is correct
    val rdd = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
      segments.toArray, storeInBlockManager = false, StorageLevel.MEMORY_ONLY)
    assert(rdd.collect() === data.flatten)

    if (testStoreInBM) {
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        segments.toArray, storeInBlockManager = true, StorageLevel.MEMORY_ONLY)
      assert(rdd2.collect() === data.flatten)
      assert(
        blockIds.forall(blockManager.get(_).nonEmpty),
        "All blocks not found in block manager"
      )
    }
  }

  private def writeLogSegments(
      blockData: Seq[Seq[String]],
      blockIds: Seq[BlockId]
    ): Seq[WriteAheadLogFileSegment] = {
    require(blockData.size === blockIds.size)
    val writer = new WriteAheadLogWriter(new File(dir, "logFile").toString, hadoopConf)
    val segments = blockData.zip(blockIds).map { case (data, id) =>
      writer.write(blockManager.dataSerialize(id, data.iterator))
    }
    writer.close()
    segments
  }

  private def generateFakeSegments(count: Int): Seq[WriteAheadLogFileSegment] = {
    Array.fill(count)(new WriteAheadLogFileSegment("random", 0l, 0))
  }
}

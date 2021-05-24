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

package org.apache.spark.streaming.kinesis

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException}
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, StreamBlockId}

abstract class KinesisBackedBlockRDDTests(aggregateTestData: Boolean)
  extends KinesisFunSuite with BeforeAndAfterEach with LocalSparkContext {

  private val testData = 1 to 8

  private var testUtils: KinesisTestUtils = null
  private var shardIds: Seq[String] = null
  private var shardIdToData: Map[String, Seq[Int]] = null
  private var shardIdToSeqNumbers: Map[String, Seq[String]] = null
  private var shardIdToDataAndSeqNumbers: Map[String, Seq[(Int, String)]] = null
  private var shardIdToRange: Map[String, SequenceNumberRange] = null
  private var allRanges: Seq[SequenceNumberRange] = null

  private var blockManager: BlockManager = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    runIfTestsEnabled("Prepare KinesisTestUtils") {
      testUtils = new KPLBasedKinesisTestUtils()
      testUtils.createStream()

      shardIdToDataAndSeqNumbers = testUtils.pushData(testData, aggregate = aggregateTestData)
      require(shardIdToDataAndSeqNumbers.size > 1, "Need data to be sent to multiple shards")

      shardIds = shardIdToDataAndSeqNumbers.keySet.toSeq
      shardIdToData = shardIdToDataAndSeqNumbers.mapValues(_.map(_._1)).toMap
      shardIdToSeqNumbers = shardIdToDataAndSeqNumbers.mapValues(_.map(_._2)).toMap
      shardIdToRange = shardIdToSeqNumbers.map { case (shardId, seqNumbers) =>
        val seqNumRange = SequenceNumberRange(
          testUtils.streamName, shardId, seqNumbers.head, seqNumbers.last, seqNumbers.size)
        (shardId, seqNumRange)
      }
      allRanges = shardIdToRange.values.toSeq
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val conf = new SparkConf().setMaster("local[4]").setAppName("KinesisBackedBlockRDDSuite")
    sc = new SparkContext(conf)
    blockManager = sc.env.blockManager
  }

  override def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.deleteStream()
      }
    } finally {
      super.afterAll()
    }
  }

  testIfEnabled("Basic reading from Kinesis") {
    // Verify all data using multiple ranges in a single RDD partition
    val receivedData1 = new KinesisBackedBlockRDD[Array[Byte]](sc, testUtils.regionName,
      testUtils.endpointUrl, fakeBlockIds(1),
      Array(SequenceNumberRanges(allRanges.toArray))
    ).map { bytes => new String(bytes).toInt }.collect()
    assert(receivedData1.toSet === testData.toSet)

    // Verify all data using one range in each of the multiple RDD partitions
    val receivedData2 = new KinesisBackedBlockRDD[Array[Byte]](sc, testUtils.regionName,
      testUtils.endpointUrl, fakeBlockIds(allRanges.size),
      allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
    ).map { bytes => new String(bytes).toInt }.collect()
    assert(receivedData2.toSet === testData.toSet)

    // Verify ordering within each partition
    val receivedData3 = new KinesisBackedBlockRDD[Array[Byte]](sc, testUtils.regionName,
      testUtils.endpointUrl, fakeBlockIds(allRanges.size),
      allRanges.map { range => SequenceNumberRanges(Array(range)) }.toArray
    ).map { bytes => new String(bytes).toInt }.collectPartitions()
    assert(receivedData3.length === allRanges.size)
    for (i <- 0 until allRanges.size) {
      assert(receivedData3(i).toSeq === shardIdToData(allRanges(i).shardId))
    }
  }

  testIfEnabled("Read data available in both block manager and Kinesis") {
    testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 2)
  }

  testIfEnabled("Read data available only in block manager, not in Kinesis") {
    testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 0)
  }

  testIfEnabled("Read data available only in Kinesis, not in block manager") {
    testRDD(numPartitions = 2, numPartitionsInBM = 0, numPartitionsInKinesis = 2)
  }

  testIfEnabled("Read data available partially in block manager, rest in Kinesis") {
    testRDD(numPartitions = 2, numPartitionsInBM = 1, numPartitionsInKinesis = 1)
  }

  testIfEnabled("Test isBlockValid skips block fetching from block manager") {
    testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 0,
      testIsBlockValid = true)
  }

  testIfEnabled("Test whether RDD is valid after removing blocks from block manager") {
    testRDD(numPartitions = 2, numPartitionsInBM = 2, numPartitionsInKinesis = 2,
      testBlockRemove = true)
  }

  /**
   * Test the WriteAheadLogBackedRDD, by writing some partitions of the data to block manager
   * and the rest to a write ahead log, and then reading it all back using the RDD.
   * It can also test if the partitions that were read from the log were again stored in
   * block manager.
   *
   *
   *
   * @param numPartitions Number of partitions in RDD
   * @param numPartitionsInBM Number of partitions to write to the BlockManager.
   *                          Partitions 0 to (numPartitionsInBM-1) will be written to BlockManager
   * @param numPartitionsInKinesis Number of partitions to write to the Kinesis.
   *                           Partitions (numPartitions - 1 - numPartitionsInKinesis) to
   *                           (numPartitions - 1) will be written to Kinesis
   * @param testIsBlockValid Test whether setting isBlockValid to false skips block fetching
   * @param testBlockRemove Test whether calling rdd.removeBlock() makes the RDD still usable with
   *                        reads falling back to the WAL
   * Example with numPartitions = 5, numPartitionsInBM = 3, and numPartitionsInWAL = 4
   *
   *   numPartitionsInBM = 3
   *   |------------------|
   *   |                  |
   *    0       1       2       3       4
   *           |                         |
   *           |-------------------------|
   *              numPartitionsInKinesis = 4
   */
  private def testRDD(
      numPartitions: Int,
      numPartitionsInBM: Int,
      numPartitionsInKinesis: Int,
      testIsBlockValid: Boolean = false,
      testBlockRemove: Boolean = false
    ): Unit = {
    require(shardIds.size > 1, "Need at least 2 shards to test")
    require(numPartitionsInBM <= shardIds.size,
      "Number of partitions in BlockManager cannot be more than the Kinesis test shards available")
    require(numPartitionsInKinesis <= shardIds.size,
      "Number of partitions in Kinesis cannot be more than the Kinesis test shards available")
    require(numPartitionsInBM <= numPartitions,
      "Number of partitions in BlockManager cannot be more than that in RDD")
    require(numPartitionsInKinesis <= numPartitions,
      "Number of partitions in Kinesis cannot be more than that in RDD")

    // Put necessary blocks in the block manager
    val blockIds = fakeBlockIds(numPartitions)
    blockIds.foreach(blockManager.removeBlock(_))
    (0 until numPartitionsInBM).foreach { i =>
      val blockData = shardIdToData(shardIds(i)).iterator.map { _.toString.getBytes() }
      blockManager.putIterator(blockIds(i), blockData, StorageLevel.MEMORY_ONLY)
    }

    // Create the necessary ranges to use in the RDD
    val fakeRanges = Array.fill(numPartitions - numPartitionsInKinesis)(
      SequenceNumberRanges(SequenceNumberRange("fakeStream", "fakeShardId", "xxx", "yyy", 1)))
    val realRanges = Array.tabulate(numPartitionsInKinesis) { i =>
      val range = shardIdToRange(shardIds(i + (numPartitions - numPartitionsInKinesis)))
      SequenceNumberRanges(Array(range))
    }
    val ranges = (fakeRanges ++ realRanges)


    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )

    require(
      blockIds.drop(numPartitionsInBM).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right sequence `numPartitionsInKinesis` are configured, and others are not
    require(
      ranges.takeRight(numPartitionsInKinesis).forall {
        _.ranges.forall { _.streamName == testUtils.streamName }
      }, "Incorrect configuration of RDD, expected ranges not set: "
    )

    require(
      ranges.dropRight(numPartitionsInKinesis).forall {
        _.ranges.forall { _.streamName != testUtils.streamName }
      }, "Incorrect configuration of RDD, unexpected ranges set"
    )

    val rdd = new KinesisBackedBlockRDD[Array[Byte]](
      sc, testUtils.regionName, testUtils.endpointUrl, blockIds, ranges)
    val collectedData = rdd.map { bytes =>
      new String(bytes).toInt
    }.collect()
    assert(collectedData.toSet === testData.toSet)

    // Verify that the block fetching is skipped when isBlockValid is set to false.
    // This is done by using an RDD whose data is only in memory but is set to skip block fetching
    // Using that RDD will throw exception, as it skips block fetching even if the blocks are in
    // in BlockManager.
    if (testIsBlockValid) {
      require(numPartitionsInBM === numPartitions, "All partitions must be in BlockManager")
      require(numPartitionsInKinesis === 0, "No partitions must be in Kinesis")
      val rdd2 = new KinesisBackedBlockRDD[Array[Byte]](
        sc, testUtils.regionName, testUtils.endpointUrl, blockIds.toArray, ranges,
        isBlockIdValid = Array.fill(blockIds.length)(false))
      intercept[SparkException] {
        rdd2.collect()
      }
    }

    // Verify that the RDD is not invalid after the blocks are removed and can still read data
    // from write ahead log
    if (testBlockRemove) {
      require(numPartitions === numPartitionsInKinesis,
        "All partitions must be in WAL for this test")
      require(numPartitionsInBM > 0, "Some partitions must be in BlockManager for this test")
      rdd.removeBlocks()
      assert(rdd.map { bytes => new String(bytes).toInt }.collect().toSet === testData.toSet)
    }
  }

  /** Generate fake block ids */
  private def fakeBlockIds(num: Int): Array[BlockId] = {
    Array.tabulate(num) { i => new StreamBlockId(0, i) }
  }
}

class WithAggregationKinesisBackedBlockRDDSuite
  extends KinesisBackedBlockRDDTests(aggregateTestData = true)

class WithoutAggregationKinesisBackedBlockRDDSuite
  extends KinesisBackedBlockRDDTests(aggregateTestData = false)

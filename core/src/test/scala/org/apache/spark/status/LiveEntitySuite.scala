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

package org.apache.spark.status

import java.util.Arrays

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{AccumulatorMetadata, CollectionAccumulator}

class LiveEntitySuite extends SparkFunSuite {

  test("partition seq") {
    val seq = new RDDPartitionSeq()
    val items = (1 to 10).map { i =>
      val part = newPartition(i)
      seq.addPartition(part)
      part
    }.toList

    checkSize(seq, 10)

    val added = newPartition(11)
    seq.addPartition(added)
    checkSize(seq, 11)
    assert(seq.last.blockName === added.blockName)

    seq.removePartition(items(0))
    assert(seq.head.blockName === items(1).blockName)
    assert(!seq.exists(_.blockName == items(0).blockName))
    checkSize(seq, 10)

    seq.removePartition(added)
    assert(seq.last.blockName === items.last.blockName)
    assert(!seq.exists(_.blockName == added.blockName))
    checkSize(seq, 9)

    seq.removePartition(items(5))
    checkSize(seq, 8)
    assert(!seq.exists(_.blockName == items(5).blockName))
  }

  test("Only show few elements of CollectionAccumulator when converting to v1.AccumulableInfo") {
    val acc = new CollectionAccumulator[Int]()
    val value = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    acc.setValue(value)
    acc.metadata = AccumulatorMetadata(0L, None, false)
    val accuInfo = LiveEntityHelpers
      .newAccumulatorInfos(Seq(acc.toInfo(Some(acc.value), Some(acc.value))))(0)
    assert(accuInfo.update.get == "[1,2,3,4,5,... 5 more items]")
    assert(accuInfo.value == "[1,2,3,4,5,... 5 more items]")
  }

  test("makeNegative correctly negates all metrics with proper argument order") {
    import LiveEntityHelpers._

    val originalMetrics = createMetrics(
      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 2L,
      executorRunTime = 3L,
      executorCpuTime = 4L,
      resultSize = 5L,
      jvmGcTime = 6L,
      resultSerializationTime = 7L,
      memoryBytesSpilled = 8L,
      diskBytesSpilled = 9L,
      peakExecutionMemory = 10L,
      inputBytesRead = 11L,
      inputRecordsRead = 12L,
      outputBytesWritten = 13L,
      outputRecordsWritten = 14L,
      shuffleRemoteBlocksFetched = 15L,
      shuffleLocalBlocksFetched = 16L,
      shuffleFetchWaitTime = 17L,
      shuffleRemoteBytesRead = 18L,
      shuffleRemoteBytesReadToDisk = 19L,
      shuffleLocalBytesRead = 20L,
      shuffleRecordsRead = 21L,
      shuffleCorruptMergedBlockChunks = 22L,
      shuffleMergedFetchFallbackCount = 23L,
      shuffleMergedRemoteBlocksFetched = 24L,
      shuffleMergedLocalBlocksFetched = 25L,
      shuffleMergedRemoteChunksFetched = 26L,
      shuffleMergedLocalChunksFetched = 27L,
      shuffleMergedRemoteBytesRead = 28L,
      shuffleMergedLocalBytesRead = 29L,
      shuffleRemoteReqsDuration = 30L,
      shuffleMergedRemoteReqsDuration = 31L,
      shuffleBytesWritten = 32L,
      shuffleWriteTime = 33L,
      shuffleRecordsWritten = 34L
    )

    val negatedMetrics = makeNegative(originalMetrics)

    def expectedNegated(v: Long): Long = v * -1L - 1L

    // Verify all fields are correctly negated
    assert(negatedMetrics.executorDeserializeTime === expectedNegated(1L))
    assert(negatedMetrics.executorDeserializeCpuTime === expectedNegated(2L))
    assert(negatedMetrics.executorRunTime === expectedNegated(3L))
    assert(negatedMetrics.executorCpuTime === expectedNegated(4L))
    assert(negatedMetrics.resultSize === expectedNegated(5L))
    assert(negatedMetrics.jvmGcTime === expectedNegated(6L))
    assert(negatedMetrics.resultSerializationTime === expectedNegated(7L))
    assert(negatedMetrics.memoryBytesSpilled === expectedNegated(8L))
    assert(negatedMetrics.diskBytesSpilled === expectedNegated(9L))
    assert(negatedMetrics.peakExecutionMemory === expectedNegated(10L))

    // Verify input metrics
    assert(negatedMetrics.inputMetrics.bytesRead === expectedNegated(11L))
    assert(negatedMetrics.inputMetrics.recordsRead === expectedNegated(12L))

    // Verify output metrics (these were in wrong position in current master)
    assert(negatedMetrics.outputMetrics.bytesWritten === expectedNegated(13L),
      "outputMetrics.bytesWritten should be correctly negated")
    assert(negatedMetrics.outputMetrics.recordsWritten === expectedNegated(14L),
      "outputMetrics.recordsWritten should be correctly negated")

    // Verify shuffle read metrics (these were in wrong position in current master)
    assert(negatedMetrics.shuffleReadMetrics.remoteBlocksFetched === expectedNegated(15L),
      "shuffleReadMetrics.remoteBlocksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.localBlocksFetched === expectedNegated(16L),
      "shuffleReadMetrics.localBlocksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.fetchWaitTime === expectedNegated(17L),
      "shuffleReadMetrics.fetchWaitTime should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.remoteBytesRead === expectedNegated(18L),
      "shuffleReadMetrics.remoteBytesRead should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.remoteBytesReadToDisk === expectedNegated(19L),
      "shuffleReadMetrics.remoteBytesReadToDisk should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.localBytesRead === expectedNegated(20L),
      "shuffleReadMetrics.localBytesRead should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.recordsRead === expectedNegated(21L),
      "shuffleReadMetrics.recordsRead should be correctly negated")

    // Verify shuffle push read metrics (these were in wrong position in current master)
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.corruptMergedBlockChunks ===
      expectedNegated(22L),
      "shufflePushReadMetrics.corruptMergedBlockChunks should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.mergedFetchFallbackCount ===
      expectedNegated(23L),
      "shufflePushReadMetrics.mergedFetchFallbackCount should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedBlocksFetched ===
      expectedNegated(24L),
      "shufflePushReadMetrics.remoteMergedBlocksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedBlocksFetched ===
      expectedNegated(25L),
      "shufflePushReadMetrics.localMergedBlocksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedChunksFetched ===
      expectedNegated(26L),
      "shufflePushReadMetrics.remoteMergedChunksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedChunksFetched ===
      expectedNegated(27L),
      "shufflePushReadMetrics.localMergedChunksFetched should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedBytesRead ===
      expectedNegated(28L),
      "shufflePushReadMetrics.remoteMergedBytesRead should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedBytesRead ===
      expectedNegated(29L),
      "shufflePushReadMetrics.localMergedBytesRead should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.remoteReqsDuration === expectedNegated(30L),
      "shuffleReadMetrics.remoteReqsDuration should be correctly negated")
    assert(negatedMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedReqsDuration ===
      expectedNegated(31L),
      "shufflePushReadMetrics.remoteMergedReqsDuration should be correctly negated")

    // Verify shuffle write metrics
    assert(negatedMetrics.shuffleWriteMetrics.bytesWritten === expectedNegated(32L))
    assert(negatedMetrics.shuffleWriteMetrics.writeTime === expectedNegated(33L))
    assert(negatedMetrics.shuffleWriteMetrics.recordsWritten === expectedNegated(34L))

    // Verify zero handling: 0 should become -1
    val zeroMetrics = createMetrics(default = 0L)
    val negatedZeroMetrics = makeNegative(zeroMetrics)
    assert(negatedZeroMetrics.executorDeserializeTime === -1L,
      "Zero value should be converted to -1")
    assert(negatedZeroMetrics.inputMetrics.bytesRead === -1L,
      "Zero input metric should be converted to -1")
    assert(negatedZeroMetrics.outputMetrics.bytesWritten === -1L,
      "Zero output metric should be converted to -1")
  }

  private def checkSize(seq: Seq[_], expected: Int): Unit = {
    assert(seq.length === expected)
    var count = 0
    seq.iterator.foreach { _ => count += 1 }
    assert(count === expected)
  }

  private def newPartition(i: Int): LiveRDDPartition = {
    val part = new LiveRDDPartition(i.toString, StorageLevel.MEMORY_AND_DISK)
    part.update(Seq(i.toString), i, i)
    part
  }

}

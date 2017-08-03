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

package org.apache.spark.executor

import org.scalatest.Assertions

import org.apache.spark._
import org.apache.spark.storage.{BlockStatus, StorageLevel, TestBlockId}
import org.apache.spark.util.AccumulatorV2


class TaskMetricsSuite extends SparkFunSuite {
  import StorageLevel._

  test("mutating values") {
    val tm = new TaskMetrics
    assert(tm.executorDeserializeTime == 0L)
    assert(tm.executorRunTime == 0L)
    assert(tm.resultSize == 0L)
    assert(tm.jvmGCTime == 0L)
    assert(tm.resultSerializationTime == 0L)
    assert(tm.memoryBytesSpilled == 0L)
    assert(tm.diskBytesSpilled == 0L)
    assert(tm.peakExecutionMemory == 0L)
    assert(tm.updatedBlockStatuses.isEmpty)
    // set or increment values
    tm.setExecutorDeserializeTime(100L)
    tm.setExecutorDeserializeTime(1L) // overwrite
    tm.setExecutorRunTime(200L)
    tm.setExecutorRunTime(2L)
    tm.setResultSize(300L)
    tm.setResultSize(3L)
    tm.setJvmGCTime(400L)
    tm.setJvmGCTime(4L)
    tm.setResultSerializationTime(500L)
    tm.setResultSerializationTime(5L)
    tm.incMemoryBytesSpilled(600L)
    tm.incMemoryBytesSpilled(6L) // add
    tm.incDiskBytesSpilled(700L)
    tm.incDiskBytesSpilled(7L)
    tm.incPeakExecutionMemory(800L)
    tm.incPeakExecutionMemory(8L)
    val block1 = (TestBlockId("a"), BlockStatus(MEMORY_ONLY, 1L, 2L))
    val block2 = (TestBlockId("b"), BlockStatus(MEMORY_ONLY, 3L, 4L))
    tm.incUpdatedBlockStatuses(block1)
    tm.incUpdatedBlockStatuses(block2)
    // assert new values exist
    assert(tm.executorDeserializeTime == 1L)
    assert(tm.executorRunTime == 2L)
    assert(tm.resultSize == 3L)
    assert(tm.jvmGCTime == 4L)
    assert(tm.resultSerializationTime == 5L)
    assert(tm.memoryBytesSpilled == 606L)
    assert(tm.diskBytesSpilled == 707L)
    assert(tm.peakExecutionMemory == 808L)
    assert(tm.updatedBlockStatuses == Seq(block1, block2))
  }

  test("mutating shuffle read metrics values") {
    val tm = new TaskMetrics
    val sr = tm.shuffleReadMetrics
    // initial values
    assert(sr.remoteBlocksFetched == 0)
    assert(sr.localBlocksFetched == 0)
    assert(sr.remoteBytesRead == 0L)
    assert(sr.localBytesRead == 0L)
    assert(sr.fetchWaitTime == 0L)
    assert(sr.recordsRead == 0L)
    // set and increment values
    sr.setRemoteBlocksFetched(100)
    sr.setRemoteBlocksFetched(10)
    sr.incRemoteBlocksFetched(1) // 10 + 1
    sr.incRemoteBlocksFetched(1) // 10 + 1 + 1
    sr.setLocalBlocksFetched(200)
    sr.setLocalBlocksFetched(20)
    sr.incLocalBlocksFetched(2)
    sr.incLocalBlocksFetched(2)
    sr.setRemoteBytesRead(300L)
    sr.setRemoteBytesRead(30L)
    sr.incRemoteBytesRead(3L)
    sr.incRemoteBytesRead(3L)
    sr.setRemoteBytesReadToDisk(10L)
    sr.incRemoteBytesReadToDisk(8L)
    sr.setLocalBytesRead(400L)
    sr.setLocalBytesRead(40L)
    sr.incLocalBytesRead(4L)
    sr.incLocalBytesRead(4L)
    sr.setFetchWaitTime(500L)
    sr.setFetchWaitTime(50L)
    sr.incFetchWaitTime(5L)
    sr.incFetchWaitTime(5L)
    sr.setRecordsRead(600L)
    sr.setRecordsRead(60L)
    sr.incRecordsRead(6L)
    sr.incRecordsRead(6L)
    // assert new values exist
    assert(sr.remoteBlocksFetched == 12)
    assert(sr.localBlocksFetched == 24)
    assert(sr.remoteBytesRead == 36L)
    assert(sr.remoteBytesReadToDisk == 18L)
    assert(sr.localBytesRead == 48L)
    assert(sr.fetchWaitTime == 60L)
    assert(sr.recordsRead == 72L)
  }

  test("mutating shuffle write metrics values") {
    val tm = new TaskMetrics
    val sw = tm.shuffleWriteMetrics
    // initial values
    assert(sw.bytesWritten == 0L)
    assert(sw.recordsWritten == 0L)
    assert(sw.writeTime == 0L)
    // increment and decrement values
    sw.incBytesWritten(100L)
    sw.incBytesWritten(10L) // 100 + 10
    sw.decBytesWritten(1L) // 100 + 10 - 1
    sw.decBytesWritten(1L) // 100 + 10 - 1 - 1
    sw.incRecordsWritten(200L)
    sw.incRecordsWritten(20L)
    sw.decRecordsWritten(2L)
    sw.decRecordsWritten(2L)
    sw.incWriteTime(300L)
    sw.incWriteTime(30L)
    // assert new values exist
    assert(sw.bytesWritten == 108L)
    assert(sw.recordsWritten == 216L)
    assert(sw.writeTime == 330L)
  }

  test("mutating input metrics values") {
    val tm = new TaskMetrics
    val in = tm.inputMetrics
    // initial values
    assert(in.bytesRead == 0L)
    assert(in.recordsRead == 0L)
    // set and increment values
    in.setBytesRead(1L)
    in.setBytesRead(2L)
    in.incRecordsRead(1L)
    in.incRecordsRead(2L)
    // assert new values exist
    assert(in.bytesRead == 2L)
    assert(in.recordsRead == 3L)
  }

  test("mutating output metrics values") {
    val tm = new TaskMetrics
    val out = tm.outputMetrics
    // initial values
    assert(out.bytesWritten == 0L)
    assert(out.recordsWritten == 0L)
    // set values
    out.setBytesWritten(1L)
    out.setBytesWritten(2L)
    out.setRecordsWritten(3L)
    out.setRecordsWritten(4L)
    // assert new values exist
    assert(out.bytesWritten == 2L)
    assert(out.recordsWritten == 4L)
  }

  test("merging multiple shuffle read metrics") {
    val tm = new TaskMetrics
    val sr1 = tm.createTempShuffleReadMetrics()
    val sr2 = tm.createTempShuffleReadMetrics()
    val sr3 = tm.createTempShuffleReadMetrics()
    sr1.incRecordsRead(10L)
    sr2.incRecordsRead(10L)
    sr1.incFetchWaitTime(1L)
    sr2.incFetchWaitTime(2L)
    sr3.incFetchWaitTime(3L)
    tm.mergeShuffleReadMetrics()
    assert(tm.shuffleReadMetrics.remoteBlocksFetched === 0L)
    assert(tm.shuffleReadMetrics.recordsRead === 20L)
    assert(tm.shuffleReadMetrics.fetchWaitTime === 6L)

    // SPARK-5701: calling merge without any shuffle deps does nothing
    val tm2 = new TaskMetrics
    tm2.mergeShuffleReadMetrics()
  }

  test("additional accumulables") {
    val tm = TaskMetrics.empty
    val acc1 = AccumulatorSuite.createLongAccum("a")
    val acc2 = AccumulatorSuite.createLongAccum("b")
    val acc3 = AccumulatorSuite.createLongAccum("c")
    val acc4 = AccumulatorSuite.createLongAccum("d", true)
    tm.registerAccumulator(acc1)
    tm.registerAccumulator(acc2)
    tm.registerAccumulator(acc3)
    tm.registerAccumulator(acc4)
    acc1.add(1)
    acc2.add(2)
    val newUpdates = tm.accumulators()
      .map(a => (a.id, a.asInstanceOf[AccumulatorV2[Any, Any]])).toMap
    assert(newUpdates.contains(acc1.id))
    assert(newUpdates.contains(acc2.id))
    assert(newUpdates.contains(acc3.id))
    assert(newUpdates.contains(acc4.id))
    assert(newUpdates(acc1.id).name === Some("a"))
    assert(newUpdates(acc2.id).name === Some("b"))
    assert(newUpdates(acc3.id).name === Some("c"))
    assert(newUpdates(acc4.id).name === Some("d"))
    assert(newUpdates(acc1.id).value === 1)
    assert(newUpdates(acc2.id).value === 2)
    assert(newUpdates(acc3.id).value === 0)
    assert(newUpdates(acc4.id).value === 0)
    assert(!newUpdates(acc3.id).countFailedValues)
    assert(newUpdates(acc4.id).countFailedValues)
    assert(newUpdates.size === tm.internalAccums.size + 4)
  }
}


private[spark] object TaskMetricsSuite extends Assertions {

  /**
   * Assert that two lists of accumulator updates are equal.
   * Note: this does NOT check accumulator ID equality.
   */
  def assertUpdatesEquals(
      updates1: Seq[AccumulatorV2[_, _]],
      updates2: Seq[AccumulatorV2[_, _]]): Unit = {
    assert(updates1.size === updates2.size)
    updates1.zip(updates2).foreach { case (acc1, acc2) =>
      // do not assert ID equals here
      assert(acc1.name === acc2.name)
      assert(acc1.countFailedValues === acc2.countFailedValues)
      assert(acc1.value == acc2.value)
    }
  }
}

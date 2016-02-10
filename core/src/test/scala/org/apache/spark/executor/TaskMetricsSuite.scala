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
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus, StorageLevel, TestBlockId}


class TaskMetricsSuite extends SparkFunSuite {
  import AccumulatorParam._
  import InternalAccumulator._
  import StorageLevel._
  import TaskMetricsSuite._

  test("create") {
    val internalAccums = InternalAccumulator.createAll()
    val tm1 = new TaskMetrics
    val tm2 = new TaskMetrics(internalAccums)
    assert(tm1.accumulatorUpdates().size === internalAccums.size)
    assert(tm1.shuffleReadMetrics.isEmpty)
    assert(tm1.shuffleWriteMetrics.isEmpty)
    assert(tm1.inputMetrics.isEmpty)
    assert(tm1.outputMetrics.isEmpty)
    assert(tm2.accumulatorUpdates().size === internalAccums.size)
    assert(tm2.shuffleReadMetrics.isEmpty)
    assert(tm2.shuffleWriteMetrics.isEmpty)
    assert(tm2.inputMetrics.isEmpty)
    assert(tm2.outputMetrics.isEmpty)
    // TaskMetrics constructor expects minimal set of initial accumulators
    intercept[IllegalArgumentException] { new TaskMetrics(Seq.empty[Accumulator[_]]) }
  }

  test("create with unnamed accum") {
    intercept[IllegalArgumentException] {
      new TaskMetrics(
        InternalAccumulator.createAll() ++ Seq(
          new Accumulator(0, IntAccumulatorParam, None, internal = true)))
    }
  }

  test("create with duplicate name accum") {
    intercept[IllegalArgumentException] {
      new TaskMetrics(
        InternalAccumulator.createAll() ++ Seq(
          new Accumulator(0, IntAccumulatorParam, Some(RESULT_SIZE), internal = true)))
    }
  }

  test("create with external accum") {
    intercept[IllegalArgumentException] {
      new TaskMetrics(
        InternalAccumulator.createAll() ++ Seq(
          new Accumulator(0, IntAccumulatorParam, Some("x"))))
    }
  }

  test("create shuffle read metrics") {
    import shuffleRead._
    val accums = InternalAccumulator.createShuffleReadAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]]
    accums(REMOTE_BLOCKS_FETCHED).setValueAny(1)
    accums(LOCAL_BLOCKS_FETCHED).setValueAny(2)
    accums(REMOTE_BYTES_READ).setValueAny(3L)
    accums(LOCAL_BYTES_READ).setValueAny(4L)
    accums(FETCH_WAIT_TIME).setValueAny(5L)
    accums(RECORDS_READ).setValueAny(6L)
    val sr = new ShuffleReadMetrics(accums)
    assert(sr.remoteBlocksFetched === 1)
    assert(sr.localBlocksFetched === 2)
    assert(sr.remoteBytesRead === 3L)
    assert(sr.localBytesRead === 4L)
    assert(sr.fetchWaitTime === 5L)
    assert(sr.recordsRead === 6L)
  }

  test("create shuffle write metrics") {
    import shuffleWrite._
    val accums = InternalAccumulator.createShuffleWriteAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]]
    accums(BYTES_WRITTEN).setValueAny(1L)
    accums(RECORDS_WRITTEN).setValueAny(2L)
    accums(WRITE_TIME).setValueAny(3L)
    val sw = new ShuffleWriteMetrics(accums)
    assert(sw.bytesWritten === 1L)
    assert(sw.recordsWritten === 2L)
    assert(sw.writeTime === 3L)
  }

  test("create input metrics") {
    import input._
    val accums = InternalAccumulator.createInputAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]]
    accums(BYTES_READ).setValueAny(1L)
    accums(RECORDS_READ).setValueAny(2L)
    accums(READ_METHOD).setValueAny(DataReadMethod.Hadoop.toString)
    val im = new InputMetrics(accums)
    assert(im.bytesRead === 1L)
    assert(im.recordsRead === 2L)
    assert(im.readMethod === DataReadMethod.Hadoop)
  }

  test("create output metrics") {
    import output._
    val accums = InternalAccumulator.createOutputAccums()
      .map { a => (a.name.get, a) }.toMap[String, Accumulator[_]]
    accums(BYTES_WRITTEN).setValueAny(1L)
    accums(RECORDS_WRITTEN).setValueAny(2L)
    accums(WRITE_METHOD).setValueAny(DataWriteMethod.Hadoop.toString)
    val om = new OutputMetrics(accums)
    assert(om.bytesWritten === 1L)
    assert(om.recordsWritten === 2L)
    assert(om.writeMethod === DataWriteMethod.Hadoop)
  }

  test("mutating values") {
    val accums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(accums)
    // initial values
    assertValueEquals(tm, _.executorDeserializeTime, accums, EXECUTOR_DESERIALIZE_TIME, 0L)
    assertValueEquals(tm, _.executorRunTime, accums, EXECUTOR_RUN_TIME, 0L)
    assertValueEquals(tm, _.resultSize, accums, RESULT_SIZE, 0L)
    assertValueEquals(tm, _.jvmGCTime, accums, JVM_GC_TIME, 0L)
    assertValueEquals(tm, _.resultSerializationTime, accums, RESULT_SERIALIZATION_TIME, 0L)
    assertValueEquals(tm, _.memoryBytesSpilled, accums, MEMORY_BYTES_SPILLED, 0L)
    assertValueEquals(tm, _.diskBytesSpilled, accums, DISK_BYTES_SPILLED, 0L)
    assertValueEquals(tm, _.peakExecutionMemory, accums, PEAK_EXECUTION_MEMORY, 0L)
    assertValueEquals(tm, _.updatedBlockStatuses, accums, UPDATED_BLOCK_STATUSES,
      Seq.empty[(BlockId, BlockStatus)])
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
    tm.incUpdatedBlockStatuses(Seq(block1))
    tm.incUpdatedBlockStatuses(Seq(block2))
    // assert new values exist
    assertValueEquals(tm, _.executorDeserializeTime, accums, EXECUTOR_DESERIALIZE_TIME, 1L)
    assertValueEquals(tm, _.executorRunTime, accums, EXECUTOR_RUN_TIME, 2L)
    assertValueEquals(tm, _.resultSize, accums, RESULT_SIZE, 3L)
    assertValueEquals(tm, _.jvmGCTime, accums, JVM_GC_TIME, 4L)
    assertValueEquals(tm, _.resultSerializationTime, accums, RESULT_SERIALIZATION_TIME, 5L)
    assertValueEquals(tm, _.memoryBytesSpilled, accums, MEMORY_BYTES_SPILLED, 606L)
    assertValueEquals(tm, _.diskBytesSpilled, accums, DISK_BYTES_SPILLED, 707L)
    assertValueEquals(tm, _.peakExecutionMemory, accums, PEAK_EXECUTION_MEMORY, 808L)
    assertValueEquals(tm, _.updatedBlockStatuses, accums, UPDATED_BLOCK_STATUSES,
      Seq(block1, block2))
  }

  test("mutating shuffle read metrics values") {
    import shuffleRead._
    val accums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(accums)
    def assertValEquals[T](tmValue: ShuffleReadMetrics => T, name: String, value: T): Unit = {
      assertValueEquals(tm, tm => tmValue(tm.shuffleReadMetrics.get), accums, name, value)
    }
    // create shuffle read metrics
    assert(tm.shuffleReadMetrics.isEmpty)
    tm.registerTempShuffleReadMetrics()
    tm.mergeShuffleReadMetrics()
    assert(tm.shuffleReadMetrics.isDefined)
    val sr = tm.shuffleReadMetrics.get
    // initial values
    assertValEquals(_.remoteBlocksFetched, REMOTE_BLOCKS_FETCHED, 0)
    assertValEquals(_.localBlocksFetched, LOCAL_BLOCKS_FETCHED, 0)
    assertValEquals(_.remoteBytesRead, REMOTE_BYTES_READ, 0L)
    assertValEquals(_.localBytesRead, LOCAL_BYTES_READ, 0L)
    assertValEquals(_.fetchWaitTime, FETCH_WAIT_TIME, 0L)
    assertValEquals(_.recordsRead, RECORDS_READ, 0L)
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
    assertValEquals(_.remoteBlocksFetched, REMOTE_BLOCKS_FETCHED, 12)
    assertValEquals(_.localBlocksFetched, LOCAL_BLOCKS_FETCHED, 24)
    assertValEquals(_.remoteBytesRead, REMOTE_BYTES_READ, 36L)
    assertValEquals(_.localBytesRead, LOCAL_BYTES_READ, 48L)
    assertValEquals(_.fetchWaitTime, FETCH_WAIT_TIME, 60L)
    assertValEquals(_.recordsRead, RECORDS_READ, 72L)
  }

  test("mutating shuffle write metrics values") {
    import shuffleWrite._
    val accums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(accums)
    def assertValEquals[T](tmValue: ShuffleWriteMetrics => T, name: String, value: T): Unit = {
      assertValueEquals(tm, tm => tmValue(tm.shuffleWriteMetrics.get), accums, name, value)
    }
    // create shuffle write metrics
    assert(tm.shuffleWriteMetrics.isEmpty)
    tm.registerShuffleWriteMetrics()
    assert(tm.shuffleWriteMetrics.isDefined)
    val sw = tm.shuffleWriteMetrics.get
    // initial values
    assertValEquals(_.bytesWritten, BYTES_WRITTEN, 0L)
    assertValEquals(_.recordsWritten, RECORDS_WRITTEN, 0L)
    assertValEquals(_.writeTime, WRITE_TIME, 0L)
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
    assertValEquals(_.bytesWritten, BYTES_WRITTEN, 108L)
    assertValEquals(_.recordsWritten, RECORDS_WRITTEN, 216L)
    assertValEquals(_.writeTime, WRITE_TIME, 330L)
  }

  test("mutating input metrics values") {
    import input._
    val accums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(accums)
    def assertValEquals(tmValue: InputMetrics => Any, name: String, value: Any): Unit = {
      assertValueEquals(tm, tm => tmValue(tm.inputMetrics.get), accums, name, value,
        (x: Any, y: Any) => assert(x.toString === y.toString))
    }
    // create input metrics
    assert(tm.inputMetrics.isEmpty)
    tm.registerInputMetrics(DataReadMethod.Memory)
    assert(tm.inputMetrics.isDefined)
    val in = tm.inputMetrics.get
    // initial values
    assertValEquals(_.bytesRead, BYTES_READ, 0L)
    assertValEquals(_.recordsRead, RECORDS_READ, 0L)
    assertValEquals(_.readMethod, READ_METHOD, DataReadMethod.Memory)
    // set and increment values
    in.setBytesRead(1L)
    in.setBytesRead(2L)
    in.incRecordsRead(1L)
    in.incRecordsRead(2L)
    in.setReadMethod(DataReadMethod.Disk)
    // assert new values exist
    assertValEquals(_.bytesRead, BYTES_READ, 2L)
    assertValEquals(_.recordsRead, RECORDS_READ, 3L)
    assertValEquals(_.readMethod, READ_METHOD, DataReadMethod.Disk)
  }

  test("mutating output metrics values") {
    import output._
    val accums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(accums)
    def assertValEquals(tmValue: OutputMetrics => Any, name: String, value: Any): Unit = {
      assertValueEquals(tm, tm => tmValue(tm.outputMetrics.get), accums, name, value,
        (x: Any, y: Any) => assert(x.toString === y.toString))
    }
    // create input metrics
    assert(tm.outputMetrics.isEmpty)
    tm.registerOutputMetrics(DataWriteMethod.Hadoop)
    assert(tm.outputMetrics.isDefined)
    val out = tm.outputMetrics.get
    // initial values
    assertValEquals(_.bytesWritten, BYTES_WRITTEN, 0L)
    assertValEquals(_.recordsWritten, RECORDS_WRITTEN, 0L)
    assertValEquals(_.writeMethod, WRITE_METHOD, DataWriteMethod.Hadoop)
    // set values
    out.setBytesWritten(1L)
    out.setBytesWritten(2L)
    out.setRecordsWritten(3L)
    out.setRecordsWritten(4L)
    out.setWriteMethod(DataWriteMethod.Hadoop)
    // assert new values exist
    assertValEquals(_.bytesWritten, BYTES_WRITTEN, 2L)
    assertValEquals(_.recordsWritten, RECORDS_WRITTEN, 4L)
    // Note: this doesn't actually test anything, but there's only one DataWriteMethod
    // so we can't set it to anything else
    assertValEquals(_.writeMethod, WRITE_METHOD, DataWriteMethod.Hadoop)
  }

  test("merging multiple shuffle read metrics") {
    val tm = new TaskMetrics
    assert(tm.shuffleReadMetrics.isEmpty)
    val sr1 = tm.registerTempShuffleReadMetrics()
    val sr2 = tm.registerTempShuffleReadMetrics()
    val sr3 = tm.registerTempShuffleReadMetrics()
    assert(tm.shuffleReadMetrics.isEmpty)
    sr1.setRecordsRead(10L)
    sr2.setRecordsRead(10L)
    sr1.setFetchWaitTime(1L)
    sr2.setFetchWaitTime(2L)
    sr3.setFetchWaitTime(3L)
    tm.mergeShuffleReadMetrics()
    assert(tm.shuffleReadMetrics.isDefined)
    val sr = tm.shuffleReadMetrics.get
    assert(sr.remoteBlocksFetched === 0L)
    assert(sr.recordsRead === 20L)
    assert(sr.fetchWaitTime === 6L)

    // SPARK-5701: calling merge without any shuffle deps does nothing
    val tm2 = new TaskMetrics
    tm2.mergeShuffleReadMetrics()
    assert(tm2.shuffleReadMetrics.isEmpty)
  }

  test("register multiple shuffle write metrics") {
    val tm = new TaskMetrics
    val sw1 = tm.registerShuffleWriteMetrics()
    val sw2 = tm.registerShuffleWriteMetrics()
    assert(sw1 === sw2)
    assert(tm.shuffleWriteMetrics === Some(sw1))
  }

  test("register multiple input metrics") {
    val tm = new TaskMetrics
    val im1 = tm.registerInputMetrics(DataReadMethod.Memory)
    val im2 = tm.registerInputMetrics(DataReadMethod.Memory)
    // input metrics with a different read method than the one already registered are ignored
    val im3 = tm.registerInputMetrics(DataReadMethod.Hadoop)
    assert(im1 === im2)
    assert(im1 !== im3)
    assert(tm.inputMetrics === Some(im1))
    im2.setBytesRead(50L)
    im3.setBytesRead(100L)
    assert(tm.inputMetrics.get.bytesRead === 50L)
  }

  test("register multiple output metrics") {
    val tm = new TaskMetrics
    val om1 = tm.registerOutputMetrics(DataWriteMethod.Hadoop)
    val om2 = tm.registerOutputMetrics(DataWriteMethod.Hadoop)
    assert(om1 === om2)
    assert(tm.outputMetrics === Some(om1))
  }

  test("additional accumulables") {
    val internalAccums = InternalAccumulator.createAll()
    val tm = new TaskMetrics(internalAccums)
    assert(tm.accumulatorUpdates().size === internalAccums.size)
    val acc1 = new Accumulator(0, IntAccumulatorParam, Some("a"))
    val acc2 = new Accumulator(0, IntAccumulatorParam, Some("b"))
    val acc3 = new Accumulator(0, IntAccumulatorParam, Some("c"))
    val acc4 = new Accumulator(0, IntAccumulatorParam, Some("d"),
      internal = true, countFailedValues = true)
    tm.registerAccumulator(acc1)
    tm.registerAccumulator(acc2)
    tm.registerAccumulator(acc3)
    tm.registerAccumulator(acc4)
    acc1 += 1
    acc2 += 2
    val newUpdates = tm.accumulatorUpdates().map { a => (a.id, a) }.toMap
    assert(newUpdates.contains(acc1.id))
    assert(newUpdates.contains(acc2.id))
    assert(newUpdates.contains(acc3.id))
    assert(newUpdates.contains(acc4.id))
    assert(newUpdates(acc1.id).name === Some("a"))
    assert(newUpdates(acc2.id).name === Some("b"))
    assert(newUpdates(acc3.id).name === Some("c"))
    assert(newUpdates(acc4.id).name === Some("d"))
    assert(newUpdates(acc1.id).update === Some(1))
    assert(newUpdates(acc2.id).update === Some(2))
    assert(newUpdates(acc3.id).update === Some(0))
    assert(newUpdates(acc4.id).update === Some(0))
    assert(!newUpdates(acc3.id).internal)
    assert(!newUpdates(acc3.id).countFailedValues)
    assert(newUpdates(acc4.id).internal)
    assert(newUpdates(acc4.id).countFailedValues)
    assert(newUpdates.values.map(_.update).forall(_.isDefined))
    assert(newUpdates.values.map(_.value).forall(_.isEmpty))
    assert(newUpdates.size === internalAccums.size + 4)
  }

  test("existing values in shuffle read accums") {
    // set shuffle read accum before passing it into TaskMetrics
    val accums = InternalAccumulator.createAll()
    val srAccum = accums.find(_.name === Some(shuffleRead.FETCH_WAIT_TIME))
    assert(srAccum.isDefined)
    srAccum.get.asInstanceOf[Accumulator[Long]] += 10L
    val tm = new TaskMetrics(accums)
    assert(tm.shuffleReadMetrics.isDefined)
    assert(tm.shuffleWriteMetrics.isEmpty)
    assert(tm.inputMetrics.isEmpty)
    assert(tm.outputMetrics.isEmpty)
  }

  test("existing values in shuffle write accums") {
    // set shuffle write accum before passing it into TaskMetrics
    val accums = InternalAccumulator.createAll()
    val swAccum = accums.find(_.name === Some(shuffleWrite.RECORDS_WRITTEN))
    assert(swAccum.isDefined)
    swAccum.get.asInstanceOf[Accumulator[Long]] += 10L
    val tm = new TaskMetrics(accums)
    assert(tm.shuffleReadMetrics.isEmpty)
    assert(tm.shuffleWriteMetrics.isDefined)
    assert(tm.inputMetrics.isEmpty)
    assert(tm.outputMetrics.isEmpty)
  }

  test("existing values in input accums") {
    // set input accum before passing it into TaskMetrics
    val accums = InternalAccumulator.createAll()
    val inAccum = accums.find(_.name === Some(input.RECORDS_READ))
    assert(inAccum.isDefined)
    inAccum.get.asInstanceOf[Accumulator[Long]] += 10L
    val tm = new TaskMetrics(accums)
    assert(tm.shuffleReadMetrics.isEmpty)
    assert(tm.shuffleWriteMetrics.isEmpty)
    assert(tm.inputMetrics.isDefined)
    assert(tm.outputMetrics.isEmpty)
  }

  test("existing values in output accums") {
    // set output accum before passing it into TaskMetrics
    val accums = InternalAccumulator.createAll()
    val outAccum = accums.find(_.name === Some(output.RECORDS_WRITTEN))
    assert(outAccum.isDefined)
    outAccum.get.asInstanceOf[Accumulator[Long]] += 10L
    val tm4 = new TaskMetrics(accums)
    assert(tm4.shuffleReadMetrics.isEmpty)
    assert(tm4.shuffleWriteMetrics.isEmpty)
    assert(tm4.inputMetrics.isEmpty)
    assert(tm4.outputMetrics.isDefined)
  }

  test("from accumulator updates") {
    val accumUpdates1 = InternalAccumulator.createAll().map { a =>
      AccumulableInfo(a.id, a.name, Some(3L), None, a.isInternal, a.countFailedValues)
    }
    val metrics1 = TaskMetrics.fromAccumulatorUpdates(accumUpdates1)
    assertUpdatesEquals(metrics1.accumulatorUpdates(), accumUpdates1)
    // Test this with additional accumulators. Only the ones registered with `Accumulators`
    // will show up in the reconstructed TaskMetrics. In practice, all accumulators created
    // on the driver, internal or not, should be registered with `Accumulators` at some point.
    // Here we show that reconstruction will succeed even if there are unregistered accumulators.
    val param = IntAccumulatorParam
    val registeredAccums = Seq(
      new Accumulator(0, param, Some("a"), internal = true, countFailedValues = true),
      new Accumulator(0, param, Some("b"), internal = true, countFailedValues = false),
      new Accumulator(0, param, Some("c"), internal = false, countFailedValues = true),
      new Accumulator(0, param, Some("d"), internal = false, countFailedValues = false))
    val unregisteredAccums = Seq(
      new Accumulator(0, param, Some("e"), internal = true, countFailedValues = true),
      new Accumulator(0, param, Some("f"), internal = true, countFailedValues = false))
    registeredAccums.foreach(Accumulators.register)
    registeredAccums.foreach { a => assert(Accumulators.originals.contains(a.id)) }
    unregisteredAccums.foreach { a => assert(!Accumulators.originals.contains(a.id)) }
    // set some values in these accums
    registeredAccums.zipWithIndex.foreach { case (a, i) => a.setValue(i) }
    unregisteredAccums.zipWithIndex.foreach { case (a, i) => a.setValue(i) }
    val registeredAccumInfos = registeredAccums.map(makeInfo)
    val unregisteredAccumInfos = unregisteredAccums.map(makeInfo)
    val accumUpdates2 = accumUpdates1 ++ registeredAccumInfos ++ unregisteredAccumInfos
    val metrics2 = TaskMetrics.fromAccumulatorUpdates(accumUpdates2)
    // accumulators that were not registered with `Accumulators` will not show up
    assertUpdatesEquals(metrics2.accumulatorUpdates(), accumUpdates1 ++ registeredAccumInfos)
  }
}


private[spark] object TaskMetricsSuite extends Assertions {

  /**
   * Assert that the following three things are equal to `value`:
   *   (1) TaskMetrics value
   *   (2) TaskMetrics accumulator update value
   *   (3) Original accumulator value
   */
  def assertValueEquals(
      tm: TaskMetrics,
      tmValue: TaskMetrics => Any,
      accums: Seq[Accumulator[_]],
      metricName: String,
      value: Any,
      assertEquals: (Any, Any) => Unit = (x: Any, y: Any) => assert(x === y)): Unit = {
    assertEquals(tmValue(tm), value)
    val accum = accums.find(_.name == Some(metricName))
    assert(accum.isDefined)
    assertEquals(accum.get.value, value)
    val accumUpdate = tm.accumulatorUpdates().find(_.name == Some(metricName))
    assert(accumUpdate.isDefined)
    assert(accumUpdate.get.value === None)
    assertEquals(accumUpdate.get.update, Some(value))
  }

  /**
   * Assert that two lists of accumulator updates are equal.
   * Note: this does NOT check accumulator ID equality.
   */
  def assertUpdatesEquals(
      updates1: Seq[AccumulableInfo],
      updates2: Seq[AccumulableInfo]): Unit = {
    assert(updates1.size === updates2.size)
    updates1.zip(updates2).foreach { case (info1, info2) =>
      // do not assert ID equals here
      assert(info1.name === info2.name)
      assert(info1.update === info2.update)
      assert(info1.value === info2.value)
      assert(info1.internal === info2.internal)
      assert(info1.countFailedValues === info2.countFailedValues)
    }
  }

  /**
   * Make an [[AccumulableInfo]] out of an [[Accumulable]] with the intent to use the
   * info as an accumulator update.
   */
  def makeInfo(a: Accumulable[_, _]): AccumulableInfo = a.toInfo(Some(a.value), None)

}

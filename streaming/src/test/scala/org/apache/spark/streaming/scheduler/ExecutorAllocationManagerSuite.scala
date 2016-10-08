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

package org.apache.spark.streaming.scheduler

import org.mockito.Matchers.{eq => meq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.SpanSugar._

import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkFunSuite}
import org.apache.spark.streaming.{DummyInputDStream, Seconds, StreamingContext}
import org.apache.spark.util.{ManualClock, Utils}


class ExecutorAllocationManagerSuite extends SparkFunSuite
  with BeforeAndAfter with BeforeAndAfterAll with MockitoSugar with PrivateMethodTester {

  import ExecutorAllocationManager._

  private val batchDurationMillis = 1000L
  private var allocationClient: ExecutorAllocationClient = null
  private var clock: ManualClock = null

  before {
    allocationClient = mock[ExecutorAllocationClient]
    clock = new ManualClock()
  }

  test("basic functionality") {
    // Test that adding batch processing time info to allocation manager
    // causes executors to be requested and killed accordingly

    // There is 1 receiver, and exec 1 has been allocated to it
    withAllocationManager(numReceivers = 1) { case (receiverTracker, allocationManager) =>
      when(receiverTracker.allocatedExecutors).thenReturn(Map(1 -> Some("1")))

      /** Add data point for batch processing time and verify executor allocation */
      def addBatchProcTimeAndVerifyAllocation(batchProcTimeMs: Double)(body: => Unit): Unit = {
        // 2 active executors
        reset(allocationClient)
        when(allocationClient.getExecutorIds()).thenReturn(Seq("1", "2"))
        addBatchProcTime(allocationManager, batchProcTimeMs.toLong)
        clock.advance(SCALING_INTERVAL_DEFAULT_SECS * 1000 + 1)
        eventually(timeout(10 seconds)) {
          body
        }
      }

      /** Verify that the expected number of total executor were requested */
      def verifyTotalRequestedExecs(expectedRequestedTotalExecs: Option[Int]): Unit = {
        if (expectedRequestedTotalExecs.nonEmpty) {
          require(expectedRequestedTotalExecs.get > 0)
          verify(allocationClient, times(1)).requestTotalExecutors(
            meq(expectedRequestedTotalExecs.get), meq(0), meq(Map.empty))
        } else {
          verify(allocationClient, never).requestTotalExecutors(0, 0, Map.empty)
        }
      }

      /** Verify that a particular executor was killed */
      def verifyKilledExec(expectedKilledExec: Option[String]): Unit = {
        if (expectedKilledExec.nonEmpty) {
          verify(allocationClient, times(1)).killExecutor(meq(expectedKilledExec.get))
        } else {
          verify(allocationClient, never).killExecutor(null)
        }
      }

      // Batch proc time = batch interval, should increase allocation by 1
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis) {
        verifyTotalRequestedExecs(Some(3)) // one already allocated, increase allocation by 1
        verifyKilledExec(None)
      }

      // Batch proc time = batch interval * 2, should increase allocation by 2
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis * 2) {
        verifyTotalRequestedExecs(Some(4))
        verifyKilledExec(None)
      }

      // Batch proc time slightly more than the scale up ratio, should increase allocation by 1
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis * SCALING_UP_RATIO_DEFAULT + 1) {
        verifyTotalRequestedExecs(Some(3))
        verifyKilledExec(None)
      }

      // Batch proc time slightly less than the scale up ratio, should not change allocation
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis * SCALING_UP_RATIO_DEFAULT - 1) {
        verifyTotalRequestedExecs(None)
        verifyKilledExec(None)
      }

      // Batch proc time slightly more than the scale down ratio, should not change allocation
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis * SCALING_DOWN_RATIO_DEFAULT + 1) {
        verifyTotalRequestedExecs(None)
        verifyKilledExec(None)
      }

      // Batch proc time slightly more than the scale down ratio, should not change allocation
      addBatchProcTimeAndVerifyAllocation(batchDurationMillis * SCALING_DOWN_RATIO_DEFAULT - 1) {
        verifyTotalRequestedExecs(None)
        verifyKilledExec(Some("2"))
      }
    }
  }

  test("requestExecutors policy") {

    /** Verify that the expected number of total executor were requested */
    def verifyRequestedExecs(
        numExecs: Int,
        numNewExecs: Int,
        expectedRequestedTotalExecs: Int)(
      implicit allocationManager: ExecutorAllocationManager): Unit = {
      reset(allocationClient)
      when(allocationClient.getExecutorIds()).thenReturn((1 to numExecs).map(_.toString))
      requestExecutors(allocationManager, numNewExecs)
      verify(allocationClient, times(1)).requestTotalExecutors(
        meq(expectedRequestedTotalExecs), meq(0), meq(Map.empty))
    }

    withAllocationManager(numReceivers = 1) { case (_, allocationManager) =>
      implicit val am = allocationManager
      intercept[IllegalArgumentException] {
        verifyRequestedExecs(numExecs = 0, numNewExecs = 0, 0)
      }
      verifyRequestedExecs(numExecs = 0, numNewExecs = 1, expectedRequestedTotalExecs = 1)
      verifyRequestedExecs(numExecs = 1, numNewExecs = 1, expectedRequestedTotalExecs = 2)
      verifyRequestedExecs(numExecs = 2, numNewExecs = 2, expectedRequestedTotalExecs = 4)
    }

    withAllocationManager(numReceivers = 2) { case(_, allocationManager) =>
      implicit val am = allocationManager

      verifyRequestedExecs(numExecs = 0, numNewExecs = 1, expectedRequestedTotalExecs = 2)
      verifyRequestedExecs(numExecs = 1, numNewExecs = 1, expectedRequestedTotalExecs = 2)
      verifyRequestedExecs(numExecs = 2, numNewExecs = 2, expectedRequestedTotalExecs = 4)
    }

    withAllocationManager(
      // Test min 2 executors
      new SparkConf().set("spark.streaming.dynamicAllocation.minExecutors", "2")) {
      case (_, allocationManager) =>
        implicit val am = allocationManager

        verifyRequestedExecs(numExecs = 0, numNewExecs = 1, expectedRequestedTotalExecs = 2)
        verifyRequestedExecs(numExecs = 0, numNewExecs = 3, expectedRequestedTotalExecs = 3)
        verifyRequestedExecs(numExecs = 1, numNewExecs = 1, expectedRequestedTotalExecs = 2)
        verifyRequestedExecs(numExecs = 1, numNewExecs = 2, expectedRequestedTotalExecs = 3)
        verifyRequestedExecs(numExecs = 2, numNewExecs = 1, expectedRequestedTotalExecs = 3)
        verifyRequestedExecs(numExecs = 2, numNewExecs = 2, expectedRequestedTotalExecs = 4)
    }

    withAllocationManager(
      // Test with max 2 executors
      new SparkConf().set("spark.streaming.dynamicAllocation.maxExecutors", "2")) {
      case (_, allocationManager) =>
        implicit val am = allocationManager

        verifyRequestedExecs(numExecs = 0, numNewExecs = 1, expectedRequestedTotalExecs = 1)
        verifyRequestedExecs(numExecs = 0, numNewExecs = 3, expectedRequestedTotalExecs = 2)
        verifyRequestedExecs(numExecs = 1, numNewExecs = 2, expectedRequestedTotalExecs = 2)
        verifyRequestedExecs(numExecs = 2, numNewExecs = 1, expectedRequestedTotalExecs = 2)
        verifyRequestedExecs(numExecs = 2, numNewExecs = 2, expectedRequestedTotalExecs = 2)
    }
  }

  test("killExecutor policy") {

    /**
     * Verify that a particular executor was killed, given active executors and executors
     * allocated to receivers.
     */
    def verifyKilledExec(
        execIds: Seq[String],
        receiverExecIds: Map[Int, Option[String]],
        expectedKilledExec: Option[String])(
        implicit x: (ReceiverTracker, ExecutorAllocationManager)): Unit = {
      val (receiverTracker, allocationManager) = x

      reset(allocationClient)
      when(allocationClient.getExecutorIds()).thenReturn(execIds)
      when(receiverTracker.allocatedExecutors).thenReturn(receiverExecIds)
      killExecutor(allocationManager)
      if (expectedKilledExec.nonEmpty) {
        verify(allocationClient, times(1)).killExecutor(meq(expectedKilledExec.get))
      } else {
        verify(allocationClient, never).killExecutor(null)
      }
    }

    withAllocationManager() { case (receiverTracker, allocationManager) =>
      implicit val rcvrTrackerAndExecAllocMgr = (receiverTracker, allocationManager)

      verifyKilledExec(Nil, Map.empty, None)
      verifyKilledExec(Seq("1", "2"), Map.empty, None)
      verifyKilledExec(Seq("1"), Map(1 -> Some("1")), None)
      verifyKilledExec(Seq("1", "2"), Map(1 -> Some("1")), Some("2"))
      verifyKilledExec(Seq("1", "2"), Map(1 -> Some("1"), 2 -> Some("2")), None)
    }

    withAllocationManager(
      new SparkConf().set("spark.streaming.dynamicAllocation.minExecutors", "2")) {
      case (receiverTracker, allocationManager) =>
        implicit val rcvrTrackerAndExecAllocMgr = (receiverTracker, allocationManager)

        verifyKilledExec(Seq("1", "2"), Map.empty, None)
        verifyKilledExec(Seq("1", "2", "3"), Map(1 -> Some("1"), 2 -> Some("2")), Some("3"))
    }
  }

  test("parameter validation") {

    def validateParams(
        numReceivers: Int = 1,
        scalingIntervalSecs: Option[Int] = None,
        scalingUpRatio: Option[Double] = None,
        scalingDownRatio: Option[Double] = None,
        minExecs: Option[Int] = None,
        maxExecs: Option[Int] = None): Unit = {
      require(numReceivers > 0)
      val receiverTracker = mock[ReceiverTracker]
      when(receiverTracker.numReceivers()).thenReturn(numReceivers)
      val conf = new SparkConf()
      if (scalingIntervalSecs.nonEmpty) {
        conf.set(
          "spark.streaming.dynamicAllocation.scalingInterval",
          s"${scalingIntervalSecs.get}s")
      }
      if (scalingUpRatio.nonEmpty) {
        conf.set("spark.streaming.dynamicAllocation.scalingUpRatio", scalingUpRatio.get.toString)
      }
      if (scalingDownRatio.nonEmpty) {
        conf.set(
          "spark.streaming.dynamicAllocation.scalingDownRatio",
          scalingDownRatio.get.toString)
      }
      if (minExecs.nonEmpty) {
        conf.set("spark.streaming.dynamicAllocation.minExecutors", minExecs.get.toString)
      }
      if (maxExecs.nonEmpty) {
        conf.set("spark.streaming.dynamicAllocation.maxExecutors", maxExecs.get.toString)
      }
      new ExecutorAllocationManager(
        allocationClient, receiverTracker, conf, batchDurationMillis, clock)
    }

    validateParams(numReceivers = 1)
    validateParams(numReceivers = 2, minExecs = Some(1))
    validateParams(numReceivers = 2, minExecs = Some(3))
    validateParams(numReceivers = 2, maxExecs = Some(3))
    validateParams(numReceivers = 2, maxExecs = Some(1))
    validateParams(minExecs = Some(3), maxExecs = Some(3))
    validateParams(scalingIntervalSecs = Some(1))
    validateParams(scalingUpRatio = Some(1.1))
    validateParams(scalingDownRatio = Some(0.1))
    validateParams(scalingUpRatio = Some(1.1), scalingDownRatio = Some(0.1))

    intercept[IllegalArgumentException] {
      validateParams(minExecs = Some(0))
    }
    intercept[IllegalArgumentException] {
      validateParams(minExecs = Some(-1))
    }
    intercept[IllegalArgumentException] {
      validateParams(maxExecs = Some(0))
    }
    intercept[IllegalArgumentException] {
      validateParams(maxExecs = Some(-1))
    }
    intercept[IllegalArgumentException] {
      validateParams(minExecs = Some(4), maxExecs = Some(3))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingIntervalSecs = Some(-1))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingIntervalSecs = Some(0))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingUpRatio = Some(-0.1))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingUpRatio = Some(0))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingDownRatio = Some(-0.1))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingDownRatio = Some(0))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingUpRatio = Some(0.5), scalingDownRatio = Some(0.5))
    }
    intercept[IllegalArgumentException] {
      validateParams(scalingUpRatio = Some(0.3), scalingDownRatio = Some(0.5))
    }
  }

  test("enabling and disabling") {
    withStreamingContext(new SparkConf()) { ssc =>
      ssc.start()
      assert(getExecutorAllocationManager(ssc).isEmpty)
    }

    withStreamingContext(
      new SparkConf().set("spark.streaming.dynamicAllocation.enabled", "true")) { ssc =>
      ssc.start()
      assert(getExecutorAllocationManager(ssc).nonEmpty)
    }

    val confWithBothDynamicAllocationEnabled = new SparkConf()
      .set("spark.streaming.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.testing", "true")
    require(Utils.isDynamicAllocationEnabled(confWithBothDynamicAllocationEnabled) === true)
    withStreamingContext(confWithBothDynamicAllocationEnabled) { ssc =>
      intercept[IllegalArgumentException] {
        ssc.start()
      }
    }
  }

  private def withAllocationManager(
      conf: SparkConf = new SparkConf,
      numReceivers: Int = 1
    )(body: (ReceiverTracker, ExecutorAllocationManager) => Unit): Unit = {

    val receiverTracker = mock[ReceiverTracker]
    when(receiverTracker.numReceivers()).thenReturn(numReceivers)

    val manager = new ExecutorAllocationManager(
      allocationClient, receiverTracker, conf, batchDurationMillis, clock)
    try {
      manager.start()
      body(receiverTracker, manager)
    } finally {
      manager.stop()
    }
  }

  private val _addBatchProcTime = PrivateMethod[Unit]('addBatchProcTime)
  private val _requestExecutors = PrivateMethod[Unit]('requestExecutors)
  private val _killExecutor = PrivateMethod[Unit]('killExecutor)
  private val _executorAllocationManager =
    PrivateMethod[Option[ExecutorAllocationManager]]('executorAllocationManager)

  private def addBatchProcTime(manager: ExecutorAllocationManager, timeMs: Long): Unit = {
    manager invokePrivate _addBatchProcTime(timeMs)
  }

  private def requestExecutors(manager: ExecutorAllocationManager, newExecs: Int): Unit = {
    manager invokePrivate _requestExecutors(newExecs)
  }

  private def killExecutor(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _killExecutor()
  }

  private def getExecutorAllocationManager(
      ssc: StreamingContext): Option[ExecutorAllocationManager] = {
    ssc.scheduler invokePrivate _executorAllocationManager()
  }

  private def withStreamingContext(conf: SparkConf)(body: StreamingContext => Unit): Unit = {
    conf.setMaster("myDummyLocalExternalClusterManager")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.dynamicAllocation.testing", "true")  // to test dynamic allocation

    var ssc: StreamingContext = null
    try {
      ssc = new  StreamingContext(conf, Seconds(1))
      new DummyInputDStream(ssc).foreachRDD(_ => { })
      body(ssc)
    } finally {
      if (ssc != null) ssc.stop()
    }
  }
}

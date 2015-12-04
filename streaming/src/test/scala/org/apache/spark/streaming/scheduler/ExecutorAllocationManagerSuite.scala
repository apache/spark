package org.apache.spark.streaming.scheduler

import scala.collection.mutable.ArrayBuffer

import org.mockito.Matchers.{eq => meq}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, PrivateMethodTester}

import org.apache.spark.util.ManualClock
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkFunSuite}


class ExecutorAllocationManagerSuite extends SparkFunSuite
  with BeforeAndAfter with BeforeAndAfterAll with MockitoSugar with PrivateMethodTester {

  import ExecutorAllocationManager._

  private var allocationClient: ExecutorAllocationClient = null
  private var receiverTracker: ReceiverTracker = null
  private var allocationManagers = new ArrayBuffer[ExecutorAllocationManager]
  private var clock: ManualClock = null
  private val batchDurationMillis = 1000L

  before {
    allocationClient = mock[ExecutorAllocationClient]
    receiverTracker = mock[ReceiverTracker]
    clock = new ManualClock()
  }

  after {
    allocationManagers.foreach { _.stop() }
  }

  test("basic functionality") {
    val allocationManager = createManager()

    allocationManager.start()

    def advanceTimeAndAssert(batchProcTimeMs: Double)(body: => Unit): Unit = {
      reset(allocationClient)
      reset(receiverTracker)
      when(allocationClient.getExecutorIds()).thenReturn(Seq("1"))
      when(receiverTracker.getAllocatedExecutors).thenReturn(Map.empty[Int, Option[String]])
      addBatchProcTime(allocationManager, batchProcTimeMs.toLong)
      clock.advance(DEFAULT_SCALING_INTERVAL_SECS * 1000)
      eventually(timeout(10 seconds)) {
        body
      }
    }

    def verifyRequestExec(expectedRequestedTotalExecs: Int): Unit = {
      if (expectedRequestedTotalExecs > 0) {
        verify(allocationClient, times(1)).requestTotalExecutors(
          meq(expectedRequestedTotalExecs), meq(0), meq(Map.empty))
      } else {
        verify(allocationClient, never()).requestTotalExecutors(0, 0, Map.empty)
      }
    }

    def verifyKillExec(expectedKilledExec: Option[String]): Unit = {
      if (expectedKilledExec.nonEmpty) {
        verify(allocationClient, times(1)).killExecutor(meq(expectedKilledExec.get))
      } else {
        verify(allocationClient, never()).killExecutor(null)
      }
    }


    advanceTimeAndAssert(batchDurationMillis) {
      verifyRequestExec(2)
      verifyKillExec(None)
    }

    advanceTimeAndAssert(batchDurationMillis * 2) {
      verifyRequestExec(3)
      verifyKillExec(None)
    }

    advanceTimeAndAssert(batchDurationMillis * DEFAULT_SCALE_UP_RATIO + 1) {
      verifyRequestExec(2)
      verifyKillExec(None)
    }

    advanceTimeAndAssert(batchDurationMillis * DEFAULT_SCALE_UP_RATIO - 1) {
      verifyRequestExec(0)
      verifyKillExec(None)
    }

    advanceTimeAndAssert(batchDurationMillis * DEFAULT_SCALE_DOWN_RATIO + 1) {
      verifyRequestExec(0)
      verifyKillExec(None)
    }

    advanceTimeAndAssert(batchDurationMillis * DEFAULT_SCALE_DOWN_RATIO - 1) {
      verifyRequestExec(0)
      verifyKillExec(Some("1"))
    }
  }

  test("requestExecutors") {
    val allocationManager = createManager()

    def assert(
        execIds: Seq[String],
        ratio: Double,
        expectedRequestedTotalExecs: Int): Unit = {
      reset(allocationClient)
      when(allocationClient.getExecutorIds()).thenReturn(execIds)
      requestExecutors(allocationManager, ratio)
      verify(allocationClient, times(1)).requestTotalExecutors(
        meq(expectedRequestedTotalExecs), meq(0), meq(Map.empty))
    }

    assert(Nil, 0, 1)
    assert(Nil, 1, 1)
    assert(Nil, 1.1, 1)
    assert(Nil, 1.6, 2)

    assert(Seq("1"), 1, 2)
    assert(Seq("1"), 1.1, 2)
    assert(Seq("1", "2"), 1.6, 4)
  }

  test("killExecutor") {
    val allocationManager = createManager()
    def assert(
        execIds: Seq[String],
      receiverExecIds: Map[Int, Option[String]],
      expectedKilledExec: Option[String]): Unit = {
      reset(allocationClient)
      reset(receiverTracker)
      when(allocationClient.getExecutorIds()).thenReturn(execIds)
      when(receiverTracker.getAllocatedExecutors).thenReturn(receiverExecIds)
      killExecutor(allocationManager)
      if (expectedKilledExec.nonEmpty) {
        verify(allocationClient, times(1)).killExecutor(meq(expectedKilledExec.get))
      } else {
        verify(allocationClient, never()).killExecutor(null)
      }
    }

    assert(Nil, Map.empty, None)
    assert(Seq("1"), Map.empty, Some("1"))
    assert(Seq("1"), Map(1 -> Some("1")), None)
    assert(Seq("1", "2"), Map(1 -> Some("1")), Some("2"))
  }


  private def createManager(conf: SparkConf = new SparkConf): ExecutorAllocationManager = {
    val manager = new ExecutorAllocationManager(
      allocationClient, receiverTracker, conf, batchDurationMillis, clock)
    allocationManagers += manager
    manager
  }

  private val _addBatchProcTime = PrivateMethod[Unit]('addBatchProcTime)
  private val _requestExecutors = PrivateMethod[Unit]('requestExecutors)
  private val _killExecutor = PrivateMethod[Unit]('killExecutor)

  private def addBatchProcTime(manager: ExecutorAllocationManager, timeMs: Long): Unit = {
    manager invokePrivate _addBatchProcTime(timeMs)
  }

  private def requestExecutors(manager: ExecutorAllocationManager, ratio: Double): Unit = {
    manager invokePrivate _requestExecutors(ratio)
  }

  private def killExecutor(manager: ExecutorAllocationManager): Unit = {
    manager invokePrivate _killExecutor()
  }
}

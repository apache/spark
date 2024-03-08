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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.concurrent.{Eventually, Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.util.StreamManualClock

class ProcessingTimeExecutorSuite extends SparkFunSuite with TimeLimits {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  val timeout = 10.seconds

  test("nextBatchTime") {
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTimeTrigger(100))
    assert(processingTimeExecutor.nextBatchTime(0) === 100)
    assert(processingTimeExecutor.nextBatchTime(1) === 100)
    assert(processingTimeExecutor.nextBatchTime(99) === 100)
    assert(processingTimeExecutor.nextBatchTime(100) === 200)
    assert(processingTimeExecutor.nextBatchTime(101) === 200)
    assert(processingTimeExecutor.nextBatchTime(150) === 200)
  }

  test("trigger timing") {
    val triggerTimes = ConcurrentHashMap.newKeySet[Int]()
    val clock = new StreamManualClock()
    @volatile var continueExecuting = true
    @volatile var clockIncrementInTrigger = 0L
    val executor = ProcessingTimeExecutor(ProcessingTimeTrigger("1000 milliseconds"), clock)
    val executorThread = new Thread() {
      override def run(): Unit = {
        executor.execute((_) => {
          // Record the trigger time, increment clock if needed and
          triggerTimes.add(clock.getTimeMillis().toInt)
          clock.advance(clockIncrementInTrigger)
          clockIncrementInTrigger = 0 // reset this so that there are no runaway triggers
          continueExecuting
        })
      }
    }
    executorThread.start()
    // First batch should execute immediately, then executor should wait for next one
    eventually {
      assert(triggerTimes.contains(0))
      assert(clock.isStreamWaitingAt(0))
      assert(clock.isStreamWaitingFor(1000))
    }

    // Second batch should execute when clock reaches the next trigger time.
    // If next trigger takes less than the trigger interval, executor should wait for next one
    clockIncrementInTrigger = 500
    clock.setTime(1000)
    eventually {
      assert(triggerTimes.contains(1000))
      assert(clock.isStreamWaitingAt(1500))
      assert(clock.isStreamWaitingFor(2000))
    }

    // If next trigger takes less than the trigger interval, executor should immediately execute
    // another one
    clockIncrementInTrigger = 1500
    clock.setTime(2000)   // allow another trigger by setting clock to 2000
    eventually {
      // Since the next trigger will take 1500 (which is more than trigger interval of 1000)
      // executor will immediately execute another trigger
      assert(triggerTimes.contains(2000) && triggerTimes.contains(3500))
      assert(clock.isStreamWaitingAt(3500))
      assert(clock.isStreamWaitingFor(4000))
    }
    continueExecuting = false
    clock.advance(1000)
    waitForThreadJoin(executorThread)
  }

  test("calling nextBatchTime with the result of a previous call should return the next interval") {
    val intervalMS = 100
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTimeTrigger(intervalMS))

    val ITERATION = 10
    var nextBatchTime: Long = 0
    for (it <- 1 to ITERATION) {
      nextBatchTime = processingTimeExecutor.nextBatchTime(nextBatchTime)
    }

    // nextBatchTime should be 1000
    assert(nextBatchTime === intervalMS * ITERATION)
  }

  private def testBatchTermination(intervalMs: Long): Unit = {
    var batchCounts = 0
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTimeTrigger(intervalMs))
    processingTimeExecutor.execute((_) => {
      batchCounts += 1
      // If the batch termination works correctly, batchCounts should be 3 after `execute`
      batchCounts < 3
    })
    assert(batchCounts === 3)
  }

  test("batch termination") {
    testBatchTermination(0)
    testBatchTermination(10)
  }

  test("notifyBatchFallingBehind") {
    val clock = new StreamManualClock()
    @volatile var batchFallingBehindCalled = false
    val t = new Thread() {
      override def run(): Unit = {
        val processingTimeExecutor = new ProcessingTimeExecutor(ProcessingTimeTrigger(100), clock) {
          override def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
            batchFallingBehindCalled = true
          }
        }
        processingTimeExecutor.execute((_) => {
          clock.waitTillTime(200)
          false
        })
      }
    }
    t.start()
    // Wait until the batch is running so that we don't call `advance` too early
    eventually { assert(clock.isStreamWaitingFor(200)) }
    clock.advance(200)
    waitForThreadJoin(t)
    assert(batchFallingBehindCalled)
  }

  private def eventually(body: => Unit): Unit = {
    Eventually.eventually(Timeout(timeout)) { body }
  }

  private def waitForThreadJoin(thread: Thread): Unit = {
    failAfter(timeout) { thread.join() }
  }
}

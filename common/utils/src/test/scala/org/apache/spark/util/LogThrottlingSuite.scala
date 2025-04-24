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

package org.apache.spark.util

import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{DeadlineWithTimeSource, Logging, LogThrottler, NanoTimeTimeSource}


class LogThrottlingSuite
  extends AnyFunSuite  // scalastyle:ignore funsuite
  with Logging {

  // Make sure that the helper works right.
  test("time control") {
    val nanoTimeControl = new MockedNanoTime
    assert(nanoTimeControl.nanoTime() === 0L)
    assert(nanoTimeControl.nanoTime() === 0L)

    nanoTimeControl.advance(112L.nanos)
    assert(nanoTimeControl.nanoTime() === 112L)
    assert(nanoTimeControl.nanoTime() === 112L)
  }

  test("deadline with time control") {
    val nanoTimeControl = new MockedNanoTime
    assert(DeadlineWithTimeSource.now(nanoTimeControl).isOverdue())
    val deadline = DeadlineWithTimeSource.now(nanoTimeControl) + 5.nanos
    assert(!deadline.isOverdue())
    nanoTimeControl.advance(5.nanos)
    assert(deadline.isOverdue())
    nanoTimeControl.advance(5.nanos)
    assert(deadline.isOverdue())
    // Check addition.
    assert(deadline + 0.nanos === deadline)
    val increasedDeadline = deadline + 10.nanos
    assert(!increasedDeadline.isOverdue())
    nanoTimeControl.advance(5.nanos)
    assert(increasedDeadline.isOverdue())
    // Ensure that wrapping keeps throwing this exact exception, since we rely on it in
    // LogThrottler.tryRecoverTokens
    assertThrows[IllegalArgumentException] {
      deadline + Long.MaxValue.nanos
    }
    // Check difference and ordering.
    assert(deadline - deadline === 0.nanos)
    assert(increasedDeadline - deadline === 10.nanos)
    assert(increasedDeadline - deadline > 9.nanos)
    assert(increasedDeadline - deadline < 11.nanos)
    assert(deadline - increasedDeadline === -10.nanos)
    assert(deadline - increasedDeadline < -9.nanos)
    assert(deadline - increasedDeadline > -11.nanos)
  }

  test("unthrottled, no burst") {
    val nanotTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 1,
      tokenRecoveryInterval = 5.nanos,
      timeSource = nanotTimeControl)
    val numInvocations = 100
    var timesExecuted = 0
    for (i <- 0 until numInvocations) {
      throttler.throttled { skipped =>
        assert(skipped === 0L)
        timesExecuted += 1
      }
      nanotTimeControl.advance(5.nanos)
    }
    assert(timesExecuted === numInvocations)
  }

  test("unthrottled, burst") {
    val nanotTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 100,
      tokenRecoveryInterval = 1000000.nanos, // Just to make it obvious that it's a large number.
      timeSource = nanotTimeControl)
    val numInvocations = 100
    var timesExecuted = 0
    for (_ <- 0 until numInvocations) {
      throttler.throttled { skipped =>
        assert(skipped === 0L)
        timesExecuted += 1
      }
      nanotTimeControl.advance(5.nanos)
    }
    assert(timesExecuted === numInvocations)
  }

  test("throttled, no burst") {
    val nanoTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 1,
      tokenRecoveryInterval = 5.nanos,
      timeSource = nanoTimeControl)
    val numInvocations = 100
    var timesExecuted = 0
    for (i <- 0 until numInvocations) {
      throttler.throttled { skipped =>
        if (timesExecuted == 0) {
          assert(skipped === 0L)
        } else {
          assert(skipped === 4L)
        }
        timesExecuted += 1
      }
      nanoTimeControl.advance(1.nanos)
    }
    assert(timesExecuted === numInvocations / 5)
  }

  test("throttled, single burst") {
    val nanoTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 5,
      tokenRecoveryInterval = 10.nanos,
      timeSource = nanoTimeControl)
    val numInvocations = 100
    var timesExecuted = 0
    for (i <- 0 until numInvocations) {
      throttler.throttled { skipped =>
        if (i < 5) {
          // First burst
          assert(skipped === 0L)
        } else if (i == 10) {
          // First token recovery
          assert(skipped === 5L)
        } else {
          // All other token recoveries
          assert(skipped === 9L)
        }
        timesExecuted += 1
      }
      nanoTimeControl.advance(1.nano)
    }
    // A burst of 5 and then 1 every 10ns/invocations.
    assert(timesExecuted === 5 + (numInvocations - 10) / 10)
  }

  test("throttled, bursty") {
    val nanoTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 5,
      tokenRecoveryInterval = 10.nanos,
      timeSource = nanoTimeControl)
    val numBursts = 10
    val numInvocationsPerBurst = 10
    var timesExecuted = 0
    for (burst <- 0 until numBursts) {
      for (i <- 0 until numInvocationsPerBurst) {
        throttler.throttled { skipped =>
          if (i == 0 && burst != 0) {
            // first after recovery
            assert(skipped === 5L)
          } else {
            // either first burst, or post-recovery on every other burst.
            assert(skipped === 0L)
          }
          timesExecuted += 1
        }
        nanoTimeControl.advance(1.nano)
      }
      nanoTimeControl.advance(100.nanos)
    }
    // Bursts of 5.
    assert(timesExecuted === 5 * numBursts)
  }

  test("wraparound") {
    val nanoTimeControl = new MockedNanoTime
    val throttler = new LogThrottler(
      bucketSize = 1,
      tokenRecoveryInterval = 100.nanos,
      timeSource = nanoTimeControl)
    def executeThrottled(expectedSkipped: Long = 0L): Boolean = {
      var executed = false
      throttler.throttled { skipped =>
        assert(skipped === expectedSkipped)
        executed = true
      }
      executed
    }
    assert(executeThrottled())
    assert(!executeThrottled())

    // Move to 2 ns before wrapping.
    nanoTimeControl.advance((Long.MaxValue - 1L).nanos)
    assert(executeThrottled(expectedSkipped = 1L))
    assert(!executeThrottled())

    nanoTimeControl.advance(1.nano)
    assert(!executeThrottled())

    // Wrapping
    nanoTimeControl.advance(1.nano)
    assert(!executeThrottled())

    // Recover
    nanoTimeControl.advance(100.nanos)
    assert(executeThrottled(expectedSkipped = 3L))
  }
}

/**
 * Use a mocked object to replace calls to `System.nanoTime()` with a custom value that can be
 * controlled by calling `advance(nanos)` on an instance of this class.
 */
class MockedNanoTime extends NanoTimeTimeSource {
  private var currentTimeNs: Long = 0L

  override def nanoTime(): Long = currentTimeNs

  def advance(time: FiniteDuration): Unit = {
    currentTimeNs += time.toNanos
  }
}

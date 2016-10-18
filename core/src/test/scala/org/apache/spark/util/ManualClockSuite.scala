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

import java.util.concurrent.CountDownLatch

import scala.util.control.NonFatal

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.Logging
import org.apache.spark.SparkFunSuite

class ManualClockSuite extends SparkFunSuite with Logging {

  // this test takes 3 seconds
  test("ManualClock - isThreadWaitingAt() & advance() are working") {

    val clock = new ManualClock()

    val latch = new CountDownLatch(1)

    new Thread(new Runnable {
      override def run(): Unit = {
        // verify that the clock should advance from 0 to 10 (in a very short time); otherwise fail
        // this test
        try {
          Eventually.eventually(Timeout(3.seconds)) {
            assert(clock.isThreadWaitingAt(0))
          }
          clock.advance(10)
        }
        catch {
          case NonFatal(e) => fail(e)
        }

        // verify that the clock should not advance any more, because no thread is waiting at 20
        val e = intercept[TestFailedException] {
          // this waiting for timeout takes most of this test's time
          Eventually.eventually(Timeout(3.seconds)) {
            assert(clock.isThreadWaitingAt(20))
          }
        }
        assert(e.getMessage().contains(
          "Last failure message: clock.isThreadWaitingAt(20L) was false"))

        // allow the main thread to proceed
        latch.countDown()
      }
    }).start()

    // verify that the clock should be advanced to 10 (in a very short time)
    Eventually.eventually(Timeout(3.seconds)) {
      assert(clock.waitTillTime(10) === 10)
    }

    // prevent this main thread to finish too early, so that we can verify the
    // clock-advance-thread would timeout as expected
    Eventually.eventually(Timeout(10.seconds)) {
      latch.await()
    }
  }

}

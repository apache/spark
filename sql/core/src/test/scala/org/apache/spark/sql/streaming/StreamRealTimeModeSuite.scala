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

package org.apache.spark.sql.streaming

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.sql.execution.streaming.RealTimeTrigger

class StreamRealTimeModeSuite extends StreamTest {

  test("test trigger") {
    def testTrigger(trigger: Trigger, actual: Long): Unit = {
      val realTimeTrigger = trigger.asInstanceOf[RealTimeTrigger]
      assert(
        realTimeTrigger.batchDurationMs == actual,
        s"Real time trigger duration should be ${actual} ms" +
        s" but got ${realTimeTrigger.batchDurationMs} ms"
      )
    }

    // test default
    testTrigger(Trigger.RealTime(), 300000)

    List(
      ("1 second", 1000),
      ("1 minute", 60000),
      ("1 hour", 3600000),
      ("1 day", 86400000),
      ("1 week", 604800000)
    ).foreach {
      case (str, ms) =>
        testTrigger(Trigger.RealTime(str), ms)
        testTrigger(RealTimeTrigger(str), ms)
        testTrigger(RealTimeTrigger.create(str), ms)

    }

    List(1000, 60000, 3600000, 86400000, 604800000).foreach { ms =>
      testTrigger(Trigger.RealTime(ms), ms)
      testTrigger(RealTimeTrigger(ms), ms)
      testTrigger(new RealTimeTrigger(ms), ms)
    }

    List(
      (Duration.apply(1000, "ms"), 1000),
      (Duration.apply(60, "s"), 60000),
      (Duration.apply(1, "h"), 3600000),
      (Duration.apply(1, "d"), 86400000)
    ).foreach {
      case (duration, ms) =>
        testTrigger(Trigger.RealTime(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
    }

    List(
      (1000, TimeUnit.MILLISECONDS, 1000),
      (60, TimeUnit.SECONDS, 60000),
      (1, TimeUnit.HOURS, 3600000),
      (1, TimeUnit.DAYS, 86400000)
    ).foreach {
      case (interval, unit, ms) =>
        testTrigger(Trigger.RealTime(interval, unit), ms)
        testTrigger(RealTimeTrigger(interval, unit), ms)
        testTrigger(RealTimeTrigger.create(interval, unit), ms)
    }
    // test invalid
    List("-1", "0").foreach(
      str =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(str), -1)
          testTrigger(RealTimeTrigger.create(str), -1)
        }
    )

    List(-1, 0).foreach(
      duration =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(duration), -1)
          testTrigger(RealTimeTrigger(duration), -1)
        }
    )
  }
}

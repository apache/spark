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

    testTrigger(Trigger.RealTime("1 second"), 1000)
    testTrigger(Trigger.RealTime("1 minute"), 60000)
    testTrigger(Trigger.RealTime("1 hour"), 3600000)
    testTrigger(Trigger.RealTime("1 day"), 86400000)
    testTrigger(Trigger.RealTime("1 week"), 604800000)

    testTrigger(Trigger.RealTime(1000), 1000)
    testTrigger(Trigger.RealTime(60000), 60000)
    testTrigger(Trigger.RealTime(3600000), 3600000)
    testTrigger(Trigger.RealTime(86400000), 86400000)
    testTrigger(Trigger.RealTime(604800000), 604800000)

    testTrigger(Trigger.RealTime(Duration.apply(1000, "ms")), 1000)
    testTrigger(Trigger.RealTime(Duration.apply(60, "s")), 60000)
    testTrigger(Trigger.RealTime(Duration.apply(1, "h")), 3600000)
    testTrigger(Trigger.RealTime(Duration.apply(1, "d")), 86400000)

    testTrigger(Trigger.RealTime(1000, TimeUnit.MILLISECONDS), 1000)
    testTrigger(Trigger.RealTime(60, TimeUnit.SECONDS), 60000)
    testTrigger(Trigger.RealTime(1, TimeUnit.HOURS), 3600000)
    testTrigger(Trigger.RealTime(1, TimeUnit.DAYS), 86400000)
  }
}

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

package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.Trigger

class ProcessingTimeSuite extends SparkFunSuite {

  test("create") {
    def getIntervalMs(trigger: Trigger): Long = {
      trigger.asInstanceOf[ProcessingTimeTrigger].intervalMs
    }

    assert(getIntervalMs(Trigger.ProcessingTime(10.seconds)) === 10 * 1000)
    assert(getIntervalMs(Trigger.ProcessingTime(10, TimeUnit.SECONDS)) === 10 * 1000)
    assert(getIntervalMs(Trigger.ProcessingTime("1 minute")) === 60 * 1000)
    assert(getIntervalMs(Trigger.ProcessingTime("interval 1 minute")) === 60 * 1000)

    intercept[IllegalArgumentException] { Trigger.ProcessingTime(null: String) }
    intercept[IllegalArgumentException] { Trigger.ProcessingTime("") }
    intercept[IllegalArgumentException] { Trigger.ProcessingTime("invalid") }
    intercept[IllegalArgumentException] { Trigger.ProcessingTime("1 month") }
    intercept[IllegalArgumentException] { Trigger.ProcessingTime("1 year") }
  }
}

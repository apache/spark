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

class ProcessingTimeSuite extends SparkFunSuite {

  test("create") {
    assert(ProcessingTime(10.seconds).intervalMs === 10 * 1000)
    assert(ProcessingTime.create(10, TimeUnit.SECONDS).intervalMs === 10 * 1000)
    assert(ProcessingTime("1 minute").intervalMs === 60 * 1000)
    assert(ProcessingTime("interval 1 minute").intervalMs === 60 * 1000)

    intercept[IllegalArgumentException] { ProcessingTime(null: String) }
    intercept[IllegalArgumentException] { ProcessingTime("") }
    intercept[IllegalArgumentException] { ProcessingTime("invalid") }
    intercept[IllegalArgumentException] { ProcessingTime("1 month") }
    intercept[IllegalArgumentException] { ProcessingTime("1 year") }
  }
}

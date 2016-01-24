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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class HasPollingPeriodSuite extends SparkFunSuite with PrivateMethodTester {
  test("HasPollingPeriod should take default values if not given") {
    val hasPollingPeriodImpl = new HasPollingPeriod {
      override def properties = new Properties()
    }
    val pollPeriod = PrivateMethod[Int]('pollPeriod)
    val pollUnit = PrivateMethod[TimeUnit]('pollUnit)
    assert(hasPollingPeriodImpl.invokePrivate(pollPeriod()) === 10)
    assert(hasPollingPeriodImpl.invokePrivate(pollUnit()) === TimeUnit.SECONDS)
  }

  test("HasPollingPeriod should pick up values if given") {
    val hasPollingPeriodImpl = new HasPollingPeriod {
      override def properties = {
        val p = new Properties()
        p.put("period", "5")
        p.put("unit", "minutes")
        p
      }
    }
    val pollPeriod = PrivateMethod[Int]('pollPeriod)
    val pollUnit = PrivateMethod[TimeUnit]('pollUnit)
    assert(hasPollingPeriodImpl.invokePrivate(pollPeriod()) === 5)
    assert(hasPollingPeriodImpl.invokePrivate(pollUnit()) === TimeUnit.MINUTES)
  }

  test("HasPollingPeriod should throw an exception if the polling period is < 1s") {
    val thrown = intercept[IllegalArgumentException] {
      new HasPollingPeriod {
        override def properties = {
          val p = new Properties()
          p.put("period", "500")
          p.put("unit", "milliseconds")
          p
        }
      }
    }
    assert(thrown.getMessage contains
      "Given polling period 500 MILLISECONDS below the minimal polling period (1 SECONDS)")
  }
}
